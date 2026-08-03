package wecom

// media_download.go — fetching the bytes a callback points at.
//
// The URL is a pre-signed Tencent COS address: no access_token, no header,
// good for five minutes. That makes the fetch itself trivial and puts all the
// care somewhere else — the body arrives encrypted, its size is not declared
// anywhere in the callback, and the only description of what the file IS
// comes back in the response headers.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// maxMediaBytes is the ceiling on one downloaded body. WeCom caps smart-bot
// files and video at 100 MB and does not document a cap for images, so this
// is the ceiling for everything: whatever arrives above it is not something
// the callback was supposed to hand us.
//
// The body is buffered whole — CBC decryption needs the tail before the head
// can be trusted — so this is also the per-download memory bound, multiplied
// by the router's media concurrency.
const maxMediaBytes = 100 << 20

// mediaDownloadTimeout caps a single fetch. The router already runs media
// resolution under a 45s budget shared by every attachment on the message and
// by whatever is queued ahead of it, so one slow object must not eat all of
// it.
const mediaDownloadTimeout = 30 * time.Second

// errMediaTooLarge is returned for a body past maxMediaBytes, either declared
// in Content-Length or discovered while reading. Callers match on it to tell
// the user the file was too big rather than that something went wrong.
var errMediaTooLarge = fmt.Errorf("wecom: media exceeds the %d byte limit", maxMediaBytes)

// downloadedMedia is one fetched body plus what the response said about it.
type downloadedMedia struct {
	// Body is the raw response — still encrypted. decryptMedia turns it into
	// the file.
	Body []byte
	// Filename is the display name parsed out of Content-Disposition, or
	// empty. The callback body carries no name, size or MIME type of its
	// own, so this header is the only place the original name exists.
	Filename string
}

// downloadMedia GETs one media URL. Errors carry the reason (expired link,
// oversize body, stalled server) because the caller turns them into something
// a person reads.
func downloadMedia(ctx context.Context, hc *http.Client, rawURL string) (downloadedMedia, error) {
	if err := checkMediaURL(rawURL); err != nil {
		return downloadedMedia{}, err
	}
	if hc == nil {
		hc = http.DefaultClient
	}
	ctx, cancel := context.WithTimeout(ctx, mediaDownloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return downloadedMedia{}, fmt.Errorf("wecom: media request: %w", err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return downloadedMedia{}, fmt.Errorf("wecom: media download: %w", err)
	}
	defer resp.Body.Close()

	if resp.ContentLength > maxMediaBytes {
		return downloadedMedia{}, errMediaTooLarge
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// A five-minute URL that has already lapsed is the ordinary failure
		// here, and COS explains itself in a short XML body. Carry a snippet
		// so the log says which kind of refusal it was.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return downloadedMedia{}, fmt.Errorf("wecom: media download: http %d %s: %s",
			resp.StatusCode, http.StatusText(resp.StatusCode), strings.TrimSpace(string(snippet)))
	}

	// LimitReader with one byte of headroom: reading exactly the cap cannot
	// tell "the file is exactly at the limit" from "there is more coming".
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMediaBytes+1))
	if err != nil {
		return downloadedMedia{}, fmt.Errorf("wecom: media download: read body: %w", err)
	}
	if len(body) > maxMediaBytes {
		return downloadedMedia{}, errMediaTooLarge
	}
	return downloadedMedia{
		Body:     body,
		Filename: mediaFilenameFromDisposition(resp.Header.Get("Content-Disposition")),
	}, nil
}

// checkMediaURL refuses anything the transport should not be pointed at. The
// URL arrives over the authenticated socket, so this is a guard rail rather
// than a defence, but it is a string from outside naming a host we then GET.
func checkMediaURL(rawURL string) error {
	s := strings.TrimSpace(rawURL)
	if s == "" {
		return errors.New("wecom: media url is empty")
	}
	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("wecom: media url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("wecom: media url scheme %q is not fetchable", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("wecom: media url has no host")
	}
	return nil
}

// mediaFilenameFromDisposition reads the display name out of
// Content-Disposition, preferring RFC 5987's extended `filename*` form over the
// plain `filename=` beside it. Servers that send both put the real (non-ASCII)
// name in the extended form and a mangled ASCII approximation in the plain
// one, so taking the plain one would rename every Chinese attachment to
// underscores. mime.ParseMediaType already resolves that preference for us
// regardless of the order the two appear in; the tests pin it because it is a
// property we depend on, not one we implement.
//
// The result is reduced to a base name: the header is remote input and a
// filename with path separators in it has no business reaching a storage key
// or a download header.
func mediaFilenameFromDisposition(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(raw)
	if err != nil {
		return ""
	}
	return cleanMediaFilename(params["filename"])
}

// cleanMediaFilename reduces a name from anywhere to a single path segment,
// or empty when nothing usable is left.
func cleanMediaFilename(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	if name == "." || name == "/" || name == ".." {
		return ""
	}
	return name
}
