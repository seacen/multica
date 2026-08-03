package wecom

// inbound_msgtype_test.go — coverage for what the read loop does with a
// callback it cannot read: a kind WeCom has that this adapter does not, or a
// known kind that arrived without the field that makes it usable. The user
// must not be left talking into a void, and a redelivered frame must not
// produce a second receipt.
//
// Voice notes, photos and files are NOT in here any more — they are ingested
// (inbound_voice_test.go, inbound_media_test.go).

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
)

// testLogger keeps the package's Warn/Debug lines out of the test output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// recordingConn captures every frame written to the socket and never reads.
type recordingConn struct {
	mu     sync.Mutex
	frames []map[string]any
}

func (c *recordingConn) WriteMessage(_ int, data []byte) error {
	var f map[string]any
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frames = append(c.frames, f)
	return nil
}

func (c *recordingConn) ReadMessage() (int, []byte, error) { return 0, nil, errConnDropped }
func (c *recordingConn) SetReadDeadline(time.Time) error   { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error  { return nil }
func (c *recordingConn) Close() error                      { return nil }

func (c *recordingConn) sends() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []map[string]any
	for _, f := range c.frames {
		if f["cmd"] == cmdSendMsg {
			out = append(out, f)
		}
	}
	return out
}

// fakeDeduper is an in-memory stand-in for the shared
// channel_inbound_message_dedup table.
type fakeDeduper struct {
	mu       sync.Mutex
	claimed  map[string]bool
	claimErr error
}

func newFakeDeduper() *fakeDeduper { return &fakeDeduper{claimed: map[string]bool{}} }

func (d *fakeDeduper) Claim(_ context.Context, _ pgtype.UUID, messageID string) (pgtype.UUID, error) {
	if d.claimErr != nil {
		return pgtype.UUID{}, d.claimErr
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.claimed[messageID] {
		return pgtype.UUID{}, engine.ErrDuplicate
	}
	d.claimed[messageID] = true
	return pgtype.UUID{Bytes: [16]byte{1}, Valid: true}, nil
}

func (d *fakeDeduper) Mark(context.Context, pgtype.UUID, string, pgtype.UUID) error { return nil }

func (d *fakeDeduper) Release(_ context.Context, _ pgtype.UUID, messageID string, _ pgtype.UUID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.claimed, messageID)
	return nil
}

// testChannel builds a wecomChannel wired to an in-memory socket + deduper.
func testChannel(t *testing.T, handler channel.InboundHandler) (*wecomChannel, *recordingConn, *fakeDeduper) {
	t.Helper()
	dedup := newFakeDeduper()
	return &wecomChannel{
		installationID: pgtype.UUID{Bytes: [16]byte{9}, Valid: true},
		botID:          "bot",
		handler:        handler,
		dedup:          dedup,
	}, &recordingConn{}, dedup
}

func mediaFrame(msgType, msgID string) frameEnvelope {
	body, _ := json.Marshal(map[string]any{
		"msgid":    msgID,
		"aibotid":  "bot",
		"chattype": "single",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  msgType,
	})
	return frameEnvelope{Cmd: cmdMsgCallback, Body: body}
}

// TestUnsupportedMsgTypeGetsAReceipt: a message kind the adapter cannot read
// must draw an answer rather than a Debug log. Fails on the pre-change code,
// which returned nil from dispatchFrame without writing anything. The example
// is a location card — a voice note is no longer unsupported (WeCom sends its
// transcript, see inbound_voice_test.go).
func TestUnsupportedMsgTypeGetsAReceipt(t *testing.T) {
	c, conn, _ := testChannel(t, func(context.Context, channel.InboundMessage) error {
		t.Fatal("an unreadable message must not reach the engine handler")
		return nil
	})
	sender := newWSSender(conn, nil)

	if err := c.dispatchFrame(context.Background(), mediaFrame("location", "msg-1"), sender, testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}

	sends := conn.sends()
	if len(sends) != 1 {
		t.Fatalf("want exactly one aibot_send_msg receipt, got %d", len(sends))
	}
	body, _ := sends[0]["body"].(map[string]any)
	md, _ := body["markdown"].(map[string]any)
	content, _ := md["content"].(string)
	if content == "" {
		t.Fatal("receipt carried no content")
	}
	if got := body["chatid"]; got != "T-alex" {
		t.Fatalf("receipt addressed to %v, want the sender's userid", got)
	}
	if got := body["chat_type"]; got != float64(chatTypeSingleInt) {
		t.Fatalf("chat_type = %v, want %d (single)", got, chatTypeSingleInt)
	}
}

// TestUnsupportedMsgTypeReceiptIsDedupedByMsgID: WeChat redelivers frames.
// The second delivery of the same msgid must stay silent.
func TestUnsupportedMsgTypeReceiptIsDedupedByMsgID(t *testing.T) {
	c, conn, _ := testChannel(t, func(context.Context, channel.InboundMessage) error { return nil })
	sender := newWSSender(conn, nil)

	for i := 0; i < 3; i++ {
		if err := c.dispatchFrame(context.Background(), mediaFrame("location", "msg-dup"), sender, testLogger()); err != nil {
			t.Fatalf("dispatchFrame #%d: %v", i, err)
		}
	}
	if n := len(conn.sends()); n != 1 {
		t.Fatalf("three deliveries of one msgid produced %d receipts, want 1", n)
	}
}

// TestUnsupportedMsgTypeReceiptReleasesClaimOnSendFailure: a failed write must
// not consume the dedup slot, otherwise a retry is silently swallowed.
func TestUnsupportedMsgTypeReceiptReleasesClaimOnSendFailure(t *testing.T) {
	c, _, dedup := testChannel(t, func(context.Context, channel.InboundMessage) error { return nil })
	failing := newWSSender(&failingConn{}, nil)

	if err := c.dispatchFrame(context.Background(), mediaFrame("location", "msg-fail"), failing, testLogger()); err != nil {
		t.Fatalf("a failed receipt must not escalate to the read loop: %v", err)
	}
	dedup.mu.Lock()
	still := dedup.claimed["msg-fail"]
	dedup.mu.Unlock()
	if still {
		t.Fatal("claim was not released after the receipt write failed")
	}
}

// TestMixedMessageRoutesItsTextPart: 图文混排 carrying a text run is a real
// message, not an unsupported type — it goes to the engine with the text and
// a placeholder holding the picture's place until the download lands.
func TestMixedMessageRoutesItsTextPart(t *testing.T) {
	var got channel.InboundMessage
	c, conn, _ := testChannel(t, func(_ context.Context, m channel.InboundMessage) error {
		got = m
		return nil
	})
	sender := newWSSender(conn, nil)

	body, _ := json.Marshal(map[string]any{
		"msgid":    "msg-mixed",
		"aibotid":  "bot",
		"chattype": "group",
		"chatid":   "wrOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "mixed",
		"mixed": map[string]any{
			"msg_item": []any{
				map[string]any{"msgtype": "text", "text": map[string]any{"content": "看下这张图"}},
				map[string]any{"msgtype": "image", "image": map[string]any{"url": "https://example.invalid/a.png"}},
			},
		},
	})
	env := frameEnvelope{Cmd: cmdMsgCallback, Body: body}

	if err := c.dispatchFrame(context.Background(), env, sender, testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	want := "看下这张图\n" + copyFor(DefaultLocale).MediaImage
	if got.Text != want {
		t.Fatalf("engine received Text=%q, want %q", got.Text, want)
	}
	if got.MessageID != "msg-mixed" {
		t.Fatalf("MessageID = %q", got.MessageID)
	}
	if n := len(conn.sends()); n != 0 {
		t.Fatalf("a mixed message with text must not draw a receipt, got %d", n)
	}
}

// TestMixedMessageWithNothingUsableGetsAReceipt: an empty text run beside an
// attachment with no url leaves nothing to store and nothing to fetch, so it
// falls back to the receipt path rather than starting a run over a blank
// message.
func TestMixedMessageWithNothingUsableGetsAReceipt(t *testing.T) {
	c, conn, _ := testChannel(t, func(context.Context, channel.InboundMessage) error {
		t.Fatal("an empty mixed message must not reach the engine handler")
		return nil
	})
	sender := newWSSender(conn, nil)

	body, _ := json.Marshal(map[string]any{
		"msgid":    "msg-mixed-empty",
		"aibotid":  "bot",
		"chattype": "single",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "mixed",
		"mixed": map[string]any{
			"msg_item": []any{
				map[string]any{"msgtype": "text", "text": map[string]any{"content": "   "}},
				map[string]any{"msgtype": "image", "image": map[string]any{"aeskey": "k"}},
			},
		},
	})
	if err := c.dispatchFrame(context.Background(), frameEnvelope{Cmd: cmdMsgCallback, Body: body}, sender, testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	if n := len(conn.sends()); n != 1 {
		t.Fatalf("want one receipt for an empty mixed message, got %d", n)
	}
}

// failingConn refuses every write.
type failingConn struct{}

func (failingConn) WriteMessage(int, []byte) error    { return errors.New("socket gone") }
func (failingConn) ReadMessage() (int, []byte, error) { return 0, nil, errConnDropped }
func (failingConn) SetReadDeadline(time.Time) error   { return nil }
func (failingConn) SetWriteDeadline(time.Time) error  { return nil }
func (failingConn) Close() error                      { return nil }
