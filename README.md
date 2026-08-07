# WeCom inbox card rendering — evidence for PR #6592

Captured on a live WeCom tenant, 2026-08-08.

Sent on the wire: `**[Status changed] \[Bug\] 登录失败**` followed by a details link.
`[Status changed]` is our own label and is not escaped. `\[Bug\]` is what the escaping helper produced
from the issue title `[Bug] 登录失败`.

| file | surface | what it shows |
|---|---|---|
| `wecom-card-list-preview.jpeg` | conversation list | the backslashes are visible verbatim — no markdown rendering here |
| `wecom-card-in-chat.jpeg` | the card in the chat | `[Status changed]` keeps its brackets; `\[Bug\]` renders as an italic serif "Bug" with the brackets gone |

The italic serif is how LaTeX display math renders, and `\[ ... \]` is the display-math delimiter —
so the escape is being read as a math delimiter rather than as a markdown escape. By the same reading
`\( ... \)` is the inline-math delimiter, which covers the escaped parens too.

This identification is inferred from the rendering; no WeCom document confirms it. The conclusion does
not depend on the mechanism's name: backslash escaping is not usable on this surface.
