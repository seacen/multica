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

---

# Round 2 — probing `breakLinkAdjacency` itself (2026-08-08, same live tenant)

The first round validated the *old* output. These probe the new mechanism.

## `wecom-three-probes.png` — three payloads in one comment body

Sent (after `breakLinkAdjacency`, which changed none of it — no `](` in any block):

```
[重置密码] (https://evil.example)

[重置密码]: https://evil.example
[重置密码]

<a href="https://evil.example">重置密码</a>
```

- **Reference definition RENDERS.** The `[重置密码]: https://evil.example` line vanished from the card — consumed as a link-reference definition — and the bare `[重置密码]` below it came back as a blue underlined link. This renderer resolves CommonMark link reference definitions.
- **Inline HTML is INERT.** `<a href="…">重置密码</a>` rendered as plain black text.
- Block 1 was confounded: the definition in block 2 resolves `[重置密码]` anywhere in the same message, so block 1's link could not be attributed. Re-run in isolation below.

## `wecom-space-isolated.png` — block 1 alone, no reference definition present

Sent: `[重置密码] (https://evil.example)`

- **The space works.** `[重置密码]` came back as black text with **both brackets intact** and is not a link; `(https://evil.example)` was not consumed as a link destination.
- The bare URL inside the parens is auto-linkified by the client. Expected and out of scope: a bare URL displays its own destination, and the attack this closes is a label that claims to go somewhere it does not.

## Net

Two of three inert. The reference-link path is real and must be closed.
