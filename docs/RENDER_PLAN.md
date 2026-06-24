# SSH tool-call rendering — Plan

## Root cause (why ssh_read shows no `path:lines` today)

The local tools render a rich title/result via `renderCall` / `renderResult`,
which live on the **`ToolDefinition`** returned by `create*ToolDefinition`.

The SSH extension instead imports the **wrapped** `create*Tool` variants:

```ts
const localRead = createReadTool(localCwd);   // = wrapToolDefinition(createReadToolDefinition(...))
pi.registerTool({ ...localRead, name: "ssh_read", … execute });
```

`wrapToolDefinition` returns an `AgentTool` that only carries
`name/label/description/parameters/execute` — it **drops `renderCall` and
`renderResult`** (verified in `dist/core/tools/tool-definition-wrapper.js`).
So every `ssh_*` tool falls back to pi's generic tool-call rendering: no
`path:lines`, no `$ command`, no diff. The fix is to give each SSH tool its own
`renderCall` (and, where useful, `renderResult`).

The local format helpers (`formatReadCall`, `renderToolPath`,
`formatReadLineRange`, `formatBashCall`) are **not exported**, so we reimplement
the small bits we need. `registerTool` accepts a full `ToolDefinition`, and the
extension's `renderCall` closes over the live `target`, so we can prefix the
remote host and relativize paths against `remoteCwd`.

## Target rendering

| tool | call title |
|------|------------|
Host shown as a bracketed tag `[root@host]` (decided), on **every** ssh_* title:

| `ssh_read`  | `ssh_read [root@host] src/agents/openclaw/eval/types.py:100-139` |
| `ssh_edit`  | `ssh_edit [root@host] src/agents/openclaw/eval/types.py (2 edits)` |
| `ssh_bash`  | `ssh_bash [root@host] $ pytest -q tests/ (timeout 60s)` |
| `ssh_process` | `ssh_process [root@host] start "train" $ python train.py` · `ssh_process [root@host] kill m1a-9f3` |

Result views:
- `ssh_read`  → reuse the local read `renderResult` (syntax highlight + truncation
  note; verified it does **no** filesystem access — formats the returned content
  by path extension), so remote reads get the same highlighted body.
- `ssh_edit`  → parse the unified diff already in `result.details.diff` and show a
  `+N -M` summary (the local edit renderer can't be reused: its renderCall
  computes a *local* fs diff preview, wrong for remote).
- `ssh_bash` / `ssh_process` → keep default text result (already fine).

## Shared helpers (new, in `index.ts`)

```ts
// remote path relative to remoteCwd for display, best-effort
function remoteDisplayPath(raw: string): string
// ":100-139" from {offset,limit}, mirrors formatReadLineRange
function readLineRange(args, theme): string
// builds the styled one-line title Text, reusing context.lastComponent
function sshTitle(parts, theme, context): Text
```

Styling matches local conventions: name in `theme.fg("toolTitle", bold(...))`,
host in `muted`, path in `accent`, line-range in `warning`, command in `bold`.

## Implementation phases

### Phase 1 — switch bases to `*ToolDefinition`, add a title helper
- Import `create*ToolDefinition` + `Text` (from pi-tui). Keep the wrapped
  `create*Tool` only where the per-call execute re-instantiates with remote ops.
- Add `remoteDisplayPath`, `readLineRange`, `sshTitle`.
- Spread the **definition** (so `parameters`/`description`/local `renderResult`
  come along) and override `name/label/description/execute` as today.

### Phase 2 — per-tool `renderCall`
- `ssh_read`: title with host + `remoteDisplayPath` + `readLineRange`; inherit
  local `renderResult`.
- `ssh_edit`: title with host + path + `(N edits)`; add `renderResult` parsing
  `details.diff` → `+adds -dels`.
- `ssh_bash`: title `$ command` (+ host tag, + `(timeout Ns)` / `(cwd …)`).
- `ssh_process`: title from `action` (+ `name`/`command` for start, `id` for
  kill/output/logs/attach).

### Phase 3 — (optional) extend to the rest
Same `sshTitle` helper applied to `ssh_write` / `ssh_ls` / `ssh_find` /
`ssh_grep` / `ssh_secret_write` / `ssh_push` / `ssh_pull` / `ssh_tunnel` /
`ssh_sync` so the whole suite is visually consistent. Cheap, same pattern.

## Constraints / non-goals
- `renderCall` reads the **live** `target` (display-time). If disconnected at
  render, drop the host and show the raw path — never throw from a renderer.
- `remoteDisplayPath` wraps `toRemotePath` in try/catch; on failure show the raw
  arg verbatim (never crash the row).
- No change to agent-facing behavior, tool params, or execute logic — purely
  presentational. The dashboard work from the previous change is untouched.
- Keep titles single-line; let the tool shell truncate to width (as local does).

## Locked decisions
1. **Scope: all `ssh_*` tools** (read/edit/bash/process + write/ls/find/grep/
   secret/push/pull/tunnel/sync/connect), one shared `sshTitle` helper.
2. **Host tag: `[root@host]` on every title**, between the tool name and the rest.
