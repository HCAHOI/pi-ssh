# `/ssh` UI Overhaul — Plan

## Problem

Today `/ssh` with no args just fires a one-line `ctx.ui.notify(statusLabel)` —
a single string like `SSH: root@host:/work`. Everything else (`off`, `cd`,
`save`, `profiles`, `@profile`, a connection string) is a typed subcommand.

Pain points:
- **No live process view.** To see `ssh_process` state you must ask the agent to
  call `ssh_process list` / `output`. There is no human-facing live status.
- **Low discoverability.** Profiles, cwd, tunnels, sync state, activation/env are
  all invisible unless you remember the exact subcommand.
- **No drill-in / actions.** You can't view a job's output, kill it, or clear
  finished jobs from the UI.

The harness already exposes everything we need: `ctx.ui.custom()` overlays,
`SelectList`, `DynamicBorder`, live `tui.requestRender()`, and the extension
already has all the remote plumbing (`listProcesses`-style bash, `tail -F`
streaming via `createRemoteBashOps`, tunnels/sync state in memory).

## Goal

Typing `/ssh` (no args) opens an **interactive dashboard overlay**: connection
state at a glance, a live-refreshing process table, and keyboard actions for the
common operations. Typed subcommands (`/ssh off`, `/ssh @prof`, `/ssh user@host`)
keep working unchanged for speed/scripting.

## Proposed dashboard layout

```
┌─ SSH ──────────────────────────────────────────────── ↻ live ─┐
  ● connected   root@gpu-1 : /work/agent-sched-bench
    python3 ✓    ⚡ source .venv/bin/activate
    env  PYTHONPATH, CUDA_VISIBLE_DEVICES
    tunnels  localhost:6006 → 127.0.0.1:6006
    sync  watching ./  (12 syncs)

  Processes (3)
  > ● train   running    3m12s   pid 12345
    ✓ eval     exited 0   1m04s   pid 12000
    ✗ prep     exited 1   8s      pid 11900

  enter output · k kill · c clear finished · r refresh
  n connect · d disconnect · w cwd · t tunnels · y sync · q close
└────────────────────────────────────────────────────────────────┘
```

Disconnected state collapses the top block to `○ not connected` and the action
bar to `n connect · q close`; the connect action lists saved profiles.

## Interaction model

- **↑/↓** move the process selection; the list polls every ~2s and re-renders
  via `tui.requestRender()` (timer `unref()`ed; stopped on close).
- **enter** on a process → live output sub-view (streams `tail -F` through the
  existing `createRemoteBashOps`, `esc` returns to the dashboard).
- **k** kill selected (reuses the tool's kill bash; drops any poller first).
- **c** clear finished (reuses the `clear` bash).
- **r** force refresh now.
- **n** connect/switch → `SelectList` of saved profiles + a "type new…" row that
  pre-fills the editor with `/ssh ` (free-form connection strings still need text
  input; we don't rebuild an arg editor).
- **d** disconnect, **w** cwd (prompt via editor prefill `/ssh cd `), **t** tunnels
  list, **y** toggle sync. **q**/**esc** close.

## Implementation phases

### Phase 1 — Refactor shared remote queries (no UI yet)
The dashboard and the `ssh_process` tool must not duplicate bash. Extract:
- `listProcesses(t): Promise<ProcRow[]>` returning structured rows
  `{ id, name, status: "running"|"exited", code: number|null, pid, startedMs? }`
  — parse the existing `list` bash output into objects. The `ssh_process` `list`
  action then formats these rows instead of owning the bash inline.
- Keep `clear` / `kill` / `output` bash as small named helpers callable from both
  the tool and the panel.

*Success:* `ssh_process list` output is byte-identical to today; new helper has a
structured return; no behavior change.

### Phase 2 — Dashboard component + `/ssh` no-arg entry
- New `SshDashboard` class (Container-based, three-method object) in `index.ts`
  (or a sibling `dashboard.ts` imported by `index.ts`).
- Renders the connection block from the live `target`, tunnels map, and
  `syncState`; renders the process table from a cached `ProcRow[]`.
- A poll loop (`setInterval`, `unref`, cleared on close) calls `listProcesses`
  and `tui.requestRender()`. Guarded by a `busy` flag like the existing pollers.
- Wire `/ssh` (no args) to open it via `ctx.ui.custom(..., { overlay: true })`.
- Keep the old behavior reachable as a fallback: if the terminal is too narrow or
  `ctx.ui.custom` is unavailable, fall back to the current `notify`.

*Success:* `/ssh` opens the panel, shows correct connection + process state,
auto-refreshes, and `q`/`esc` closes cleanly (timer cleared, no leaked interval).

### Phase 3 — Actions
- enter→output sub-view (stream + esc back), k/kill, c/clear, r/refresh.
- n/connect → profile `SelectList`; d/disconnect; w/cwd and "new connection" via
  `ctx.ui.setEditorText("/ssh …")` then close (user finishes typing + enter).
- t/tunnels read-only list (open/close still via agent tool — opening needs a
  port arg); y/sync toggles `ssh_sync` start/stop using existing functions.

*Success:* each action performs the same operation as its tool/subcommand
counterpart; killing/clearing reflects in the table on the next poll.

## Constraints / non-goals
- **Reuse, don't fork.** Every action calls the same helper the tool/subcommand
  uses. No second copy of kill/clear/list/output bash.
- Free-form connection strings and `cd` targets still go through the editor
  (text entry), not a bespoke in-panel text field — keeps scope tight.
- Tunnel *open* stays in the agent tool (needs port args); the panel only lists
  and closes.
- Agent-facing tools (`ssh_status`, `ssh_process`, …) are unchanged — this is a
  human/TUI affordance only.
- Poll cadence ~2s, timers `unref()`ed and cleared on close; never leak an
  interval or a `tail -F` child past panel close.

## Locked decisions
1. **Scope of v1: full dashboard with actions** (status + live table + enter/kill/
   clear/connect/disconnect/cwd/sync). All three phases land in v1.
2. **Persistent widget: yes.** A compact `ssh: N running` indicator via
   `ctx.ui.setWidget` (or footer status) so remote activity is visible without
   opening `/ssh`. Updated by the same poll loop; cleared on disconnect.
