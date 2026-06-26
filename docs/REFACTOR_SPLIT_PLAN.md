# Phase 0: Split & Reorganize `index.ts`

> Status: ✅ **DONE** (runtime-verified). Pure structural refactor — **zero
> behavior change**. Prerequisite for the monitor decoupling (see
> `MONITOR_PLAN.md`). Precedent mirrored: `@aliou/pi-processes`.

---

## ✅ Outcome (what actually shipped)

`index.ts` went **2858 → 400 lines**; 21 focused modules. Done in 9 commits
(`0a`→`0e`), each gated on the tsc baseline (19 pre-existing version-skew errors,
never more) and `--noUnusedLocals`. Two independent fresh-context reviews: both
"safe to build on," zero blockers/concerns. Smoke-tested live against a real
remote after a pi restart (connect, bash, fs, edit, secret_write, process
start/output/list/kill/clear, push/pull, tunnel, sync, **and the full poller
lifecycle: 3 watch matches + success completion notification**).

**Final module layout (`src/`):**

| module | role |
|---|---|
| `index.ts` (400) | activation: builds `SshContext`, attaches managers, calls `setup*(ctx)` |
| `context.ts` | `SshContext` interface (the shared surface) |
| `types.ts` | shared interfaces (`SshTarget`, `Run*`, `Watch*`, `PollerState`, …) |
| `utils.ts` | shQuote, paths, formatDuration, summarizeRsync, withFileLock, grepArgs, buildEnvExports |
| `notify.ts` | `sendProcessMessage` — the agent notification sink |
| `render.ts` | `createRender(getTarget, localCwd)` — renderCall/renderResult helpers |
| `transfer.ts` | rsync core (runLocalProcess, runRsyncTransfer, …; take `localCwd`) |
| `process-queries.ts` | processRoot, listProcesses, formatProcRows, build{Kill,Clear}Command, processRunScript |
| `poller.ts` | `createPollerManager(pi)` — start/stop/has/repointAll/markLatestStart/rehydrate; `buildWatchStates`, `NotifyConfig` |
| `remote-ops.ts` | create*Ops factories, runRemoteGrep, remotePatchEdit, PATCH_SCRIPT |
| `tunnels.ts` | `createTunnelManager(ctx)` — stopAll/list + ssh_tunnel tool |
| `sync.ts` | `createSyncManager(ctx)` — stop/startSync/getState + ssh_sync tool |
| `hooks.ts` | `setupHooks(ctx)` — session_start/shutdown, before_agent_start |
| `dashboard.ts` | `setupDashboard(ctx)` — SshDashboard TUI + /ssh command |
| `ssh/{transport,reconnect,target}.ts` | runSsh/runRemoteCommand; backoff+notifier; resolveTarget |
| `tools/{connection,fs,bash,process,transfer}.ts` | `setup*(ctx)` tool registrations |

**Key seams for the monitor work:** `poller.ts` (the watch engine) → `notify.ts`
(`sendProcessMessage`); a NotifyPolicy/throttle/digest layer slots between
`runWatches` and `emit`. New subsystems register through the same
`setup*(ctx: SshContext)` pattern. `fetchDeltaLines` (in `poller.ts`) and
`runRemoteCommand` (in `ssh/transport.ts`) are the reusable signal-acquisition
primitives.

> The sections below are the original pre-implementation plan, kept for the
> rationale/sub-phase record. They are now historical.

---

## 0. Why this is the real Phase 0

`index.ts` is 2858 lines in one file. Before adding a monitor subsystem we split
it into modules. This is not cosmetic: the monitor work needs a clean seam
between transport, process/poller, tools, and UI — which the monolith doesn't
have.

**pi supports multi-file `.ts` extensions** (verified): `@aliou/pi-processes`
ships `pi.extensions: ["./src/index.ts"]`, whose `index.ts` does relative `./`
imports of sibling modules; pi's loader transpiles the whole graph. We mirror
that exactly.

---

## 1. Current shape (grounded)

```
index.ts (2858 lines)
├─ 1–72     imports + header
├─ 73–731   TOP-LEVEL, stateless helpers & factories  ← easy to move
│   ├─ types: SshTarget, WatchSpec, WatchState, PollerState, Activation,
│   │         RunResult, RunOptions, ReconnectInfo, ReconnectPhase, EditResult
│   ├─ transport: runSsh, baseSshOptions, sshConnArgs, remoteShell, sshExec,
│   │             sshExecRaw, runRemoteCommand, sshFailureMessage,
│   │             isRetryableSshFailure, probePython, closeMaster, resolveTarget
│   ├─ reconnect: RECONNECT_* consts, reconnectCtx, withReconnect,
│   │             backoffDelay, abortableSleep
│   ├─ utils: shQuote, stripTrailingSlash, formatDuration, summarizeRsync,
│   │         toRemotePath, withFileLock/fileLocks, grepArgs
│   └─ remote-ops factories: createRemote{Read,Write,Edit,Ls,Find,Bash}Ops,
│                            runRemoteGrep, PATCH_SCRIPT, remotePatchEdit
└─ 749–2858 export default function (pi) {  ← ONE activation closure
    ├─ local tool instances (localRead/Write/Edit/Find/Grep/Ls/Bash)
    ├─ SHARED MUTABLE STATE (closure vars):
    │     target, get(), uiRef, widgetTimer, pollers, latestStartByName,
    │     sync-watcher state, reconnect UI state
    ├─ widget poller (779–816)            startWidgetPoller/stopWidgetPoller/tickWidget
    ├─ reconnect UI wiring (819–830)
    ├─ POLLER subsystem (1000–1290):
    │     stopPoller, stopAllPollers, statusCmd, neededStreams, fetchDeltaLines,
    │     emit, runWatches, sweepWatches, fireCompletion, tick, startPoller,
    │     buildWatchStates, NotifyConfig, rehydratePollers, requireTarget
    ├─ process queries (1736+): processRoot, buildProcessListCommand,
    │     listProcesses, formatProcRows, processRunScript, build{Kill,Clear}Command
    ├─ TOOLS (1421–2406): ssh_connect/disconnect/status, ssh_read/ls/find/grep/
    │     write/secret_write/edit, ssh_bash, ssh_process, ssh_push/pull, ssh_tunnel,
    │     ssh_sync
    ├─ HOOKS: session_start (2406), session_shutdown (2420), before_agent_start (2425)
    └─ COMMAND: /ssh dashboard (2788+)
  }
```

**The crux:** everything in the closure (tools, poller, hooks, dashboard) reads
and mutates `target` / `pollers` / `uiRef` / sync state via lexical closure. You
cannot move a tool into its own file without first giving that shared state an
explicit home.

---

## 2. Target module layout (`src/`)

Mirror the pi-processes convention: move to `src/`, `index.ts` becomes a thin
wiring entry.

```
src/
├─ index.ts            // thin: build ctx, call setup*(pi, ctx), register hooks/cmd
├─ types.ts            // all interfaces/types (SshTarget, Run*, Watch*, Poller*, …)
├─ context.ts          // SshContext: the shared-state container (see §3)
├─ ssh/
│  ├─ transport.ts     // runSsh, sshExec(Raw), runRemoteCommand, sshConnArgs,
│  │                   //   baseSshOptions, remoteShell, sshFailureMessage,
│  │                   //   isRetryableSshFailure, probePython, closeMaster
│  ├─ reconnect.ts     // RECONNECT_*, reconnectCtx, withReconnect, backoffDelay,
│  │                   //   abortableSleep
│  └─ target.ts        // resolveTarget, toRemotePath, processRoot, stripTrailingSlash
├─ utils.ts            // shQuote, formatDuration, summarizeRsync, withFileLock, grepArgs
├─ remote-ops.ts       // create*Ops factories, runRemoteGrep, PATCH_SCRIPT, remotePatchEdit
├─ poller.ts           // the poller subsystem (createPoller(ctx) → {start,stop,rehydrate})
├─ process-queries.ts  // processRoot users: listProcesses, formatProcRows, run-script + kill/clear builders
├─ tools/
│  ├─ connect.ts       // ssh_connect, ssh_disconnect, ssh_status
│  ├─ fs.ts            // ssh_read, ssh_ls, ssh_find, ssh_grep, ssh_write, ssh_secret_write, ssh_edit
│  ├─ bash.ts          // ssh_bash
│  ├─ process.ts       // ssh_process
│  ├─ transfer.ts      // ssh_push, ssh_pull, ssh_sync
│  └─ tunnel.ts        // ssh_tunnel
├─ hooks.ts            // session_start, session_shutdown, before_agent_start
└─ dashboard.ts        // /ssh command + widget poller + reconnect UI wiring
```

~16 files, none over ~400 lines. Grouping is by concern, not by line count.

---

## 3. The linchpin: `SshContext` (breaks the closure)

Replace the implicit closure state with one explicit object, constructed in
`index.ts` and passed to every `setup*`. This is the only non-mechanical part.

```ts
// context.ts
export interface SshContext {
  pi: ExtensionAPI;
  localCwd: string;
  // connection (was the `target` / `get()` closure var)
  getTarget(): SshTarget | null;
  setTarget(t: SshTarget | null): void;
  requireTarget(): SshTarget;
  // UI handle (was `uiRef`)
  ui: { ref: UiRef | null; capture(ctx): void };
  // subsystems own their own state, not the closure:
  poller: PollerManager;        // from poller.ts; owns `pollers`, latestStartByName
  // local tool instances (localRead/Write/…), built once
  local: { read; write; edit; find; grep; ls; bash; readDef };
}
```

- `pollers`, `latestStartByName`, the poller timers → move **inside**
  `PollerManager` (a small factory/class in `poller.ts`). Tools call
  `ctx.poller.start(...)` / `ctx.poller.rehydrate(t)` instead of touching a
  closure map.
- `uiRef`, `widgetTimer` → into `dashboard.ts` / an `ui` slice of ctx.
- sync-watcher state → into `tools/transfer.ts`.
- `emit()` (the sink) lives in `poller.ts` (it's the poller's notifier) but takes
  `pi` from ctx.

Every former closure function becomes `fn(ctx, …)` or a method on the subsystem
that owns the relevant state.

---

## 4. Sub-phases (each ends green: `tsc --noEmit` passes, smoke test OK)

Do it in dependency order, leaf-first, so each step compiles standalone.

- **0a — Scaffold.** Create `src/`, move `index.ts` → `src/index.ts` unchanged.
  Update `package.json` (`pi.extensions: ["./src/index.ts"]`, `files: ["src", …]`),
  add `"typecheck": "tsc --noEmit"` script, widen tsconfig `include` to
  `["src/**/*.ts"]`. **Verify load still works before any code moves.**
- **0b — Extract stateless leaves** (zero closure deps): `types.ts`, `utils.ts`,
  `ssh/reconnect.ts`, `ssh/transport.ts`, `ssh/target.ts`, `remote-ops.ts`.
  These are lines 73–731 — pure cut + add `export`/`import`. Lowest risk.
- **0c — Introduce `SshContext` + `PollerManager`.** Move the poller subsystem
  into `poller.ts` behind a factory that owns `pollers`/`latestStartByName`.
  `index.ts` constructs ctx. Still one tools blob, but now reading ctx.
- **0d — Carve tools into `tools/*.ts`,** one group at a time (connect → fs →
  bash → process → transfer → tunnel), each as `setupXxxTools(ctx)`. Typecheck
  after each.
- **0e — Move hooks + dashboard** into `hooks.ts` / `dashboard.ts`. `index.ts`
  is now ~40 lines of wiring.

---

## 5. Package / build changes

```jsonc
// package.json
"pi": { "extensions": ["./src/index.ts"] },
"files": ["src", "README.md", "LICENSE"],
"scripts": { "typecheck": "tsc --noEmit" }   // pi-processes precedent
```
```jsonc
// tsconfig.json
"include": ["src/**/*.ts"]   // was ["index.ts"]
```

---

## 6. Verification (no test suite exists today)

`pi-ssh` has **no tests and no build script** currently. The automated gate is
`tsc --noEmit` after every sub-phase. Beyond that, a manual smoke matrix per
sub-phase:

1. Extension loads (no import/transpile error on session start).
2. `ssh_connect` → `ssh_status` works.
3. `ssh_process start` with a `logWatch` → watch fires a notification; completion
   fires; `ssh_process list/output/attach/kill/clear` all work.
4. Reconnect (`ssh_connect` to switch) → `rehydratePollers` re-arms (a finished
   job's missed completion fires once).
5. `/ssh` dashboard renders; widget updates.

**Strong recommendation:** before 0c (the risky closure-breaking step), add a
couple of `vitest` unit tests for the pure helpers (`shQuote`, `formatDuration`,
`fetchDeltaLines` offset math, `buildWatchStates`) so the refactor has a
regression net. This is also the seam where the monitor work will need tests.

---

## 7. Risks & rules

- **No behavior change.** If a diff changes observable behavior, it's out of
  scope for Phase 0 — defer to the monitor phases.
- **The closure→ctx conversion (0c) is the only place bugs hide.** Every
  `target` / `pollers` / `uiRef` reference must be rerouted; a missed one
  silently reads a stale/undefined value. Typecheck catches most; the smoke
  matrix catches the rest.
- **Keep `emit`/`pi.sendMessage` semantics identical** (`triggerTurn:true`,
  `deliverAs:"followUp"`, `customType:"ssh-process"`) — downstream rendering keys
  off `customType`/`details.kind`.
- **One PR per sub-phase** (0a…0e), each green, so a regression bisects cleanly.

---

## 8. Then → MONITOR_PLAN.md

After 0e, the monitor decoupling slots in cleanly: `poller.ts` already isolates
the watch engine, `remote-ops`/`transport` give the reusable `fetchDeltaLines` /
`runRemoteCommand` primitives, and the new `monitor/` modules + `ssh_monitor`
tool register through the same `setup*(ctx)` seam.
