# Plan: Decouple Monitoring from Processes in `pi-ssh`

> Status: design / not yet implemented (Phase 0 split is **DONE** — see below).
> Scope: this repo (`pi-ssh`). Remote-only.
> North star: monitoring is a first-class, runtime-manageable capability that
> binds to *any* remote signal source (process stream / log file / probe
> command), under a configurable notify policy — not a frozen `ssh_process start`
> parameter.

> **Prerequisite done.** The codebase is now split into `src/` modules
> (`REFACTOR_SPLIT_PLAN.md`, runtime-verified). The watch engine lives in
> `src/poller.ts` (`createPollerManager(pi)`); the sink is `src/notify.ts`
> (`sendProcessMessage`); subsystems register via `setup*(ctx: SshContext)`. The
> "grounded in `index.ts`" references below now map to `src/poller.ts` +
> `src/process-queries.ts` + `src/tools/process.ts` (see the mapping in §1).

---

## 0. Decision: build natively, do NOT depend on `@aliou/pi-processes`

`@aliou/pi-processes` acquires signal from local Node `ChildProcess` streams
(`child.stdout.on("data")`). `pi-ssh` acquires signal by polling remote log
files (`tail -c +offset` over SSH, every `POLL_INTERVAL_MS`). **The
signal-acquisition layer — where all the work is — has zero overlap.** The only
transport-agnostic piece is the notify-policy state machine, which pi-processes
does not even export (it's inlined in `matchWatches`).

`pi-ssh` today has **zero runtime dependencies** (only peer deps on the pi
framework). Taking a hard dep on a local-only package to reuse ~200 lines of pure
logic is a bad trade. → **Implement monitoring natively**, reusing this repo's
own primitives. Keep the notify-policy logic in a self-contained, transport-
agnostic helper so it stays testable and could be upstreamed later.

---

## 1. What exists today (now in `src/poller.ts` + `src/tools/process.ts`)

The monitoring pipeline already has three layers — they're just welded together
and bound 1:1 to a process job. After the Phase 0 split, all of this lives inside
`createPollerManager(pi)` in `src/poller.ts` (engine) and the `ssh_process` tool
in `src/tools/process.ts` (schema), with the sink in `src/notify.ts`.

| Layer | Symbols (file) | Notes |
|-------|-------------------|-------|
| **Source** | `fetchDeltaLines(p, stream)` (`poller.ts`) | `tail -c +offset` of `<dir>/{stdout,stderr}.log`; advances byte offset to last `\n`. Only source = a managed process's two streams. |
| **Match/Engine** | `PollerState.watches: WatchState[]`, `runWatches()`, `sweepWatches()`, `tick()` (`poller.ts`) | Watches live **inside** the per-job poller (`createPollerManager` closure). `tick` (one `setInterval` per job, 3s) does completion-detection **and** watch-sweeping together. Watch state = `fired: boolean`. |
| **Sink** | `sendProcessMessage(pi, content, details)` (`notify.ts`) → `pi.sendMessage({customType:"ssh-process"}, {triggerTurn:true, deliverAs:"followUp"})` | Re-engages the agent. The poller's internal `emit` + the sync watcher both delegate to it. Used for watch hits (`kind:"watch"`) and completion (`kind:"completion"`). |
| **Config/persist** | `notify.json` per job dir, `notified` sentinel, `poller.rehydrate()` (`poller.ts`) | Watches set only at `start` (and re-set at `attach`). Persisted so pollers re-arm after reconnect/restart; offsets seek to EOF on rehydrate so history doesn't re-fire. |
| **Schema** | `ssh_process` tool (`tools/process.ts`), `logWatches` param, `WatchSpec`, `buildWatchStates()` (`poller.ts`) | `logWatches` is a parameter of `start`/`attach`. |

### The coupling, precisely

1. `PollerState.watches` — watches are a field of the poller, which is 1:1 with a
   process. No standalone monitor object, no N:M.
2. Watches are settable only at `start`/`attach`. No mid-run add/change/remove.
3. `tick()` fuses process-completion and log-watching into one loop.
4. The only source is `process:<id>`'s stdout/stderr. No file/probe/resource.
5. Stateless per-line match → one `emit` per hit → 50 `[N/50] DONE` = 50
   notifications. `WatchState` has nowhere to keep aggregation state and no
   notify policy.

---

## 2. Target design: Source / Monitor / Sink

```
Source (remote signal producer) ──┐
                                  ├─< Binding N:M >── Monitor ── Sink
Source ───────────────────────────┘                 (rule +     (emit →
                                                      state +     sendMessage)
                                                      policy)
```

- **Source** — addressed by id:
  - `process:<id>:stdout|stderr|both` → existing `fetchDeltaLines`
  - `file:<remotePath>` → same tail-by-offset logic on an arbitrary remote file
  - `probe:<cmd>` → run a remote command every interval, read stdout/exit code
- **Monitor** — standalone object, own id (`mon_…`), own lifecycle and **state**
  (captures, matchCount, lastFireAt, digest buffer, silence timer). Holds the
  rule + notify policy. Lives in a `Map<string, MonitorState>`, **not** inside
  `PollerState`.
- **Sink** — keep `emit()` as the single sink initially (agent notification);
  leave room for severity/channel later.
- **Binding** — a monitor lists one or more sources. N:M.

Process **completion** alerts (`alertOnSuccess/Failure/Kill`) stay where they are
— that's process-lifecycle, not log-monitoring. Only **watches** get decoupled.
(Conceptually completion is "a built-in monitor on a `status` source"; we don't
need to refactor it to get the win, and keeping it put de-risks migration.)

---

## 3. New types (in `index.ts`, near current `WatchSpec`/`WatchState`)

```ts
// Source binding for a monitor (remote).
type MonitorSource =
  | { kind: "process"; procId: string; stream: "stdout" | "stderr" | "both" }
  | { kind: "file"; path: string }
  | { kind: "probe"; command: string; intervalMs: number };

// Transport-agnostic notify policy (own helper module — see §5).
type NotifyPolicy =
  | { mode: "every-match" }                      // == today's behavior
  | { mode: "every-n"; n: number }
  | { mode: "throttle"; minIntervalMs: number }
  | { mode: "digest"; everyMs: number }
  | { mode: "milestone"; fractions: number[] };  // uses captured n/total

interface MonitorSpec {                          // persisted (JSON)
  id: string;
  sources: MonitorSource[];
  pattern: string;                               // regex; may have named groups
  notifyWhen?: string;                           // optional expr over captures
  expectEveryMs?: number;                        // silence/stall detection
  notify: NotifyPolicy;                          // default { mode:"every-match" }
  template?: string;                             // text; may reference captures
}

interface MonitorState extends MonitorSpec {
  re: RegExp;
  off: Record<string, number>;                   // per-source byte offset (sourceId -> off)
  // runtime aggregation state (the thing WatchState has nowhere to put):
  fired: boolean;
  matchCount: number;
  lastFireAt: number | null;
  lastMatchAt: number | null;
  captures: Record<string, string>;
  digestBuffer: string[];
  timer: NodeJS.Timeout | null;
  busy: boolean;
}
```

`WatchState`/`WatchSpec` are kept for the legacy `ssh_process logWatches` sugar,
which desugars into a `MonitorSpec` bound to `process:<id>` (§6).

---

## 4. New tool: `ssh_monitor` (sibling to `ssh_process`)

A dedicated tool is cleaner than overloading `ssh_process`, and mirrors the
existing `ssh_process attach` ergonomics.

```
ssh_monitor create  --source process:<id>:stderr --pattern '\[(?<n>\d+)/(?<total>\d+)\] DONE' \
                    --notify digest:5m --template 'progress {n}/{total}'   → mon_1
ssh_monitor create  --source file:/var/log/train.log --pattern 'Traceback' --notify throttle:60s
ssh_monitor create  --source 'probe:nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits' \
                    --intervalMs 60000 --notifyWhen 'value==0' --consecutive 3
ssh_monitor list
ssh_monitor update  mon_1 --notify milestone:0.25,0.5,0.75,1.0
ssh_monitor pause   mon_1
ssh_monitor resume  mon_1
ssh_monitor remove  mon_1
```

Actions: `create | list | update | pause | resume | remove | attach` (TypeBox
union, matching the `ssh_process` action pattern).

---

## 5. Engine changes (reuse, don't duplicate)

1. **Generalize `fetchDeltaLines`** to take `(file, off)` instead of `(poller,
   stream)`, returning `{ lines, nextOff }`. The process-stream path and the new
   `file:` source both call it. (Currently it's hard-bound to
   `p.dir/<stream>.log` and mutates `p.off`.)
2. **Monitor scheduler** — one shared `setInterval` (or per-monitor timer, like
   pollers) that for each live monitor: sweeps each source's delta lines, runs
   the rule, updates `MonitorState`, then applies the notify policy before
   `emit()`. Reuse the `busy`/`finished` guard and swallow-errors-per-tick
   discipline from `tick()`.
3. **`probe:` sources** reuse `runRemoteCommand(target, command, {login:false})`
   on `intervalMs`; predicate over `{ stdout, exitCode }`.
4. **Silence detection** — in the same sweep, if `expectEveryMs` set and
   `now - lastMatchAt > expectEveryMs`, fire once.
5. **NotifyPolicy + captures = a pure, transport-agnostic helper** (e.g. a
   `makeNotifyGate(policy)` returning `(event) => { fire: bool, text? }`). No SSH,
   no I/O — unit-testable, upstreamable.
6. **Sink unchanged** — keep `emit()`. Monitor events carry `kind:"monitor"`,
   `monitorId`, `captures` in `details`.

What we explicitly reuse from current code: `fetchDeltaLines` (generalized),
`runRemoteCommand`, `emit`/`pi.sendMessage`, the EOF-seek-on-rehydrate trick, the
`notified`/idempotency pattern, the per-tick error-swallow + `busy` guard.

---

## 6. Persistence & rehydrate

- Process-bound monitors created via `ssh_process logWatches` sugar keep living
  in the job's `notify.json` (back-compat; desugared to a `MonitorSpec` at load).
- Standalone monitors get a target-level store: `<processRoot>/../.pi-ssh-monitors/<mon_id>.json`
  (parallel to `.pi-ssh-processes/`), since they can bind to non-process sources.
- Extend `rehydratePollers()` (or add `rehydrateMonitors()`) to re-arm monitors
  on (re)connect, seeking each source offset to EOF so history doesn't re-fire —
  exactly the existing poller rule. Reuse the python-scan approach already used
  for jobs.

---

## 7. Capability roadmap (each maps to a field, never a new `start` flag)

Priority by value/effort:

1. **Notify policy (throttle/every-n/digest/milestone) + captures** — kills the
   50-notification spam. Pure helper (§5.5). *Do first.*
2. **Runtime lifecycle** (`ssh_monitor create/update/pause/remove` on a running
   job) — add/change watches with zero restart. The core decoupling win.
3. **Silence/stall detection** (`expectEveryMs`) — catches hangs that never exit,
   which completion alerts can't see. One timer.
4. **`file:` and `probe:` sources** — monitor arbitrary logs / GPU / `ps` / ports
   / checkpoints, including things `ssh_process` didn't start.
5. **Digest + ETA** — rolling `23/50 done · 1.4/min · ETA ~19m` from captures.
6. **Routing/composition** — severity channels, mute-progress-on-error, sequence
   combinators. Deferred.

---

## 8. Migration phases (each independently shippable, back-compat)

- **Phase 0 — Split & reorganize `index.ts` (prerequisite).** ✅ **DONE** &
  runtime-verified. `index.ts` 2858 → 400; 21 modules; watch engine in
  `poller.ts`, sink in `notify.ts`, `setup*(ctx)` seam. Detail +
  outcome in `REFACTOR_SPLIT_PLAN.md`.
- **Phase 1 — `ssh_monitor` tool + standalone monitor store + rehydrate.** ✅ **DONE.**
  Process-source only. Mid-run create/update/pause/resume/remove now possible.
  Watches lifted out of `PollerState` into a standalone `MonitorManager`
  (`src/monitor.ts`) with one shared scheduler; the poller is completion-only.
  `ssh_process --logWatches` desugars to process-bound monitors (`<procId>#w<i>`,
  persisted in the job's `notify.json`); standalone monitors persist to
  `.pi-ssh-monitors/<id>.json`. Both rehydrate on (re)connect, seeking each source
  to EOF. See §9 decisions + accepted trade-offs below.
- **Phase 2 — Notify policy + captures (roadmap 1).** Spam fix ships.
- **Phase 3 — `file:`/`probe:` sources + silence (roadmap 3,4).**
- **Phase 4 — digest/ETA, routing (roadmap 5,6).**

`ssh_process start --logWatches` stays as sugar throughout, desugaring to a
process-bound `MonitorSpec` with `{ mode:"every-match" }`.

---

## 9. Open questions

Phase 1 decisions (resolved):

1. **Expression evaluator** for `notifyWhen`/`probeWhen` — **deferred to Phase 2/3**
   (Phase 1 has no `notifyWhen`/`probe`). Will be a tiny safe parser over a fixed
   variable set; no `eval`.
2. **One shared monitor timer vs per-monitor `setInterval`** — **one shared
   `setInterval`** that kicks each monitor's `sweepMonitor` concurrently under a
   per-monitor `busy` guard (single timer, no head-of-line blocking).
3. **Monitor ↔ process lifecycle** — **yes, auto-remove.** A process-bound
   monitor tears down (and deletes its persisted file) when its process dir is
   `gone`; on `done` it does one final sweep then stops. `ssh_process kill` calls
   `monitors.stopForProcess` to suppress spurious trailing alerts (mirrors
   `poller.stopPoller`).
4. **Group sources (N:M)** — **deferred** (Phase 1 = one process source per
   monitor).
5. **`ssh_monitor` vs extending `ssh_process`** — **separate `ssh_monitor` tool.**

Accepted Phase 1 trade-offs (revisit in later phases):

- **Ordering:** the completion poller and each monitor run on independent timers,
  so a process's final matching line and its completion alert may arrive in either
  order. Trailing lines are never lost (the monitor sweeps the delta when it
  observes `done`, before teardown) — only relative ordering is unguaranteed.
- **SSH amplification:** each monitor independently issues `procStatus` +
  per-stream `fetchDelta` every tick, so k logWatches on a job cost ~k× the SSH
  round-trips the old single-poller sweep did. Fine at Phase 1 scale; a future
  optimization can coalesce status/delta across monitors sharing a procId+stream.
- **Sugar-monitor mutability:** `ssh_monitor update` rejects process-sugar
  monitors (their source of truth is `notify.json`); `pause/resume/remove` on them
  are session-scoped and reset on the next rehydrate.

---

## 10. One-line summary

Lift watches out of `PollerState`/`notify.json` into a first-class, runtime-
manageable **Monitor** that binds (N:M) to pluggable remote **Sources**
(process / file / probe) and fires through the existing `emit` **Sink** under a
configurable **NotifyPolicy** — built natively in `pi-ssh`, reusing
`fetchDeltaLines` / `runRemoteCommand` / `emit`, no external dependency.
