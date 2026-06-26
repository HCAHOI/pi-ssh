# Phase 2: Notify Policy + Captures (kill the notification spam)

> Status: 2a ✅ + 2b/2c ✅ implemented (pending live smoke after a pi restart);
> 2d (logWatches policies) optional/not started. Builds on Phase 1
> (`src/monitor.ts`). Scope: this repo (`pi-ssh`). Remote-only.
> North star: a monitor maps *matches* to *notifications* under a configurable
> **NotifyPolicy**, using **named captures**, so `[1/50] DONE … [50/50] DONE`
> becomes one digest / four milestones / a throttled trickle — not 50 pings.

> See `MONITOR_PLAN.md` §3 (types), §5.5 (the pure helper), §7.1 (roadmap), §8
> (phases). This doc is the concrete Phase 2 implementation plan.

---

## 0. Decisions (resolved up front)

1. **Scope = notify policy + captures + template.** `notifyWhen` and the safe
   expression evaluator (MONITOR_PLAN §9.1) are **deferred to Phase 3**, where
   they ship with `probe:`/`file:` sources (their core variables — `value`,
   `exitCode` — come from probes). Phase 2 touches only the
   match→notification mapping.
2. **The policy state machine is a pure, transport-agnostic helper**
   (`src/notify-policy.ts`): no SSH, no I/O, no timers of its own — driven by the
   monitor's existing shared scheduler tick. Unit-testable, upstreamable.
3. **Back-compat is total.** No `notify` field ⇒ `{ mode: "every-match" }` ⇒
   byte-for-byte today's behavior. Old `.pi-ssh-monitors/*.json` and old
   `notify.json` logWatches keep working unchanged.
4. **`digest` flushes on the shared tick**, not a per-monitor timer (we already
   have one shared `setInterval`; adding N timers contradicts §9.2). Flush
   granularity = `POLL_INTERVAL_MS` (3s): an `everyMs` smaller than the tick is
   effectively tick-gated (flushes each tick), not sub-tick.

---

## 1. What exists today (the seam Phase 1 left)

`src/monitor.ts` `runMatch(m, stream, lines)` is the single choke point — every
match calls `emit()` directly:

```ts
function runMatch(m, stream, lines) {
  for (const line of lines) {
    if (m.fired && !m.repeat) break;
    if (!m.re.test(line)) continue;          // ← test(): captures discarded
    m.matchCount++; m.lastMatchAt = Date.now();
    emit(`🔔 ssh_monitor … matched …\n${line}`, { kind:"monitor", … });  // ← 1 match = 1 emit
    if (!m.repeat) { m.fired = true; break; }
  }
}
```

`MonitorState` already carries the aggregation seedlings Phase 2 needs:
`matchCount`, `lastMatchAt`. `sweepMonitor` already runs every tick and already
has a `done` branch (final sweep before teardown) — the natural place to flush a
pending digest. The persisted `MonitorFile` and `CreateMonitorOpts`/
`UpdateMonitorPatch` are the schema surfaces to extend.

**Two changes unlock everything:** `re.test` → `re.exec` (to get
`match.groups`), and `emit(...)` → `gate.onMatch(...)` (to apply policy).

---

## 2. New types

```ts
// src/notify-policy.ts (or co-located in monitor.ts; helper lives in its own file)
export type NotifyPolicy =
  | { mode: "every-match" }                       // default == today
  | { mode: "every-n"; n: number }                // fire once per n matches
  | { mode: "throttle"; minIntervalMs: number }   // min gap between fires
  | { mode: "digest"; everyMs: number }           // batch, flush on a timer
  | { mode: "milestone"; fractions: number[] };   // progress crossings (needs total)

export interface MatchEvent {
  line: string;
  captures: Record<string, string>;   // named groups from re.exec
  matchCount: number;                  // cumulative (post-increment)
  now: number;                         // epoch ms (injected, never Date.now() inside)
}

export interface GateDecision {
  fire: boolean;
  text?: string;                       // rendered notification body when fire=true
  details?: Record<string, unknown>;   // extra emit details (e.g. batchedCount)
}

export interface NotifyGate {
  onMatch(ev: MatchEvent): GateDecision;   // called per matching line
  onTick(now: number): GateDecision;       // called each scheduler tick (digest flush)
  onClose(now: number): GateDecision;      // called when the monitor stops (flush remainder)
}

export function makeNotifyGate(policy: NotifyPolicy, opts: { template?: string }): NotifyGate;
```

`MonitorFile` (persisted) gains two optional fields:

```ts
export interface MonitorFile {
  id; source; pattern; repeat; name?; paused;
  notify?: NotifyPolicy;   // absent ⇒ { mode:"every-match" }
  template?: string;       // absent ⇒ default body
}
```

`MonitorState` gains: `gate: NotifyGate`, `captures: Record<string,string>`
(last match's groups, for `list` display + close text). `CreateMonitorOpts` and
`UpdateMonitorPatch` gain `notify?: NotifyPolicy` and `template?: string`.

---

## 3. Per-policy semantics (what `makeNotifyGate` implements)

All state lives **inside the gate closure** (counters, `lastFireAt`, digest
buffer, fired-milestone set). Pure functions of injected `now`.

| mode | `onMatch` fires when | `onTick` / `onClose` | notes |
|---|---|---|---|
| `every-match` | always | no-op / no-op | identical to Phase 1 |
| `every-n: n` | `matchCount % n === 0` | no-op / **flush partial final batch** | close reports the tail (e.g. 67 under every-n:50 → `17 more, 67 total`) so it is not silently dropped |
| `throttle: ms` | `now - lastFireAt >= ms` (else buffer count) | **flush suppressed tail once `ms` elapsed** / **flush tail** | a firing match reports `suppressedCount`; tick/close surface a backlog that would otherwise hide after burst-then-silence |
| `digest: ms` | never (buffers line + captures) | flush if `now - lastFlush >= ms` & non-empty (window restarts at the next batch's first match) / flush remainder | summary = count + (if `total` captured) progress/ETA + latest line |
| `milestone: f[]` | progress crosses an unfired fraction | no-op / no-op | crossing-only — completion is `ssh_process`'s job, so no synthetic 1.0 on close; progress = `n/total` if `n` captured else `matchCount/total`; **requires a `total` capture** (validated at create) |

**`repeat` interaction:** a non-`every-match` policy implies continuous matching,
so the gate-driven monitor ignores the one-shot `fired` latch when
`policy.mode !== "every-match"`. (`every-match` keeps `repeat` exactly as Phase 1:
`repeat=false` = one-shot.) Encode this as: `effectiveRepeat = repeat || policy.mode !== "every-match"`.

**Template rendering** (pure): `renderTemplate(tpl, captures, extras)` substitutes
`{name}` from `captures` then `extras` (`matchCount`, `count`, `line`, `total`,
`pct`, `eta`). Unknown `{x}` left literal. No template ⇒ today's default body.
ETA (digest/milestone, when `total` known): `rate = count / elapsedMs`,
`eta = (total - matchCount) / rate` → `formatDuration`.

---

## 4. Engine wiring (`src/monitor.ts`)

1. **`runMatch` → capture + gate.** Replace `re.test` with `re.exec`; on a match,
   build a `MatchEvent` (`captures = { ...match.groups }`, `matchCount`, `now`),
   call `m.gate.onMatch(ev)`; if `fire`, `emit(decision.text, { kind:"monitor",
   monitorId, …, ...decision.details })`. Update `m.captures`. Honor
   `effectiveRepeat` for the one-shot break.
2. **`sweepMonitor` tick flush.** After the per-stream delta loop, call
   `const d = m.gate.onTick(Date.now()); if (d.fire) emit(...)`. In the `done`
   branch, before teardown, call `m.gate.onClose(Date.now())` and emit any
   remainder (so an in-flight digest is not lost when the process exits).
3. **`buildState` builds the gate** from `notify ?? { mode:"every-match" }` +
   `template`. Store `notify`/`template` on state for `toFile`/persistence.
4. **`create` validation:** compile pattern (exists); if `policy.mode ===
   "milestone"`, require a `total` named group in the pattern (and `fractions`
   non-empty, sorted, ∈ (0,1]); if `every-n`, `n >= 1`; if `throttle`/`digest`,
   `ms > 0`. Fail fast (no remote round-trip).
5. **`update`** accepts `notify`/`template`; rebuilding the gate **resets policy
   counters** (document this) — pattern/captures unaffected unless pattern also
   changes.
6. **Persistence:** `toFile` writes `notify`/`template`; `rehydrate` reads them
   (absent ⇒ every-match). Gate state itself is **not** persisted (counters reset
   on reconnect, exactly like `fired`/`matchCount` already do today).

`emit` (the sink) and the shared scheduler are **unchanged** — Phase 2 slots the
gate strictly between `runMatch`'s detection and `emit`'s delivery, as
MONITOR_PLAN §5.5 / the REFACTOR seam intended.

---

## 5. Tool surface (`src/tools/monitor.ts`)

`create`/`update` gain two params; compact string forms, parsed to `NotifyPolicy`:

```
ssh_monitor create source=process:<id>:stderr pattern='\[(?<n>\d+)/(?<total>\d+)\] DONE' \
                   notify=digest:5m  template='progress {n}/{total} · ETA {eta}'
ssh_monitor create source=process:<id>:both   pattern='Traceback'  notify=throttle:60s
ssh_monitor create source=process:<id>:stdout pattern='loss=(?<loss>[\d.]+)' notify=every-n:50
ssh_monitor update <mon_id> notify=milestone:0.25,0.5,0.75,1.0
```

`notify` grammar (`parseNotifyPolicy(s)`):
- `every-match` (or omitted)
- `every-n:<int>`
- `throttle:<dur>` / `digest:<dur>` where `<dur>` ∈ `90s | 5m | 1h | <ms>`
- `milestone:<f1>,<f2>,…` (floats in (0,1])

Add a `parseDuration(s)` util (`s/m/h` suffix → ms; bare = ms). `list` gains a
policy column (`digest:5m`, `every-n:50`, `active/paused`, `matches=k`). Update
the tool description + `promptGuidelines` to mention policies as the spam fix.

---

## 6. logWatches sugar (optional sub-phase, back-compat)

To let `ssh_process start --logWatches` also escape spam, extend `WatchSpec`
(`src/types.ts`) with optional `notify?: NotifyPolicy` and `template?: string`,
thread them through `armSugarWatches` → `buildState`, and add the fields to the
`ssh_process` `logWatches` TypeBox schema. Absent ⇒ every-match (unchanged).
Process-sugar monitors persist in `notify.json` (already carries `watches`), so
no new store work. **Gate Phase 2 on the standalone path first** (sub-phase 2a/b);
ship logWatches policies as 2d only if cheap.

---

## 7. Sub-phases (each ends green: tsc baseline unchanged + smoke OK)

- **2a — Pure helper + tests.** ✅ `src/notify-policy.ts` (`makeNotifyGate`,
  `parseNotifyPolicy`, `parseDuration`, `renderTemplate`) + `notify-policy.test.ts`
  (14 cases, `node:test`, run via `npm test` = `tsx --test`). Used `node:test`
  instead of vitest to keep the zero-dependency posture (no install). Pure gate,
  zero SSH.
- **2b/2c — Captures + policy wiring.** ✅ Shipped together (2b alone would leave
  captures populated-but-unused). `runMatch` `test`→`exec` populates `m.captures`;
  `buildState` builds the gate; `onMatch`/`onTick`/`onClose` wired in
  `runMatch`/`sweepMonitor` (tick flush while running, close flush on `done`);
  `notify`/`template` persisted in `MonitorFile` + rehydrated (absent ⇒
  every-match); `create`/`update` validate (milestone needs `(?<total>…)`);
  `ssh_monitor` gained `notify=`/`template=` params + a policy column in `list`.
- **2d — (optional) logWatches policies.** Not started. Extend `WatchSpec` +
  `ssh_process` schema per §6; sugar monitors are every-match until then.

---

## 8. Verification

- `tsc --noEmit`: must stay at the 19-error version-skew baseline (+1 per new
  `registerTool`; no new error *class*).
- **Unit (new):** `notify-policy.test.ts` — every-n boundary, throttle suppress +
  resume count, digest buffer/flush/close, milestone crossing & total-from-capture,
  template substitution + ETA, default/every-match parity.
- **Smoke (live box, after pi restart):** a job emitting `[k/N] DONE` →
  `notify=digest:10s` yields **one** rolling summary per window, not N pings;
  `milestone:0.5,1.0` fires twice; `every-match` (no policy) still pings per line;
  digest remainder flushes on completion; reconnect re-arms with counters reset.

---

## 9. Risks & rules

- **No behavior change without an opt-in.** Every default path = `every-match`.
  A regression here means a monitor that used to ping per line stopped — gate the
  smoke test on the no-policy path explicitly.
- **Gate purity.** `makeNotifyGate` must never call `Date.now()`/SSH/`emit`
  internally — `now` is injected and decisions are returned, so it stays testable
  and the engine owns all I/O.
- **Digest loss on teardown.** The `onClose` flush in `sweepMonitor`'s `done`
  branch is mandatory — without it a job that finishes mid-window drops its last
  digest. Cover it in the smoke test.
- **Counters are ephemeral** (reset on reconnect, like `fired`/`matchCount`
  today). Don't persist gate state; persist only the policy.

---

## 10. One-line summary

Insert a pure, tick-driven **NotifyGate** between `runMatch`'s `re.exec`
(captures) and `emit`'s delivery, configured by a persisted **NotifyPolicy**
(`every-match | every-n | throttle | digest | milestone`) + a capture-aware
**template** — killing per-line spam with zero new I/O, full back-compat, and the
project's first unit tests; `notifyWhen` + sources stay in Phase 3.
