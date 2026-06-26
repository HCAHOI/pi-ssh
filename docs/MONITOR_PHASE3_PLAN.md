# Phase 3: Pluggable Sources (`file:` / `probe:`) + Silence + `notifyWhen`

> Status: design / not started. Builds on Phase 1 (decoupled `ssh_monitor`) and
> Phase 2 (notify policy + captures + first-class logWatches), both shipped and
> runtime-verified. Scope: this repo (`pi-ssh`). Remote-only.
> North star: a monitor binds to **any** remote signal source — a managed
> process stream (today), an **arbitrary remote file**, or a **periodic probe
> command** — and fires through the existing NotifyGate, optionally gated by a
> tiny safe **`notifyWhen`** expression. Plus **silence/stall detection** for
> hangs that never exit.

---

## 0. Operating context for the implementer (read this first)

You can start from a cold context. Everything you need:

- **Repo:** `/Users/chiyuh/.pi/agent/extensions/ssh`. Zero runtime deps by design
  (only peer deps on the pi framework). Do NOT add a runtime dependency.
- **Typecheck (the gate):** `cd <repo> && npx -y -p typescript@latest tsc --noEmit`.
  There is a **known baseline of 20 errors** (19 pre-existing version-skew vs the
  installed pi `.d.ts` — `TS2322` details-missing, `TS2554` arg-arity, `TS18047`
  child-null — plus exactly one `TS2322` per `registerTool` execute). Your gate is
  **"no NEW error signature class beyond those"**. Each new tool you register adds
  one more `TS2322` of the same class; that's expected. Confirm the set with:
  `npx -y -p typescript@latest tsc --noEmit 2>&1 | grep -oE 'src/[^ ]+ error TS[0-9]+' | sort`.
- **Unit tests:** `npx -y tsx --test "src/**/*.test.ts"` (also `npm test`). Tests
  use Node's built-in `node:test`/`node:assert` (no vitest — keep it that way).
  Pure helpers (`notify-policy.ts`, and the new `expr.ts`) are the testable core;
  the engine (`monitor.ts`) is SSH-bound and validated by live smoke.
- **Module layout** (`src/`): `monitor.ts` (the monitor engine — `createMonitorManager`),
  `notify-policy.ts` (pure notify gate), `poller.ts` (ssh_process completion poller),
  `process-queries.ts` (`processRoot`, list/kill/clear), `ssh/transport.ts`
  (`runRemoteCommand`), `notify.ts` (`sendProcessMessage` sink), `context.ts`
  (`SshContext`), `tools/*.ts` (`setup*(ctx)` tool registrations), `index.ts`
  (wiring). New subsystems register through `setup*(ctx: SshContext)`.
- **Review gate (mandatory):** after each shippable sub-phase, spawn a
  fresh-context sub-agent (the `Agent` tool, `general-purpose`) to review the diff
  for correctness / race conditions / back-compat / shell-quoting. Fix
  blockers/concerns, re-review until clean, THEN commit. One commit per
  sub-phase; push when the phase is done. Commit style: `[feat]/[fix]/[docs]`
  prefix, imperative subject, body explaining what+why.
- **Live smoke:** a real GPU box is available — connect with the `ssh_connect`
  tool, target `"-p 40227 root@connect.singapore-a.gpuhub.com"`. **pi loads
  extensions at session start**, so after code changes the running tools reflect
  the build loaded at session start — you must **restart/resume pi** to smoke-test
  a new build. Exercise the new source kinds end-to-end and confirm notifications
  fire. Clean up test artifacts (`ssh_process clear`, remove
  `.pi-ssh-monitors`/`.pi-ssh-processes` test dirs).
- **Production-quality bar:** no behavior gaming, no unjustified magic numbers,
  fail fast with clear errors, reuse existing primitives, keep pure logic in
  transport-agnostic, unit-tested helpers.

---

## 1. Current shape (the seam Phases 1–2 left), grounded in `src/monitor.ts`

The monitor engine is **process-source-only** today. The relevant symbols:

```ts
// The source union — Phase 3 widens this:
export type MonitorSource = { kind: "process"; procId: string; stream: "stdout" | "stderr" | "both" };  // monitor.ts:25

interface MonitorState {            // monitor.ts:41 — carries process-specific fields:
  id; kind: "standalone" | "legacy"; source: MonitorSource; pattern; re; repeat; name?;
  target; dir;                      // dir = `${processRoot(target)}/${procId}` — process-specific
  off: Record<string, number>;     // per-STREAM byte offsets — process-specific shape
  paused; fired; matchCount; lastMatchAt;
  notify: NotifyPolicy; template?; gate: NotifyGate; captures; busy; finished;
}
```

Process-specific logic that Phase 3 must generalize (all in `monitor.ts`):
- `procStatus(t, dir) → "running"|"done"|"gone"` (`:240`) — pid-based liveness.
- `eofOffsets(t, dir, stream)` (`:248`) — `wc -c` per stream for EOF seek.
- `fetchDelta(t, file, off) → {lines, nextOff}` (`:197`, **already source-agnostic** —
  takes an explicit file + offset; reuse verbatim for `file:`).
- `sweepMonitor(m)` (`:300`) — the per-tick engine. Currently: `procStatus` →
  `gone`⇒remove / `done`⇒`onClose`+remove / else sweep `streamsOf(source.stream)`
  via `fetchDelta` → `runMatch` → `gate.onTick`. **Hard-wired to process semantics.**
- `create(t, opts)` (`:412`) — `procStatus` existence check (`gone`/`done` reject) +
  `eofOffsets`. Process-specific.
- `createForProcess(...)` (`:433`) — logWatches sugar; stays process-only.
- `rehydrate(t)` (`:544`) — a `python3` scan returns `{monitors:[b64 files], procs:{id→{running,outSize,errSize,notify}}}`; standalone monitors rebuilt from files (EOF via `proc.outSize/errSize`), legacy shim from running jobs' `notify.json.watches`. **Offset seeding is process-specific.**
- `parseSource(raw)` (`:172`) / `sourceLabel(s)` (`:167`) — only parse/format `process:<id>[:stream]`.
- Lifecycle: process monitors **auto-remove + delete their file** on `gone`/`done`
  (`removeInternal`). The shared scheduler is one `setInterval` (POLL_INTERVAL_MS =
  3000) kicking each monitor's `sweepMonitor` under a per-monitor `busy` guard.

Reusable primitives (no change): `fetchDelta`, `runRemoteCommand`
(`ssh/transport.ts:101`), the NotifyGate (`notify-policy.ts` `makeNotifyGate`),
the `emit` sink, `persist`/`deleteFile`/`monitorRoot`, the per-tick
error-swallow + `busy` discipline.

---

## 2. Decisions (resolve up front)

1. **Source abstraction = a small `SourceDriver` per kind, not a `switch` in
   `sweepMonitor`.** The three kinds differ in liveness, offset shape, and poll
   cadence enough that inlining bloats the engine. Define a driver interface;
   `sweepMonitor` becomes source-agnostic orchestration. (See §4.) Refactor the
   existing process logic into `ProcessSourceDriver` **with zero behavior change**
   as sub-phase 3a — gate it on the full Phase-1/2 smoke before adding new kinds.
2. **`notifyWhen` evaluator = a tiny safe expression module (`src/expr.ts`), no
   `eval`/`Function`.** Tokenizer + Pratt parser → AST → `evalAst(env)`. Variable
   set: named `captures`, `value`, `exitCode`, `matchCount`, `elapsedMs` (numbers
   where parseable, else strings). Operators: `== != < <= > >= && || !`, `+ - * /`,
   parens, numeric/string literals. Pure, unit-tested, upstreamable. This is
   MONITOR_PLAN §9.1.
3. **Where `notifyWhen` applies:** **required-ish for `probe:`** (the predicate
   over `{value, exitCode}` that decides a probe "fired"), **optional for
   `process:`/`file:` matches** (an extra filter over captures after the regex
   matches). Absent ⇒ regex match alone fires (today's behavior).
4. **Lifecycle by source kind:**
   - `process:` — unchanged (auto-remove on gone/done).
   - `file:` — does NOT auto-remove when the file is missing (logrotate / not-yet-created
     are normal); it just polls (offset stays, re-reads on reappear). Removed only
     by explicit `ssh_monitor remove` or `stopAll`/disconnect. Document this.
   - `probe:` — never self-removes; explicit remove only.
   So **non-process monitors are long-lived** — make sure `ssh_monitor list`/`remove`
   and rehydrate handle them without a bound process.
5. **`probe:` cadence:** the probe has its own `intervalMs` (may differ from the
   3 s scheduler tick). The driver runs the command only when
   `now - lastRunAt >= intervalMs`; the shared tick just polls it. Add
   `consecutive?: number` (default 1) — fire only after N consecutive
   `notifyWhen`-true polls (debounce, e.g. GPU util==0 for 3 ticks). MONITOR_PLAN §4.
6. **Silence/stall detection** = a monitor-level `expectEveryMs?`. In the same
   sweep, if `expectEveryMs` is set and `now - lastMatchAt > expectEveryMs`, fire
   **once** (a `kind:"silence"` event), re-armed when the next match arrives.
   Orthogonal to source kind. MONITOR_PLAN §7.3.

---

## 3. New types

```ts
// monitor.ts — widen the union (keep `process` first/unchanged):
export type MonitorSource =
  | { kind: "process"; procId: string; stream: "stdout" | "stderr" | "both" }
  | { kind: "file"; path: string }                                   // absolute remote path
  | { kind: "probe"; command: string; intervalMs: number; consecutive?: number };

// Per-source runtime state (replaces the bare process `dir`/`off` assumptions).
// Keep it a discriminated payload on MonitorState, e.g. `srt: SourceRuntime`:
type SourceRuntime =
  | { kind: "process"; dir: string; off: Record<string, number> }   // today's fields
  | { kind: "file"; off: number }
  | { kind: "probe"; lastRunAt: number; hits: number /*consecutive*/ };

// MonitorState gains:
//   notifyWhen?: string; notifyWhenAst?: Ast;   // compiled once
//   expectEveryMs?: number; silenceFired: boolean;
//   srt: SourceRuntime;                          // replaces dir/off
// MonitorFile (persisted) gains: notifyWhen?, expectEveryMs? (source already serializes).
```

`src/expr.ts`:
```ts
export type Ast = …;                       // internal node union
export function parseExpr(src: string): Ast;          // throws on syntax error (fail fast at create)
export type EvalEnv = Record<string, string | number>;
export function evalExpr(ast: Ast, env: EvalEnv): boolean;   // truthy coercion at top level
export function evalExprSource(src: string, env: EvalEnv): boolean;  // parse+eval convenience (tests)
```

---

## 4. The `SourceDriver` abstraction (3a — pure refactor, zero behavior change)

```ts
// monitor.ts (or src/sources.ts if it keeps monitor.ts lean)
interface PollResult {
  matches?: { stream?: "stdout" | "stderr"; line: string }[];  // lines to run the regex on
  probe?: { stdout: string; exitCode: number };                // probe sample (predicate path)
  ended?: boolean;     // source finished for good (process done) → final sweep then remove
  vanished?: boolean;  // source gone (process dir removed) → remove now, no final sweep
}
interface SourceDriver {
  /** create-time existence/validity check; throw a clear error to reject. */
  validate(t: SshTarget, source: MonitorSource): Promise<void>;
  /** initial runtime (seek EOF for process/file; zero clocks for probe). */
  initRuntime(t: SshTarget, source: MonitorSource): Promise<SourceRuntime>;
  /** runtime from persisted/rehydrated state, seeking EOF so history doesn't re-fire. */
  rehydrateRuntime(t: SshTarget, source: MonitorSource): Promise<SourceRuntime>;
  /** one poll; mutates srt offsets/clocks; returns what happened this tick. */
  poll(t: SshTarget, m: MonitorState): Promise<PollResult>;
}
const DRIVERS: Record<MonitorSource["kind"], SourceDriver> = { process, file, probe };
```

`sweepMonitor(m)` becomes source-agnostic:
```
if (m.paused || m.busy || m.finished) return; m.busy = true;
try {
  const r = await DRIVERS[m.source.kind].poll(t, m);
  if (r.vanished) { await removeInternal(m); return; }
  if (!(m.fired && isOneShot(m))) {
    for (const {stream, line} of r.matches ?? []) runMatch(m, stream, [line]);  // regex + notifyWhen filter
    if (r.probe) evalProbe(m, r.probe);                                         // notifyWhen predicate + consecutive
  }
  const now = Date.now();
  if (m.expectEveryMs && m.lastMatchAt && now - m.lastMatchAt > m.expectEveryMs && !m.silenceFired) {
    m.silenceFired = true; emitSilence(m, now);
  }
  if (r.ended) { const c = m.gate.onClose(now); if (c.fire) emitMonitor(m, c); m.finished = true; await removeInternal(m); }
  else { const tk = m.gate.onTick(now); if (tk.fire) emitMonitor(m, tk); }
} catch { /* swallow per tick */ } finally { m.busy = false; }
```

- **ProcessSourceDriver:** lift today's `procStatus`+`streamsOf`+`fetchDelta`+`eofOffsets`
  verbatim. `poll` returns `vanished` on `gone`, `ended` on `done`, else
  `matches` from both streams. **3a ships only this driver; behavior identical.**
- **FileSourceDriver (3b):** `validate` = optional `test -e` (warn-not-fail if
  missing, per §2.4). `initRuntime`/`rehydrateRuntime` = `off = wc -c path` (EOF).
  `poll` = `fetchDelta(t, path, srt.off)` → `matches` (no stream). Never
  `ended`/`vanished`.
- **ProbeSourceDriver (3c):** `validate` = none (or a dry `command` run?). `poll`:
  if `now - srt.lastRunAt < intervalMs` return `{}`; else run
  `runRemoteCommand(t, source.command, {login:false, timeout:…})`, set
  `lastRunAt`, return `{ probe: { stdout: r.stdout.toString().trim(), exitCode: r.code ?? -1 } }`.

`evalProbe(m, {stdout, exitCode})`: build `env = { value: numOr(stdout), valueStr: stdout, exitCode, matchCount: m.matchCount, elapsedMs: … }`; if `evalExpr(m.notifyWhenAst, env)` → `srt.hits++` else `srt.hits = 0`; when `hits >= consecutive`, treat as a match → `m.matchCount++`, feed `gate.onMatch` (captures `{value, exitCode}`), reset `hits` if one-shot.

`runMatch` (process/file): after `re.exec`, if `m.notifyWhenAst` is set, build
`env` from captures (+ `matchCount`, `elapsedMs`) and skip the line when
`evalExpr` is false.

---

## 5. Tool surface (`src/tools/monitor.ts`)

Extend `parseSource` and the `ssh_monitor` schema:
```
ssh_monitor create source=file:/var/log/train.log pattern='Traceback' notify=throttle:60s
ssh_monitor create source='probe:nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits' \
                    intervalMs=60000 notifyWhen='value==0' consecutive=3 name=gpu-idle
ssh_monitor create source=process:<id>:stderr pattern='loss=(?<loss>[\d.]+)' notifyWhen='loss > 10'
ssh_monitor create source=process:<id>:both pattern='epoch' expectEveryMs=300000   # stall if silent 5m
```
New params (all optional): `intervalMs` (probe), `consecutive` (probe),
`notifyWhen` (string → `parseExpr`, validated at create), `expectEveryMs` (number).
`parseSource` must handle `file:<path>` (path may contain `:`? no — file paths
here are absolute, and `probe:` takes the rest verbatim incl. spaces/colons).
Note: `probe:` parsing must NOT split on `:` like process does — treat everything
after `probe:` as the command. `list` adds a source-kind-aware column.

---

## 6. Persistence & rehydrate

- `MonitorFile` already serializes `source`; add `notifyWhen?`, `expectEveryMs?`,
  and the widened source carries its own fields. `pattern` is optional for
  `probe:` (predicate-only) — make `pattern` optional in `MonitorFile`/create for
  probe sources (process/file still require it).
- `rehydrate` (`monitor.ts:544`): the standalone-file scan stays, but offset
  seeding moves into `DRIVERS[kind].rehydrateRuntime` (process: from the python
  scan's `outSize/errSize`; file: a `wc -c` per file; probe: zero clocks). The
  python scan can stay process-focused for `procs`; file/probe offsets are cheap
  per-monitor `wc -c`/no-op done in TS after decoding each file. Keep the per-item
  try/catch (a corrupt/invalid monitor file must not abort the rest).
- A `file:`/`probe:` monitor has **no bound process**, so the "drop the file when
  its process is gone" branch must be guarded to process sources only.

---

## 7. Sub-phases (each ends green: tsc baseline + tests + smoke)

- **3a — SourceDriver refactor (no behavior change).** Introduce the driver
  interface + `ProcessSourceDriver`; move `dir`/`off` into `SourceRuntime`;
  `sweepMonitor`/`create`/`rehydrate`/`createForProcess` delegate to the process
  driver. **Smoke must reproduce the full Phase 1/2 matrix identically** (logWatch
  milestone/digest, completion, auto-remove). Commit only when byte-identical.
- **3b — `file:` source.** Driver + `parseSource` + schema + rehydrate offset.
  Smoke: `ssh_monitor create source=file:/tmp/x.log pattern=…`, append lines, see
  notifications; reconnect re-arms from EOF.
- **3c — `probe:` source + `src/expr.ts`.** Ship `expr.ts` with **unit tests
  first** (operators, precedence, string/number coercion, `notifyWhen` over
  `{value, exitCode}`, syntax-error throws), then the driver + `notifyWhen`
  wiring + `consecutive`. Smoke: a probe like `echo 0` / `nvidia-smi` with
  `notifyWhen='value==0' consecutive=2`.
- **3d — silence/stall (`expectEveryMs`).** The one-shot silence fire + re-arm.
  Unit-test the timing logic if extractable; smoke a monitor on a stalled job.

Each sub-phase: tsc gate → unit tests → fresh-context review → fix → commit.

---

## 8. Verification

- `tsc --noEmit`: baseline unchanged (20 + one TS2322 per any new `registerTool`;
  Phase 3 likely adds no new tool, so stays 20).
- **Unit (new):** `expr.test.ts` — precedence (`a && b || c`, `!`, comparisons,
  arithmetic), numeric vs string coercion, undefined-variable handling, parse
  errors; plus any extractable silence/consecutive logic.
- **Smoke (live, after pi restart):** the per-sub-phase scenarios above. Confirm
  `process:` behavior is unchanged (regression guard), `file:` fires on appended
  lines and re-arms from EOF on reconnect, `probe:` fires per `notifyWhen` +
  `consecutive`, `expectEveryMs` fires once on a stall and re-arms.

---

## 9. Risks & invariants

- **Back-compat is sacred for `process:`.** 3a must be a pure refactor — if any
  observable process-monitor behavior changes, it's a bug. The Phase 1/2 smoke
  matrix is the regression net.
- **No `eval`.** `expr.ts` is a hand-written parser/evaluator over a fixed
  variable set. Reject unknown identifiers at parse or treat as undefined → false;
  never execute arbitrary code. Probe `command` is the user's own shell (already
  trusted, like `ssh_bash`), but `notifyWhen` is data, not code.
- **Shell quoting:** every remote command uses `shQuote` (probe command is run via
  `runRemoteCommand(t, source.command, {login:false})` — it's a full command, not
  interpolated into another string; keep it that way).
- **Long-lived non-process monitors:** ensure `stopForProcess`, the
  gone/done auto-remove, and rehydrate's "drop file if process gone" only apply to
  `process:` sources (guard on `source.kind`), or you'll wrongly reap file/probe
  monitors.
- **Probe load:** one SSH exec per probe per `intervalMs`; document and keep
  `intervalMs` sane (reject `< POLL_INTERVAL_MS`? or allow and tick-gate).

---

## 10. One-line summary

Generalize the monitor engine behind a per-kind `SourceDriver` (process / file /
probe), add a pure no-`eval` `notifyWhen` expression evaluator and one-shot
silence detection, so a monitor can watch any remote file, poll any command
under a predicate, and catch hangs — reusing `fetchDelta` / `runRemoteCommand` /
the NotifyGate, with `process:` behavior byte-unchanged.
