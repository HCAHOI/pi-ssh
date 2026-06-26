// ---------------------------------------------------------------------------
// Monitor subsystem: first-class, runtime-manageable log monitors decoupled
// from ssh_process. A monitor binds to a remote signal Source (Phase 1: a
// process stream), matches a regex per delta line, and fires through the
// shared notification Sink. State lives in its own Map (not inside the poller),
// is mutable at runtime (create/update/pause/resume/remove), and rehydrates on
// reconnect/restart. See docs/MONITOR_PLAN.md.
// ---------------------------------------------------------------------------

import { randomBytes } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { SshTarget, WatchSpec } from "./types";
import { shQuote } from "./utils";
import { runRemoteCommand } from "./ssh/transport";
import { processRoot } from "./process-queries";
import { sendProcessMessage } from "./notify";
import { makeNotifyGate, policyLabel, type GateDecision, type NotifyGate, type NotifyPolicy } from "./notify-policy";
import { DRIVERS, processRuntime, sourceLabel, streamsOf, type MonitorSource, type SourceRuntime } from "./sources";
import { evalExpr, parseExpr, type Ast } from "./expr";

// Re-export the source surface so existing importers (tools/monitor.ts) keep their
// `from "../monitor"` paths; the types/helpers now live in sources.ts.
export { fetchDelta, parseSource, sourceLabel, type MonitorSource } from "./sources";

const EVERY_MATCH: NotifyPolicy = { mode: "every-match" };

const POLL_INTERVAL_MS = 3000;

/** Persisted shape of a standalone monitor (<remoteCwd>/.pi-ssh-monitors/<id>.json). */
export interface MonitorFile {
	id: string;
	source: MonitorSource;
	/** Optional for probe sources (predicate-only); required for process/file. */
	pattern?: string;
	repeat: boolean;
	name?: string;
	paused: boolean;
	/** Notification policy. Absent (legacy file) ⇒ every-match. */
	notify?: NotifyPolicy;
	/** Optional capture-aware notification body template. */
	template?: string;
	/** Optional safe predicate gating a fire (probe: required; process/file: extra filter). */
	notifyWhen?: string;
}

interface MonitorState {
	id: string;
	/** standalone = a first-class monitor persisted in .pi-ssh-monitors (created via
	 * ssh_monitor OR desugared from ssh_process logWatches); legacy = a back-compat
	 * shim rebuilt in-memory from an OLD job's notify.json watches (not persisted). */
	kind: "standalone" | "legacy";
	source: MonitorSource;
	/** Empty for probe sources (predicate-only); the matched regex source otherwise. */
	pattern: string;
	re: RegExp;
	repeat: boolean;
	name?: string;
	/** Safe predicate over captures/value/exitCode/matchCount/elapsedMs; gates a fire. */
	notifyWhen?: string;
	notifyWhenAst?: Ast;
	/** Monitor creation epoch ms (elapsedMs anchor for notifyWhen). */
	startedAt: number;
	target: SshTarget;
	/** Per-source mutable runtime (offsets/clocks). Replaces the bare process dir/off. */
	srt: SourceRuntime;
	paused: boolean;
	/** One-shot (repeat=false) latch: once matched it stops matching. Only applies
	 * under the every-match policy; aggregating policies always keep matching. */
	fired: boolean;
	matchCount: number;
	lastMatchAt: number | null;
	/** Notification policy + the live gate that applies it (rebuilt on update). */
	notify: NotifyPolicy;
	template?: string;
	gate: NotifyGate;
	/** Named groups from the most recent match (for list display). */
	captures: Record<string, string>;
	busy: boolean;
	finished: boolean;
}

export interface MonitorRow {
	id: string;
	kind: "standalone" | "legacy";
	source: string;
	pattern: string;
	repeat: boolean;
	notify: string;
	paused: boolean;
	fired: boolean;
	matchCount: number;
	name?: string;
	notifyWhen?: string;
}

export interface CreateMonitorOpts {
	source: MonitorSource;
	/** Required for process/file; omitted for probe (predicate-only). */
	pattern?: string;
	repeat?: boolean;
	name?: string;
	notify?: NotifyPolicy;
	template?: string;
	notifyWhen?: string;
}

/** One logWatch resolved to a monitor spec (notify already parsed), bound to the
 * job at ssh_process start via createForProcess. */
export interface ProcessWatch {
	pattern: string;
	stream?: "stdout" | "stderr" | "both";
	repeat?: boolean;
	notify?: NotifyPolicy;
	template?: string;
}

export interface UpdateMonitorPatch {
	pattern?: string;
	repeat?: boolean;
	name?: string;
	notify?: NotifyPolicy;
	template?: string;
	notifyWhen?: string;
}

export interface MonitorManager {
	/** Create a standalone monitor; seeks the source to EOF so history does not re-fire. */
	create(t: SshTarget, opts: CreateMonitorOpts): Promise<MonitorState>;
	/** Create first-class, persisted monitors for an ssh_process job's logWatches
	 * (the standard path: each logWatch == an ssh_monitor create). offsets default to
	 * 0 (fresh job at start); pass current EOF when attaching a running job. */
	createForProcess(t: SshTarget, procId: string, watches: ProcessWatch[], name?: string, offsets?: { stdout: number; stderr: number }): Promise<void>;
	/** Back-compat shim: rebuild in-memory every-match monitors from an OLD job's
	 * notify.json watches (deterministic ids, not persisted). Only legacy jobs hit this. */
	armLegacyWatches(t: SshTarget, procId: string, watches: WatchSpec[], opts?: { offsets?: { stdout: number; stderr: number }; name?: string }): void;
	list(): MonitorRow[];
	update(id: string, patch: UpdateMonitorPatch): Promise<MonitorState>;
	pause(id: string): Promise<void>;
	resume(id: string): Promise<void>;
	remove(id: string): Promise<void>;
	/** Stop (and drop) every monitor bound to a process — used on agent-initiated kill. */
	stopForProcess(procId: string): void;
	has(id: string): boolean;
	/** Repoint every live monitor at a new connection (same host+cwd reconnect). */
	repointAll(t: SshTarget): void;
	stopAll(): void;
	rehydrate(t: SshTarget): Promise<void>;
}

export function monitorRoot(t: SshTarget): string {
	return `${t.remoteCwd}/.pi-ssh-monitors`;
}

/** Compile a monitor/watch regex, throwing a clear error on a bad pattern (fail fast). */
export function compilePattern(pattern: string): RegExp {
	try {
		return new RegExp(pattern);
	} catch (e) {
		throw new Error(`Invalid monitor pattern /${pattern}/: ${e instanceof Error ? e.message : String(e)}`);
	}
}

/** Validate logWatch patterns before launching a job (so a bad regex fails fast). */
export function validateWatchPatterns(watches: WatchSpec[] | undefined): void {
	for (const w of watches ?? []) compilePattern(w.pattern);
}

// A milestone policy computes progress from a `total` capture; without it there is
// nothing to measure against. Validate at create/update/start time (fail fast).
export function assertPolicyMatchesPattern(notify: NotifyPolicy, pattern: string): void {
	if (notify.mode === "milestone" && !/\(\?<total>/.test(pattern)) {
		throw new Error(`milestone policy needs a (?<total>…) named capture group in the pattern to measure progress; got /${pattern}/`);
	}
}

export function createMonitorManager(pi: ExtensionAPI): MonitorManager {
	const monitors = new Map<string, MonitorState>();
	let timer: NodeJS.Timeout | null = null;

	const emit = (content: string, details: Record<string, unknown>): void => sendProcessMessage(pi, content, details);

	function ensureTimer(): void {
		if (timer) return;
		timer = setInterval(() => {
			// One shared timer; each monitor sweeps concurrently under its own busy
			// guard so a slow/stuck source never blocks the others (§9.2).
			for (const m of monitors.values()) void sweepMonitor(m);
		}, POLL_INTERVAL_MS);
		timer.unref?.();
	}

	function maybeStopTimer(): void {
		if (timer && monitors.size === 0) {
			clearInterval(timer);
			timer = null;
		}
	}

	function dropFromMap(id: string): void {
		const m = monitors.get(id);
		if (!m) return;
		monitors.delete(id);
		maybeStopTimer();
	}

	// Only the every-match policy honors the one-shot latch; aggregating policies
	// (every-n/throttle/digest/milestone) must keep matching to do their job.
	function isOneShot(m: MonitorState): boolean {
		return !m.repeat && m.notify.mode === "every-match";
	}

	// Deliver a gate decision through the sink. `stream` is known for per-line
	// matches but not for tick/close flushes (a digest can span both streams).
	function emitMonitor(m: MonitorState, d: GateDecision, stream?: "stdout" | "stderr", line?: string): void {
		const label = m.name ? `"${m.name}" (${m.id})` : m.id;
		const where = stream ? ` on ${stream}` : "";
		const src = m.source;
		const srcDesc = src.kind === "process" ? `process ${src.procId}` : src.kind === "file" ? `file ${src.path}` : `probe ${src.command}`;
		// Probe sources have no regex — surface the predicate that fired instead.
		const what = src.kind === "probe" ? `notifyWhen ${m.notifyWhen ?? ""}` : `/${m.pattern}/`;
		emit(`🔔 ssh_monitor ${label} ${what}${where} (${srcDesc}):\n${d.text ?? ""}`, {
			kind: "monitor",
			monitorId: m.id,
			source: sourceLabel(src),
			...(src.kind === "process" ? { procId: src.procId } : {}),
			...(stream ? { stream } : {}),
			...(line !== undefined ? { line } : {}),
			pattern: m.pattern,
			matchCount: m.matchCount,
			...d.details,
		});
	}

	// Evaluate an optional notifyWhen predicate over a match's captures (+ value/
	// matchCount/elapsedMs). Absent predicate ⇒ pass. A predicate that throws at
	// runtime is treated as false (never crashes the tick).
	function passesNotifyWhen(m: MonitorState, captures: Record<string, string>, extra: Record<string, string | number>): boolean {
		if (!m.notifyWhenAst) return true;
		try {
			return evalExpr(m.notifyWhenAst, { ...captures, ...extra });
		} catch {
			return false;
		}
	}

	function runMatch(m: MonitorState, stream: "stdout" | "stderr" | undefined, line: string): void {
		if (m.fired && isOneShot(m)) return;
		// exec (not test) so named groups are captured for the notify policy/template.
		// The regex is non-global (compilePattern adds no flags), so lastIndex never
		// advances and each line is matched independently.
		const match = m.re.exec(line);
		if (!match) return;
		const captures = { ...(match.groups ?? {}) };
		// An optional notifyWhen filters the regex match (e.g. only fire when loss>10).
		if (!passesNotifyWhen(m, captures, { matchCount: m.matchCount + 1, elapsedMs: Date.now() - m.startedAt })) return;
		m.matchCount++;
		m.lastMatchAt = Date.now();
		m.captures = captures;
		const decision = m.gate.onMatch({ line, captures: m.captures, matchCount: m.matchCount, now: m.lastMatchAt });
		if (decision.fire) emitMonitor(m, decision, stream, line);
		if (isOneShot(m)) m.fired = true;
	}

	// Probe predicate path: a probe poll returns a {stdout, exitCode} sample, not log
	// lines. notifyWhen (required for probe) decides whether this sample "fires"; the
	// `consecutive` debounce requires N true samples in a row before it counts as a
	// match (e.g. GPU util==0 for 3 ticks), then feeds the notify gate like a match.
	function evalProbe(m: MonitorState, probe: { stdout: string; exitCode: number }): void {
		if (m.srt.kind !== "probe" || m.source.kind !== "probe") return;
		if (m.fired && isOneShot(m)) return;
		const valueNum = Number(probe.stdout);
		const value: string | number = probe.stdout !== "" && Number.isFinite(valueNum) ? valueNum : probe.stdout;
		const now = Date.now();
		const pass = passesNotifyWhen(m, {}, { value, valueStr: probe.stdout, exitCode: probe.exitCode, matchCount: m.matchCount + 1, elapsedMs: now - m.startedAt });
		if (pass) m.srt.hits++;
		else m.srt.hits = 0;
		const consecutive = m.source.consecutive ?? 1;
		if (m.srt.hits < consecutive) return;
		m.srt.hits = 0; // re-arm the debounce so it needs another N trues to fire again
		m.matchCount++;
		m.lastMatchAt = now;
		m.captures = { value: String(value), exitCode: String(probe.exitCode) };
		const line = `${probe.stdout} (exit ${probe.exitCode})`;
		const decision = m.gate.onMatch({ line, captures: m.captures, matchCount: m.matchCount, now });
		if (decision.fire) emitMonitor(m, decision, undefined, line);
		if (isOneShot(m)) m.fired = true;
	}

	// Source-agnostic orchestration: the per-kind SourceDriver does the polling
	// (liveness, offsets, sampling); this loop applies the regex + notify gate and
	// handles teardown. Adding a source kind means adding a driver, not editing this.
	async function sweepMonitor(m: MonitorState): Promise<void> {
		if (m.paused || m.busy || m.finished) return;
		m.busy = true;
		try {
			// A latched one-shot reads nothing (offsets frozen) but still probes liveness
			// so it tears down when its source ends — same as the pre-refactor sweep.
			const readDeltas = !(m.fired && isOneShot(m));
			const result = await DRIVERS[m.source.kind].poll(m.target, m.source, m.srt, { readDeltas });
			if (result.vanished) {
				// Source gone for good (e.g. process dir cleared): orphaned monitor, tear it
				// down (and delete its persisted file for standalone monitors), no final sweep.
				await removeInternal(m);
				return;
			}
			if (readDeltas) {
				for (const { stream, line } of result.matches ?? []) runMatch(m, stream, line);
				if (result.probe) evalProbe(m, result.probe);
			}
			const now = Date.now();
			if (result.ended) {
				// Source finished: the poll above read every remaining byte, so no new lines
				// can arrive. Flush any pending digest, then stop (the monitor is spent).
				const close = m.gate.onClose(now);
				if (close.fire) emitMonitor(m, close);
				m.finished = true;
				await removeInternal(m);
			} else {
				// Still live: give time-based policies (digest) a chance to flush.
				const tick = m.gate.onTick(now);
				if (tick.fire) emitMonitor(m, tick);
			}
		} catch {
			// Swallow: a single failed tick must never escape and kill the scheduler.
		} finally {
			m.busy = false;
		}
	}

	// --- persistence (standalone monitors only) ---
	function toFile(m: MonitorState): MonitorFile {
		return { id: m.id, source: m.source, pattern: m.pattern, repeat: m.repeat, name: m.name, paused: m.paused, notify: m.notify, template: m.template, notifyWhen: m.notifyWhen };
	}

	async function persist(m: MonitorState): Promise<void> {
		if (m.kind !== "standalone") return;
		const root = monitorRoot(m.target);
		const path = `${root}/${m.id}.json`;
		const json = JSON.stringify(toFile(m));
		await runRemoteCommand(m.target, `mkdir -p ${shQuote(root)} && printf %s ${shQuote(json)} > ${shQuote(path)}`, { timeout: 20, login: false });
	}

	async function deleteFile(m: MonitorState): Promise<void> {
		if (m.kind !== "standalone") return;
		const path = `${monitorRoot(m.target)}/${m.id}.json`;
		await runRemoteCommand(m.target, `rm -f -- ${shQuote(path)}`, { timeout: 20, login: false }).catch(() => {});
	}

	// Tear a monitor down: drop it from the live map and delete its persisted file
	// (standalone only). Every removal path — explicit remove, gone/done teardown,
	// process kill — wants the file cleaned up. stopAll() bypasses this (files persist
	// across a disconnect so they rehydrate on the next connect).
	async function removeInternal(m: MonitorState): Promise<void> {
		dropFromMap(m.id);
		await deleteFile(m);
	}

	function addMonitor(state: MonitorState): void {
		monitors.set(state.id, state);
		ensureTimer();
	}

	function buildState(args: {
		id: string;
		kind?: "standalone" | "legacy";
		source: MonitorSource;
		pattern?: string;
		repeat: boolean;
		name?: string;
		paused: boolean;
		target: SshTarget;
		srt: SourceRuntime;
		notify?: NotifyPolicy;
		template?: string;
		notifyWhen?: string;
	}): MonitorState {
		const notify = args.notify ?? EVERY_MATCH;
		const pattern = args.pattern ?? "";
		return {
			id: args.id,
			kind: args.kind ?? "standalone",
			source: args.source,
			pattern,
			re: compilePattern(pattern),
			repeat: args.repeat,
			name: args.name,
			notifyWhen: args.notifyWhen,
			// Compile the predicate once (throws on a bad expression — fail fast at create,
			// drop the file at rehydrate via the surrounding try/catch).
			notifyWhenAst: args.notifyWhen ? parseExpr(args.notifyWhen) : undefined,
			startedAt: Date.now(),
			target: args.target,
			srt: args.srt,
			paused: args.paused,
			fired: false,
			matchCount: 0,
			lastMatchAt: null,
			notify,
			template: args.template,
			gate: makeNotifyGate(notify, { template: args.template }),
			captures: {},
			busy: false,
			finished: false,
		};
	}

	async function create(t: SshTarget, opts: CreateMonitorOpts): Promise<MonitorState> {
		// Fail fast before any remote round-trip. Probe sources are predicate-only (no
		// regex) but REQUIRE a notifyWhen; process/file require a regex pattern.
		if (opts.source.kind === "probe") {
			if (!opts.notifyWhen?.trim()) throw new Error("probe source requires notifyWhen (the predicate that decides when to fire), e.g. notifyWhen='value==0'.");
		} else {
			if (!opts.pattern?.trim()) throw new Error(`${opts.source.kind} source requires a pattern (the regex matched per log line).`);
			compilePattern(opts.pattern);
			assertPolicyMatchesPattern(opts.notify ?? EVERY_MATCH, opts.pattern);
		}
		if (opts.notifyWhen?.trim()) parseExpr(opts.notifyWhen); // validate the predicate up front
		const driver = DRIVERS[opts.source.kind];
		await driver.validate(t, opts.source);
		const id = `mon_${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
		// Seek to current EOF so a monitor attached to an already-running source does
		// not re-fire on historical log lines (same rule as poller rehydrate).
		const srt = await driver.initRuntime(t, opts.source);
		const state = buildState({ id, kind: "standalone", source: opts.source, pattern: opts.pattern, repeat: opts.repeat ?? false, name: opts.name, paused: false, target: t, srt, notify: opts.notify, template: opts.template, notifyWhen: opts.notifyWhen?.trim() || undefined });
		addMonitor(state);
		await persist(state);
		return state;
	}

	// Standard path: ssh_process start desugars each logWatch into a first-class,
	// persisted standalone monitor bound to the just-started job. The logs are fresh
	// (empty) so offsets start at 0 (== EOF), and the process is necessarily running,
	// so we skip the status/EOF probes create() does.
	async function createForProcess(t: SshTarget, procId: string, watches: ProcessWatch[], name?: string, offsets?: { stdout: number; stderr: number }): Promise<void> {
		for (const w of watches) {
			const stream = w.stream ?? "both";
			let id = `mon_${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
			while (monitors.has(id)) id = `mon_${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`; // guard the loop's only un-deduped id source
			const off: Record<string, number> = {};
			for (const s of streamsOf(stream)) off[s] = offsets ? offsets[s] : 0;
			const srt = processRuntime(`${processRoot(t)}/${procId}`, off);
			const state = buildState({ id, kind: "standalone", source: { kind: "process", procId, stream }, pattern: w.pattern, repeat: w.repeat ?? false, name, paused: false, target: t, srt, notify: w.notify, template: w.template });
			addMonitor(state);
			// Best-effort: the in-memory monitor is armed regardless; a failed file write
			// only costs cross-restart persistence, it must not fail the already-started job.
			await persist(state).catch(() => {});
		}
	}

	// Back-compat shim ONLY: rebuild in-memory every-match monitors from an OLD job's
	// notify.json watches (jobs started before logWatches became first-class). New
	// jobs write no watches to notify.json, so this is a no-op for them. Deterministic
	// ids keep it idempotent across reconnects; legacy monitors are not persisted.
	function armLegacyWatches(t: SshTarget, procId: string, watches: WatchSpec[], opts?: { offsets?: { stdout: number; stderr: number }; name?: string }): void {
		watches.forEach((w, i) => {
			const id = `${procId}#w${i}`;
			if (monitors.has(id)) return;
			const stream = w.stream ?? "both";
			const off: Record<string, number> = {};
			for (const s of streamsOf(stream)) off[s] = opts?.offsets ? opts.offsets[s] : 0;
			const srt = processRuntime(`${processRoot(t)}/${procId}`, off);
			const state = buildState({ id, kind: "legacy", source: { kind: "process", procId, stream }, pattern: w.pattern, repeat: w.repeat ?? false, name: opts?.name, paused: false, target: t, srt });
			addMonitor(state);
		});
	}

	function list(): MonitorRow[] {
		return [...monitors.values()].map((m) => ({
			id: m.id,
			kind: m.kind,
			source: sourceLabel(m.source),
			pattern: m.pattern,
			repeat: m.repeat,
			notify: policyLabel(m.notify),
			paused: m.paused,
			fired: m.fired,
			matchCount: m.matchCount,
			name: m.name,
			notifyWhen: m.notifyWhen,
		}));
	}

	function getMonitor(id: string): MonitorState {
		const m = monitors.get(id);
		if (!m) throw new Error(`Monitor not found: ${id}. Use ssh_monitor list.`);
		return m;
	}

	async function update(id: string, patch: UpdateMonitorPatch): Promise<MonitorState> {
		const m = getMonitor(id);
		if (m.kind === "legacy") throw new Error(`${id} is a legacy monitor rebuilt from an old ssh_process job's notify.json; it cannot be edited. Re-attach the job (ssh_process attach) or create a fresh monitor with ssh_monitor create.`);
		// Validate everything before mutating any field (so a bad patch leaves the
		// monitor untouched).
		const nextPattern = patch.pattern ?? m.pattern;
		const nextNotify = patch.notify ?? m.notify;
		const nextTemplate = patch.template !== undefined ? patch.template : m.template;
		if (patch.pattern !== undefined) compilePattern(patch.pattern);
		assertPolicyMatchesPattern(nextNotify, nextPattern);
		// Validate a new predicate before mutating anything (empty string clears it).
		const nextNotifyWhen = patch.notifyWhen === undefined ? m.notifyWhen : patch.notifyWhen.trim() || undefined;
		const nextNotifyWhenAst = patch.notifyWhen === undefined ? m.notifyWhenAst : nextNotifyWhen ? parseExpr(nextNotifyWhen) : undefined;
		// A probe is predicate-only: clearing its notifyWhen would make every sample fire
		// (passesNotifyWhen returns true with no AST), violating the create-time invariant.
		if (m.source.kind === "probe" && !nextNotifyWhen) throw new Error("probe source requires notifyWhen; it cannot be cleared. Pass a new predicate or remove the monitor.");
		if (patch.pattern !== undefined) {
			m.re = compilePattern(patch.pattern);
			m.pattern = patch.pattern;
			m.fired = false; // a changed pattern re-arms the one-shot latch
		}
		if (patch.notifyWhen !== undefined) {
			m.notifyWhen = nextNotifyWhen;
			m.notifyWhenAst = nextNotifyWhenAst;
			m.fired = false; // a changed predicate re-arms the one-shot latch
		}
		if (patch.repeat !== undefined) m.repeat = patch.repeat;
		if (patch.name !== undefined) m.name = patch.name;
		// Rebuild the gate when policy or template changes (resets policy counters).
		// Flush any pending buffer (digest window / throttle backlog) through the OLD
		// gate first so a mid-window edit does not silently drop buffered matches.
		if (patch.notify !== undefined || patch.template !== undefined) {
			const flushed = m.gate.onClose(Date.now());
			if (flushed.fire) emitMonitor(m, flushed);
			m.notify = nextNotify;
			m.template = nextTemplate;
			m.gate = makeNotifyGate(nextNotify, { template: nextTemplate });
		}
		await persist(m);
		return m;
	}

	async function pause(id: string): Promise<void> {
		const m = getMonitor(id);
		m.paused = true;
		await persist(m);
	}

	async function resume(id: string): Promise<void> {
		const m = getMonitor(id);
		m.paused = false;
		await persist(m);
	}

	async function remove(id: string): Promise<void> {
		const m = getMonitor(id);
		await removeInternal(m);
	}

	function stopForProcess(procId: string): void {
		for (const m of [...monitors.values()]) {
			if (m.source.kind === "process" && m.source.procId === procId) void removeInternal(m);
		}
	}

	// Re-arm monitors after a (re)connect or pi restart: standalone monitors from
	// their persisted files, plus a back-compat shim that rebuilds every-match
	// monitors from each RUNNING old job's notify.json watches (new jobs write none).
	// Each source offset seeks to EOF so historical lines do not re-fire. Already-live
	// monitors are skipped (idempotent ids).
	async function rehydrate(t: SshTarget): Promise<void> {
		if (!t.hasPython) return;
		const mroot = monitorRoot(t);
		const proot = processRoot(t);
		const script = `
import base64, json, os, sys
mroot, proot = sys.argv[1], sys.argv[2]
mons = []
if os.path.isdir(mroot):
    for f in sorted(os.listdir(mroot)):
        if not f.endswith('.json'):
            continue
        try:
            data = open(os.path.join(mroot, f)).read()
        except OSError:
            continue
        mons.append(base64.b64encode(data.encode()).decode())
procs = {}
if os.path.isdir(proot):
    for d in sorted(os.listdir(proot)):
        pp = os.path.join(proot, d)
        if not os.path.isdir(pp):
            continue
        pid = ''
        try:
            pid = open(os.path.join(pp, 'pid')).read().strip()
        except OSError:
            pass
        running = False
        if pid:
            try:
                os.kill(int(pid), 0)
                running = True
            except (OSError, ValueError):
                running = False
        def sz(fn):
            try:
                return os.path.getsize(os.path.join(pp, fn))
            except OSError:
                return 0
        notify = ''
        try:
            notify = base64.b64encode(open(os.path.join(pp, 'notify.json')).read().encode()).decode()
        except OSError:
            pass
        procs[d] = {'running': running, 'outSize': sz('stdout.log'), 'errSize': sz('stderr.log'), 'notify': notify}
print(json.dumps({'monitors': mons, 'procs': procs}))
`;
		let parsed: { monitors: string[]; procs: Record<string, { running: boolean; outSize: number; errSize: number; notify: string }> };
		try {
			const r = await runRemoteCommand(t, `python3 -c ${shQuote(script)} ${shQuote(mroot)} ${shQuote(proot)}`, { timeout: 20, login: false });
			if (r.code !== 0) return;
			parsed = JSON.parse(r.stdout.toString() || '{"monitors":[],"procs":{}}');
		} catch {
			return;
		}
		// Standalone monitors from their persisted files.
		for (const b64 of parsed.monitors) {
			let spec: MonitorFile;
			try {
				spec = JSON.parse(Buffer.from(b64, "base64").toString());
			} catch {
				continue;
			}
			const source = spec.source;
			if (!spec.id || monitors.has(spec.id) || (source?.kind !== "process" && source?.kind !== "file" && source?.kind !== "probe")) continue;
			if (source.kind === "process" && !parsed.procs[source.procId]) {
				// Bound process is gone: drop the stale monitor file. (file: sources have no
				// bound process and are never reaped here — only an explicit remove drops them.)
				await runRemoteCommand(t, `rm -f -- ${shQuote(`${mroot}/${spec.id}.json`)}`, { timeout: 20, login: false }).catch(() => {});
				continue;
			}
			try {
				// Seek to EOF so history does not re-fire: process seeds from the scan's known
				// sizes (no extra round-trip); file does a cheap per-monitor wc -c.
				let srt: SourceRuntime;
				if (source.kind === "process") {
					const proc = parsed.procs[source.procId];
					srt = await DRIVERS.process.rehydrateRuntime(t, source, { outSize: proc.outSize, errSize: proc.errSize });
				} else {
					srt = await DRIVERS[source.kind].rehydrateRuntime(t, source);
				}
				const state = buildState({ id: spec.id, kind: "standalone", source, pattern: spec.pattern, repeat: !!spec.repeat, name: spec.name, paused: !!spec.paused, target: t, srt, notify: spec.notify, template: spec.template, notifyWhen: spec.notifyWhen });
				addMonitor(state);
			} catch {
				// A corrupt/invalid persisted monitor (e.g. bad regex) must not abort
				// re-arming the rest. Drop the unusable file and move on.
				await runRemoteCommand(t, `rm -f -- ${shQuote(`${mroot}/${spec.id}.json`)}`, { timeout: 20, login: false }).catch(() => {});
			}
		}
		// Legacy shim: rebuild every-match monitors from each RUNNING OLD job's
		// notify.json watches. New jobs write no watches here (logWatches are persisted
		// as standalone monitor files above), so this only fires for pre-upgrade jobs.
		for (const [procId, proc] of Object.entries(parsed.procs)) {
			if (!proc.running || !proc.notify) continue;
			let cfg: { watches?: WatchSpec[]; name?: string };
			try {
				cfg = JSON.parse(Buffer.from(proc.notify, "base64").toString());
			} catch {
				continue;
			}
			const watches = cfg.watches ?? [];
			const procName = cfg.name?.trim() || undefined;
			watches.forEach((w, i) => {
				const id = `${procId}#w${i}`;
				if (monitors.has(id)) return;
				const stream = w.stream ?? "both";
				try {
					const off: Record<string, number> = {};
					for (const s of streamsOf(stream)) off[s] = s === "stdout" ? proc.outSize : proc.errSize;
					const srt = processRuntime(`${proot}/${procId}`, off);
					const state = buildState({ id, kind: "legacy", source: { kind: "process", procId, stream }, pattern: w.pattern, repeat: w.repeat ?? false, name: procName, paused: false, target: t, srt });
					addMonitor(state);
				} catch {
					// Skip an unusable watch (e.g. bad regex from a hand-edited notify.json)
					// rather than aborting the whole rehydrate.
				}
			});
		}
	}

	return {
		create,
		createForProcess,
		armLegacyWatches,
		list,
		update,
		pause,
		resume,
		remove,
		stopForProcess,
		has: (id) => monitors.has(id),
		repointAll: (t) => {
			for (const m of monitors.values()) {
				m.target = t;
				// Re-resolve any process-relative paths against the new connection's cwd.
				if (m.source.kind === "process" && m.srt.kind === "process") m.srt.dir = `${processRoot(t)}/${m.source.procId}`;
			}
		},
		stopAll: () => {
			for (const id of [...monitors.keys()]) dropFromMap(id);
		},
		rehydrate,
	};
}
