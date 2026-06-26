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

const EVERY_MATCH: NotifyPolicy = { mode: "every-match" };

const POLL_INTERVAL_MS = 3000;

// Phase 1: the only source kind is a managed process's stdout/stderr stream(s).
// file:/probe: sources land in Phase 3 (MONITOR_PLAN §7.4), extending this union.
export type MonitorSource = { kind: "process"; procId: string; stream: "stdout" | "stderr" | "both" };

/** Persisted shape of a standalone monitor (<remoteCwd>/.pi-ssh-monitors/<id>.json). */
export interface MonitorFile {
	id: string;
	source: MonitorSource;
	pattern: string;
	repeat: boolean;
	name?: string;
	paused: boolean;
	/** Notification policy. Absent (legacy file) ⇒ every-match. */
	notify?: NotifyPolicy;
	/** Optional capture-aware notification body template. */
	template?: string;
}

interface MonitorState {
	id: string;
	/** standalone = a first-class monitor persisted in .pi-ssh-monitors (created via
	 * ssh_monitor OR desugared from ssh_process logWatches); legacy = a back-compat
	 * shim rebuilt in-memory from an OLD job's notify.json watches (not persisted). */
	kind: "standalone" | "legacy";
	source: MonitorSource;
	pattern: string;
	re: RegExp;
	repeat: boolean;
	name?: string;
	target: SshTarget;
	/** Resolved process dir (<root>/<procId>) the source streams live under. */
	dir: string;
	/** Per-stream byte offset into the source log(s). */
	off: Record<string, number>;
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
}

export interface CreateMonitorOpts {
	source: MonitorSource;
	pattern: string;
	repeat?: boolean;
	name?: string;
	notify?: NotifyPolicy;
	template?: string;
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
}

export interface MonitorManager {
	/** Create a standalone monitor; seeks the source to EOF so history does not re-fire. */
	create(t: SshTarget, opts: CreateMonitorOpts): Promise<MonitorState>;
	/** Create first-class, persisted monitors for an ssh_process job's logWatches
	 * (the standard path: each logWatch == an ssh_monitor create). */
	createForProcess(t: SshTarget, procId: string, watches: ProcessWatch[], name?: string): Promise<void>;
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

function streamsOf(stream: "stdout" | "stderr" | "both"): Array<"stdout" | "stderr"> {
	return stream === "both" ? ["stdout", "stderr"] : [stream];
}

export function sourceLabel(s: MonitorSource): string {
	return `process:${s.procId}:${s.stream}`;
}

/** Parse a `process:<procId>[:<stream>]` source string. Stream defaults to "both". */
export function parseSource(raw: string): MonitorSource {
	const text = raw.trim();
	if (!text.startsWith("process:")) {
		throw new Error(`Unsupported --source "${raw}". Phase 1 supports process:<procId>[:stdout|stderr|both].`);
	}
	const rest = text.slice("process:".length);
	const lastColon = rest.lastIndexOf(":");
	let procId = rest;
	let stream: "stdout" | "stderr" | "both" = "both";
	if (lastColon !== -1) {
		const tail = rest.slice(lastColon + 1);
		if (tail === "stdout" || tail === "stderr" || tail === "both") {
			stream = tail;
			procId = rest.slice(0, lastColon);
		}
	}
	if (!procId.trim()) throw new Error(`Invalid --source "${raw}": missing process id.`);
	return { kind: "process", procId: procId.trim(), stream };
}

// Read new complete lines appended to a remote file since `off`. Advances only to
// the last newline so partial lines (and multibyte chars straddling the boundary)
// are re-read intact next tick. Byte-offset based, so nothing logged during Mac
// sleep is lost — it is swept on the first tick after wake. (Generalized from the
// poller's fetchDeltaLines: takes an explicit file + offset, returns nextOff.)
export async function fetchDelta(t: SshTarget, file: string, off: number): Promise<{ lines: string[]; nextOff: number }> {
	const r = await runRemoteCommand(t, `tail -c +${off + 1} -- ${shQuote(file)} 2>/dev/null || true`, { timeout: 20, login: false });
	if (r.code !== 0) return { lines: [], nextOff: off };
	const buf = r.stdout;
	if (buf.length === 0) return { lines: [], nextOff: off };
	const lastNl = buf.lastIndexOf(0x0a);
	if (lastNl === -1) return { lines: [], nextOff: off }; // no complete line yet
	return { lines: buf.subarray(0, lastNl).toString("utf8").split("\n"), nextOff: off + lastNl + 1 };
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

	// Process status, used only to decide when to auto-tear-down a process-bound
	// monitor. A transient SSH failure returns "running" so a flaky tick during a
	// reconnect blip never spuriously removes a live monitor.
	async function procStatus(t: SshTarget, dir: string): Promise<"running" | "done" | "gone"> {
		const cmd = `d=${shQuote(dir)}; if [ ! -d "$d" ]; then echo gone; else pid=$(cat "$d/pid" 2>/dev/null || true); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then echo running; else echo done; fi; fi`;
		const r = await runRemoteCommand(t, cmd, { timeout: 20, login: false });
		if (r.code !== 0) return "running";
		const out = r.stdout.toString().trim().split("\n").pop() || "";
		return out === "gone" ? "gone" : out === "done" ? "done" : "running";
	}

	async function eofOffsets(t: SshTarget, dir: string, stream: "stdout" | "stderr" | "both"): Promise<Record<string, number>> {
		const off: Record<string, number> = {};
		for (const s of streamsOf(stream)) {
			const r = await runRemoteCommand(t, `wc -c < ${shQuote(`${dir}/${s}.log`)} 2>/dev/null || echo 0`, { timeout: 20, login: false });
			off[s] = r.code === 0 ? Number.parseInt(r.stdout.toString().trim(), 10) || 0 : 0;
		}
		return off;
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
		emit(`🔔 ssh_monitor ${label} /${m.pattern}/${where} (process ${m.source.procId}):\n${d.text ?? ""}`, {
			kind: "monitor",
			monitorId: m.id,
			procId: m.source.procId,
			...(stream ? { stream } : {}),
			...(line !== undefined ? { line } : {}),
			pattern: m.pattern,
			matchCount: m.matchCount,
			...d.details,
		});
	}

	function runMatch(m: MonitorState, stream: "stdout" | "stderr", lines: string[]): void {
		for (const line of lines) {
			if (m.fired && isOneShot(m)) break;
			// exec (not test) so named groups are captured for the notify policy/template.
			// The regex is non-global (compilePattern adds no flags), so lastIndex never
			// advances and each line is matched independently.
			const match = m.re.exec(line);
			if (!match) continue;
			m.matchCount++;
			m.lastMatchAt = Date.now();
			m.captures = { ...(match.groups ?? {}) };
			const decision = m.gate.onMatch({ line, captures: m.captures, matchCount: m.matchCount, now: m.lastMatchAt });
			if (decision.fire) emitMonitor(m, decision, stream, line);
			if (isOneShot(m)) {
				m.fired = true;
				break;
			}
		}
	}

	async function sweepMonitor(m: MonitorState): Promise<void> {
		if (m.paused || m.busy || m.finished) return;
		m.busy = true;
		try {
			const status = await procStatus(m.target, m.dir);
			if (status === "gone") {
				// Source process cleared/removed: orphaned monitor, tear it down (and
				// delete its persisted file for standalone monitors). Mirrors the poller
				// gone→stop path (§9.3).
				await removeInternal(m);
				return;
			}
			if (!(m.fired && isOneShot(m))) {
				for (const s of streamsOf(m.source.stream)) {
					const file = `${m.dir}/${s}.log`;
					const { lines, nextOff } = await fetchDelta(m.target, file, m.off[s] ?? 0);
					if (nextOff !== (m.off[s] ?? 0)) m.off[s] = nextOff;
					if (lines.length) runMatch(m, s, lines);
				}
			}
			const now = Date.now();
			if (status === "done") {
				// Process exited: the sweep above read every remaining byte, so no new
				// lines can arrive. Flush any pending digest, then stop (the bound process
				// is finished, the monitor is spent).
				const close = m.gate.onClose(now);
				if (close.fire) emitMonitor(m, close);
				m.finished = true;
				await removeInternal(m);
			} else {
				// Still running: give time-based policies (digest) a chance to flush.
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
		return { id: m.id, source: m.source, pattern: m.pattern, repeat: m.repeat, name: m.name, paused: m.paused, notify: m.notify, template: m.template };
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
		pattern: string;
		repeat: boolean;
		name?: string;
		paused: boolean;
		target: SshTarget;
		off: Record<string, number>;
		notify?: NotifyPolicy;
		template?: string;
	}): MonitorState {
		const notify = args.notify ?? EVERY_MATCH;
		return {
			id: args.id,
			kind: args.kind ?? "standalone",
			source: args.source,
			pattern: args.pattern,
			re: compilePattern(args.pattern),
			repeat: args.repeat,
			name: args.name,
			target: args.target,
			dir: `${processRoot(args.target)}/${args.source.procId}`,
			off: args.off,
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
		compilePattern(opts.pattern); // fail fast before any remote round-trip
		assertPolicyMatchesPattern(opts.notify ?? EVERY_MATCH, opts.pattern);
		const dir = `${processRoot(t)}/${opts.source.procId}`;
		const status = await procStatus(t, dir);
		if (status === "gone") throw new Error(`Process ${opts.source.procId} not found (no ${dir}). Start it with ssh_process or check the id with ssh_process list.`);
		if (status === "done") throw new Error(`Process ${opts.source.procId} has already exited; a monitor only matches NEW log lines, so there is nothing to watch. Inspect past output with ssh_process output instead.`);
		const id = `mon_${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
		// Seek to current EOF so a monitor attached to an already-running process does
		// not re-fire on historical log lines (same rule as poller rehydrate).
		const off = await eofOffsets(t, dir, opts.source.stream);
		const state = buildState({ id, kind: "standalone", source: opts.source, pattern: opts.pattern, repeat: opts.repeat ?? false, name: opts.name, paused: false, target: t, off, notify: opts.notify, template: opts.template });
		addMonitor(state);
		await persist(state);
		return state;
	}

	// Standard path: ssh_process start desugars each logWatch into a first-class,
	// persisted standalone monitor bound to the just-started job. The logs are fresh
	// (empty) so offsets start at 0 (== EOF), and the process is necessarily running,
	// so we skip the status/EOF probes create() does.
	async function createForProcess(t: SshTarget, procId: string, watches: ProcessWatch[], name?: string): Promise<void> {
		for (const w of watches) {
			const stream = w.stream ?? "both";
			const id = `mon_${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
			const off: Record<string, number> = {};
			for (const s of streamsOf(stream)) off[s] = 0;
			const state = buildState({ id, kind: "standalone", source: { kind: "process", procId, stream }, pattern: w.pattern, repeat: w.repeat ?? false, name, paused: false, target: t, off, notify: w.notify, template: w.template });
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
			const state = buildState({ id, kind: "legacy", source: { kind: "process", procId, stream }, pattern: w.pattern, repeat: w.repeat ?? false, name: opts?.name, paused: false, target: t, off });
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
		if (patch.pattern !== undefined) {
			m.re = compilePattern(patch.pattern);
			m.pattern = patch.pattern;
			m.fired = false; // a changed pattern re-arms the one-shot latch
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
			if (m.source.procId === procId) void removeInternal(m);
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
		const eofFor = (proc: { outSize: number; errSize: number }, stream: "stdout" | "stderr" | "both"): Record<string, number> => {
			const off: Record<string, number> = {};
			for (const s of streamsOf(stream)) off[s] = s === "stdout" ? proc.outSize : proc.errSize;
			return off;
		};
		// Standalone monitors from their persisted files.
		for (const b64 of parsed.monitors) {
			let spec: MonitorFile;
			try {
				spec = JSON.parse(Buffer.from(b64, "base64").toString());
			} catch {
				continue;
			}
			if (!spec.id || monitors.has(spec.id) || spec.source?.kind !== "process") continue;
			const proc = parsed.procs[spec.source.procId];
			if (!proc) {
				// Bound process is gone: drop the stale monitor file.
				await runRemoteCommand(t, `rm -f -- ${shQuote(`${mroot}/${spec.id}.json`)}`, { timeout: 20, login: false }).catch(() => {});
				continue;
			}
			try {
				const state = buildState({ id: spec.id, kind: "standalone", source: spec.source, pattern: spec.pattern, repeat: !!spec.repeat, name: spec.name, paused: !!spec.paused, target: t, off: eofFor(proc, spec.source.stream), notify: spec.notify, template: spec.template });
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
					const state = buildState({ id, kind: "legacy", source: { kind: "process", procId, stream }, pattern: w.pattern, repeat: w.repeat ?? false, name: procName, paused: false, target: t, off: eofFor(proc, stream) });
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
				m.dir = `${processRoot(t)}/${m.source.procId}`;
			}
		},
		stopAll: () => {
			for (const id of [...monitors.keys()]) dropFromMap(id);
		},
		rehydrate,
	};
}
