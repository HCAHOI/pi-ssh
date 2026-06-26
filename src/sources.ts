// ---------------------------------------------------------------------------
// Source drivers: the per-kind abstraction the monitor engine polls. A monitor
// binds to a remote signal Source; each kind (process today; file/probe in later
// Phase-3 sub-phases) differs in liveness, offset shape, and poll cadence enough
// that a driver per kind keeps the engine (sweepMonitor) source-agnostic. See
// docs/MONITOR_PHASE3_PLAN.md §4.
//
// Drivers are stateless singletons. They operate on (target, source, runtime)
// and mutate the caller-owned SourceRuntime in place — they never touch the
// MonitorState, the NotifyGate, or the emit sink, so this module has no
// dependency on monitor.ts (no import cycle). All orchestration (regex match,
// notify gate, teardown) stays in monitor.ts.
// ---------------------------------------------------------------------------

import type { SshTarget } from "./types";
import { shQuote } from "./utils";
import { runRemoteCommand } from "./ssh/transport";
import { processRoot } from "./process-queries";

// The source union. Phase 3 widens this; `process` stays first/unchanged.
export type MonitorSource =
	| { kind: "process"; procId: string; stream: "stdout" | "stderr" | "both" }
	| { kind: "file"; path: string }; // absolute remote path

// Per-source mutable runtime: offsets/clocks the driver advances each poll.
// Replaces the bare process-only `dir`/`off` that lived on MonitorState.
export type SourceRuntime =
	| { kind: "process"; dir: string; off: Record<string, number> }
	| { kind: "file"; off: number };

/** What one poll observed this tick. The engine turns this into matches/teardown. */
export interface PollResult {
	/** New complete lines to run the regex on (stream known for process, absent otherwise). */
	matches?: { stream?: "stdout" | "stderr"; line: string }[];
	/** A probe sample (predicate path); Phase 3c. */
	probe?: { stdout: string; exitCode: number };
	/** Source finished for good (process exited) → final sweep already done, then remove. */
	ended?: boolean;
	/** Source gone (process dir removed) → remove now, no final sweep. */
	vanished?: boolean;
}

export interface SourceDriver {
	/** create-time existence/validity check; throw a clear error to reject. */
	validate(t: SshTarget, source: MonitorSource): Promise<void>;
	/** initial runtime (seek EOF for process/file; zero clocks for probe). */
	initRuntime(t: SshTarget, source: MonitorSource): Promise<SourceRuntime>;
	/** runtime from persisted/rehydrated state, seeking EOF so history does not re-fire.
	 * `hints` lets the caller pass already-known sizes (process: from the rehydrate scan). */
	rehydrateRuntime(t: SshTarget, source: MonitorSource, hints?: RehydrateHints): Promise<SourceRuntime>;
	/** one poll; mutates the runtime's offsets/clocks; returns what happened this tick.
	 * `readDeltas=false` (a latched one-shot) means probe liveness only — do NOT read or
	 * advance offsets, so a later pattern re-arm still re-scans the lines logged while latched. */
	poll(t: SshTarget, source: MonitorSource, srt: SourceRuntime, opts: PollOpts): Promise<PollResult>;
}

export interface PollOpts {
	/** read new bytes + advance offsets this tick. False when the monitor is a latched one-shot. */
	readDeltas: boolean;
}

export interface RehydrateHints {
	/** process: byte sizes of stdout.log / stderr.log from the rehydrate scan (EOF seed). */
	outSize?: number;
	errSize?: number;
}

export function streamsOf(stream: "stdout" | "stderr" | "both"): Array<"stdout" | "stderr"> {
	return stream === "both" ? ["stdout", "stderr"] : [stream];
}

// Byte size of a remote file, or 0 if it does not exist (a missing file is normal
// for process logs before the first write and for file sources pre-rotation).
export async function wcBytes(t: SshTarget, file: string): Promise<number> {
	const r = await runRemoteCommand(t, `wc -c < ${shQuote(file)} 2>/dev/null || echo 0`, { timeout: 20, login: false });
	return r.code === 0 ? Number.parseInt(r.stdout.toString().trim(), 10) || 0 : 0;
}

// Read new complete lines appended to a remote file since `off`. Advances only to
// the last newline so partial lines (and multibyte chars straddling the boundary)
// are re-read intact next tick. Byte-offset based, so nothing logged during Mac
// sleep is lost — it is swept on the first tick after wake. Source-agnostic: takes
// an explicit file + offset, returns nextOff (reused by every file-backed driver).
export async function fetchDelta(t: SshTarget, file: string, off: number): Promise<{ lines: string[]; nextOff: number }> {
	const r = await runRemoteCommand(t, `tail -c +${off + 1} -- ${shQuote(file)} 2>/dev/null || true`, { timeout: 20, login: false });
	if (r.code !== 0) return { lines: [], nextOff: off };
	const buf = r.stdout;
	if (buf.length === 0) return { lines: [], nextOff: off };
	const lastNl = buf.lastIndexOf(0x0a);
	if (lastNl === -1) return { lines: [], nextOff: off }; // no complete line yet
	return { lines: buf.subarray(0, lastNl).toString("utf8").split("\n"), nextOff: off + lastNl + 1 };
}

// --- process source --------------------------------------------------------

function procDir(t: SshTarget, procId: string): string {
	return `${processRoot(t)}/${procId}`;
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
	for (const s of streamsOf(stream)) off[s] = await wcBytes(t, `${dir}/${s}.log`);
	return off;
}

/** Build a process SourceRuntime from a dir + per-stream offsets (shared constructor). */
export function processRuntime(dir: string, off: Record<string, number>): SourceRuntime {
	return { kind: "process", dir, off };
}

const ProcessSourceDriver: SourceDriver = {
	async validate(t, source) {
		if (source.kind !== "process") return;
		const dir = procDir(t, source.procId);
		const status = await procStatus(t, dir);
		if (status === "gone") throw new Error(`Process ${source.procId} not found (no ${dir}). Start it with ssh_process or check the id with ssh_process list.`);
		if (status === "done") throw new Error(`Process ${source.procId} has already exited; a monitor only matches NEW log lines, so there is nothing to watch. Inspect past output with ssh_process output instead.`);
	},
	async initRuntime(t, source) {
		if (source.kind !== "process") return processRuntime("", {});
		// Seek to current EOF so a monitor attached to an already-running process does
		// not re-fire on historical log lines (same rule as poller rehydrate).
		const dir = procDir(t, source.procId);
		const off = await eofOffsets(t, dir, source.stream);
		return processRuntime(dir, off);
	},
	async rehydrateRuntime(t, source, hints) {
		if (source.kind !== "process") return processRuntime("", {});
		const dir = procDir(t, source.procId);
		const off: Record<string, number> = {};
		for (const s of streamsOf(source.stream)) off[s] = s === "stdout" ? (hints?.outSize ?? 0) : (hints?.errSize ?? 0);
		return processRuntime(dir, off);
	},
	async poll(t, source, srt, opts) {
		if (source.kind !== "process" || srt.kind !== "process") return {};
		const status = await procStatus(t, srt.dir);
		// Source process cleared/removed: orphaned monitor, tear it down without a
		// final sweep (mirrors the poller gone→stop path).
		if (status === "gone") return { vanished: true };
		// A latched one-shot probes liveness only: skip the read so offsets stay frozen
		// (a later pattern re-arm re-scans the lines logged while latched) and no extra
		// tail round-trips run per tick.
		if (!opts.readDeltas) return { ended: status === "done" };
		// Read every stream's delta first (even on "done", to capture the last bytes
		// the process wrote), then signal `ended` so the engine flushes + removes.
		const matches: { stream?: "stdout" | "stderr"; line: string }[] = [];
		for (const s of streamsOf(source.stream)) {
			const file = `${srt.dir}/${s}.log`;
			const { lines, nextOff } = await fetchDelta(t, file, srt.off[s] ?? 0);
			if (nextOff !== (srt.off[s] ?? 0)) srt.off[s] = nextOff;
			for (const line of lines) matches.push({ stream: s, line });
		}
		return { matches, ended: status === "done" };
	},
};

// --- file source -----------------------------------------------------------

const FileSourceDriver: SourceDriver = {
	async validate(_t, source) {
		if (source.kind !== "file") return;
		// A file source never rejects on a missing path (logrotate / not-yet-created are
		// normal); it just polls and picks the file up when it appears. Only reject a
		// path that isn't an absolute remote path.
		if (!source.path.startsWith("/")) throw new Error(`file source path must be an absolute remote path, got "${source.path}".`);
	},
	async initRuntime(t, source) {
		if (source.kind !== "file") return { kind: "file", off: 0 };
		// Seek to current EOF so historical lines do not re-fire.
		return { kind: "file", off: await wcBytes(t, source.path) };
	},
	async rehydrateRuntime(t, source) {
		if (source.kind !== "file") return { kind: "file", off: 0 };
		// Defensive: a hand-edited/corrupt persisted file path that isn't absolute is
		// dropped by the rehydrate loop's catch rather than wc'd against the login cwd.
		if (!source.path.startsWith("/")) throw new Error(`file source path must be absolute, got "${source.path}".`);
		return { kind: "file", off: await wcBytes(t, source.path) };
	},
	async poll(t, source, srt, opts) {
		if (source.kind !== "file" || srt.kind !== "file") return {};
		// A latched one-shot probes nothing here (no liveness to track) and keeps its
		// offset frozen so a pattern re-arm re-scans the lines logged while latched.
		if (!opts.readDeltas) return {};
		// Detect rotation/truncation: if the file is now smaller than our offset (default
		// logrotate mv+recreate, or copytruncate to 0), the absolute byte offset is stale
		// — reset to 0 so we re-read the fresh file from its start instead of going blind
		// until it regrows past the old offset (§2.4 logrotate-tolerated).
		const size = await wcBytes(t, source.path);
		if (size < srt.off) srt.off = 0;
		// fetchDelta tolerates a missing file (returns no lines, offset unchanged), so an
		// absent file is handled transparently and re-read when it reappears. A file
		// source never ends or vanishes — only an explicit remove tears it down.
		const { lines, nextOff } = await fetchDelta(t, source.path, srt.off);
		if (nextOff !== srt.off) srt.off = nextOff;
		return { matches: lines.map((line) => ({ line })) };
	},
};

export const DRIVERS: Record<MonitorSource["kind"], SourceDriver> = {
	process: ProcessSourceDriver,
	file: FileSourceDriver,
};

export function sourceLabel(s: MonitorSource): string {
	return s.kind === "process" ? `process:${s.procId}:${s.stream}` : `file:${s.path}`;
}

/** Parse a `process:<procId>[:<stream>]` or `file:<absolute-path>` source string. */
export function parseSource(raw: string): MonitorSource {
	const text = raw.trim();
	if (text.startsWith("file:")) {
		// Everything after `file:` is the path verbatim (absolute remote path; may
		// legitimately contain spaces/colons — do NOT split on ':').
		const path = text.slice("file:".length).trim();
		if (!path) throw new Error(`Invalid --source "${raw}": missing file path.`);
		if (!path.startsWith("/")) throw new Error(`file source path must be an absolute remote path, got "${path}".`);
		return { kind: "file", path };
	}
	if (!text.startsWith("process:")) {
		throw new Error(`Unsupported --source "${raw}". Use process:<procId>[:stdout|stderr|both] or file:<absolute-path>.`);
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
