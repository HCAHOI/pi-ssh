// ---------------------------------------------------------------------------
// ssh_process background notification poller
// ---------------------------------------------------------------------------
// Each tracked job gets a PollerState with its own SshTarget and byte offsets.
// A per-job setInterval tails the remote logs (delta by byte offset), matches
// log watches, and fires a completion notification when the job ends. State is
// re-armed from each job's persisted notify.json after reconnect/restart.

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { PollerState, SshTarget, WatchSpec, WatchState } from "./types";
import { formatDuration, shQuote } from "./utils";
import { runRemoteCommand } from "./ssh/transport";
import { processRoot } from "./process-queries";
import { sendProcessMessage } from "./notify";

const POLL_INTERVAL_MS = 3000;

export interface NotifyConfig {
	name?: string;
	alertOnSuccess?: boolean;
	alertOnFailure?: boolean;
	alertOnKill?: boolean;
	watches?: WatchSpec[];
}

export function buildWatchStates(specs: WatchSpec[] | undefined): WatchState[] {
	return (specs ?? []).map((w) => {
		let re: RegExp;
		try {
			re = new RegExp(w.pattern);
		} catch (e) {
			throw new Error(`Invalid logWatches pattern /${w.pattern}/: ${e instanceof Error ? e.message : String(e)}`);
		}
		return { re, pattern: w.pattern, stream: w.stream ?? "both", repeat: w.repeat ?? false, fired: false };
	});
}

export interface StartPollerArgs {
	procId: string;
	name: string;
	dir: string;
	target: SshTarget;
	alertOnSuccess: boolean;
	alertOnFailure: boolean;
	alertOnKill: boolean;
	watches: WatchState[];
	/** Initial byte offsets. Fresh start: {0,0} (watch from the top). Rehydrate on
	 * reconnect/restart: seek to current EOF so historical log lines are not
	 * re-matched (only completion + new lines fire). */
	offsets?: { stdout: number; stderr: number };
	/** Epoch ms of the fresh start (for run-duration reporting); omit for rehydrate. */
	startedAt?: number;
}

export interface PollerManager {
	startPoller(args: StartPollerArgs): void;
	stopPoller(procId: string): void;
	stopAll(): void;
	has(procId: string): boolean;
	/** Repoint every live poller at a new connection (same host+cwd reconnect). */
	repointAll(t: SshTarget): void;
	/** Record the most-recent start for a job name (supersede detection). */
	markLatestStart(name: string, procId: string): void;
	rehydrate(t: SshTarget): Promise<void>;
}

export function createPollerManager(pi: ExtensionAPI): PollerManager {
	const pollers = new Map<string, PollerState>();
	// Most-recent start per job name. Lets a late completion notification flag that a
	// newer run of the same name was started after it, so the agent can tell stale
	// (superseded) alerts from current ones in parallel multi-run workflows. Survives
	// the finished poller's removal from `pollers`.
	const latestStartByName = new Map<string, string>();

	const emit = (content: string, details: Record<string, unknown>): void => sendProcessMessage(pi, content, details);

	function stopPoller(procId: string): void {
		const p = pollers.get(procId);
		if (!p) return;
		if (p.timer) clearInterval(p.timer);
		p.timer = null;
		pollers.delete(procId);
	}

	function stopAll(): void {
		for (const id of [...pollers.keys()]) stopPoller(id);
	}

	function statusCmd(dir: string): string {
		return `d=${shQuote(dir)}; if [ ! -d "$d" ]; then printf 'gone\\t\\n'; else pid=$(cat "$d/pid" 2>/dev/null || true); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then printf 'running\\t\\n'; else code=$(cat "$d/exit_code" 2>/dev/null || true); printf 'done\\t%s\\n' "$code"; fi; fi`;
	}

	function neededStreams(p: PollerState): Array<"stdout" | "stderr"> {
		const set = new Set<"stdout" | "stderr">();
		for (const w of p.watches) {
			if (w.stream === "stdout" || w.stream === "both") set.add("stdout");
			if (w.stream === "stderr" || w.stream === "both") set.add("stderr");
		}
		return [...set];
	}

	// Read new complete lines appended to a log since the last byte offset. We only
	// advance the offset to the last newline, so partial lines (and multibyte chars
	// straddling the boundary) are re-read intact next tick. Survives Mac sleep:
	// offsets are byte positions in the remote file, so nothing logged while asleep
	// is lost — it is swept on the first tick after wake.
	async function fetchDeltaLines(p: PollerState, stream: "stdout" | "stderr"): Promise<string[]> {
		const file = `${p.dir}/${stream}.log`;
		const start = p.off[stream];
		const r = await runRemoteCommand(p.target, `tail -c +${start + 1} -- ${shQuote(file)} 2>/dev/null || true`, { timeout: 20, login: false });
		if (r.code !== 0) return [];
		const buf = r.stdout;
		if (buf.length === 0) return [];
		const lastNl = buf.lastIndexOf(0x0a);
		if (lastNl === -1) return []; // no complete line yet
		p.off[stream] = start + lastNl + 1;
		return buf.subarray(0, lastNl).toString("utf8").split("\n");
	}

	function runWatches(p: PollerState, stream: "stdout" | "stderr", lines: string[]): void {
		for (const w of p.watches) {
			if (w.fired && !w.repeat) continue;
			if (w.stream !== "both" && w.stream !== stream) continue;
			for (const line of lines) {
				if (!w.re.test(line)) continue;
				emit(`🔔 ssh_process "${p.name}" (${p.procId}) matched /${w.pattern}/ on ${stream}:\n${line}`, {
					kind: "watch",
					procId: p.procId,
					name: p.name,
					pattern: w.pattern,
					stream,
					line,
				});
				if (!w.repeat) {
					w.fired = true;
					break;
				}
			}
		}
	}

	async function sweepWatches(p: PollerState): Promise<void> {
		for (const stream of neededStreams(p)) {
			const lines = await fetchDeltaLines(p, stream);
			if (lines.length) runWatches(p, stream, lines);
		}
	}

	async function fireCompletion(p: PollerState, codeStr: string): Promise<void> {
		const code = codeStr === "" ? null : Number.parseInt(codeStr, 10);
		let outcome: "success" | "failure" | "killed";
		if (code === 0) outcome = "success";
		else if (code === null) outcome = "killed"; // exit_code absent => SIGKILL (EXIT trap never ran)
		else if (code >= 128) outcome = "killed"; // 128 + signal
		else outcome = "failure";
		// Mark completion handled so a later reconnect/rehydrate never re-notifies for
		// this job, regardless of whether this particular alert was opted into.
		await runRemoteCommand(p.target, `touch -- ${shQuote(`${p.dir}/notified`)}`, { timeout: 20, login: false }).catch(() => {});
		const want = outcome === "success" ? p.alertOnSuccess : outcome === "killed" ? p.alertOnKill : p.alertOnFailure;
		if (!want) return;
		const tailCmd = `d=${shQuote(p.dir)}; echo '--- stdout (tail) ---'; tail -n 15 "$d/stdout.log" 2>/dev/null; echo '--- stderr (tail) ---'; tail -n 15 "$d/stderr.log" 2>/dev/null`;
		const tr = await runRemoteCommand(p.target, tailCmd, { timeout: 20, login: false }).catch(() => null);
		const tail = tr && tr.code === 0 ? tr.stdout.toString().trimEnd() : "";
		const emoji = outcome === "success" ? "✅" : outcome === "killed" ? "⛔" : "❌";
		const codeLabel = code === null ? "" : ` (exit ${code})`;
		const latest = latestStartByName.get(p.name);
		const superseded = latest !== undefined && latest !== p.procId;
		const supersedeLabel = superseded ? " [superseded: a newer run of this name was started after it]" : "";
		const ageLabel = p.startedAt ? ` after ${formatDuration(Date.now() - p.startedAt)}` : "";
		emit(`${emoji} ssh_process "${p.name}" (${p.procId})${supersedeLabel} ${outcome}${codeLabel}${ageLabel}.${tail ? `\n${tail}` : ""}`, {
			kind: "completion",
			procId: p.procId,
			name: p.name,
			outcome,
			code,
			superseded,
		});
	}

	async function tick(p: PollerState): Promise<void> {
		if (p.busy || p.finished) return;
		p.busy = true;
		try {
			const r = await runRemoteCommand(p.target, statusCmd(p.dir), { timeout: 20, login: false });
			if (r.code !== 0) return; // transient (e.g. stale socket post-wake); retry next tick
			// login:false avoids profile banners, but defensively take the last
			// non-empty line rather than the first.
			const out = r.stdout.toString().trim();
			if (!out) return;
			const [status, code = ""] = out.split("\n").pop()!.split("\t");
			if (status === "gone") {
				// dir removed (e.g. ssh_process clear / manual rm): orphaned poller,
				// stop silently without a spurious completion notification.
				p.finished = true;
				stopPoller(p.procId);
				return;
			}
			if (p.watches.length) await sweepWatches(p);
			if (status === "running") return;
			if (p.watches.length) await sweepWatches(p); // final sweep before we stop
			p.finished = true;
			await fireCompletion(p, code);
			stopPoller(p.procId);
		} catch {
			// Swallow: a single failed tick must never escape setInterval and kill the
			// poller. The next tick retries (critical for Mac sleep/wake recovery).
		} finally {
			p.busy = false;
		}
	}

	function startPoller(args: StartPollerArgs): void {
		if (!args.alertOnSuccess && !args.alertOnFailure && !args.alertOnKill && args.watches.length === 0) return;
		if (pollers.has(args.procId)) return; // already tracked
		const state: PollerState = {
			...args,
			off: args.offsets ?? { stdout: 0, stderr: 0 },
			timer: null,
			busy: false,
			finished: false,
		};
		pollers.set(state.procId, state);
		state.timer = setInterval(() => {
			void tick(state);
		}, POLL_INTERVAL_MS);
		state.timer.unref?.();
	}

	// Re-arm pollers from each job's persisted notify.json after a (re)connect, so
	// completion / log-watch notifications survive reconnect, ssh_connect switching,
	// and full pi restarts. Jobs already tracked or already notified are skipped;
	// offsets seek to EOF so historical watch lines do not re-fire. A finished,
	// un-notified job fires its missed completion on the first tick.
	async function rehydrate(t: SshTarget): Promise<void> {
		if (!t.hasPython) return;
		const root = processRoot(t);
		const script = `
import base64, json, os, sys
root = sys.argv[1]
out = []
if os.path.isdir(root):
    for d in sorted(os.listdir(root)):
        p = os.path.join(root, d)
        nf = os.path.join(p, 'notify.json')
        if not (os.path.isdir(p) and os.path.isfile(nf)):
            continue
        try:
            notify = open(nf).read()
        except OSError:
            continue
        pid = ''
        try:
            pid = open(os.path.join(p, 'pid')).read().strip()
        except OSError:
            pass
        running = False
        if pid:
            try:
                os.kill(int(pid), 0)
                running = True
            except (OSError, ValueError):
                running = False
        def sz(f):
            try:
                return os.path.getsize(os.path.join(p, f))
            except OSError:
                return 0
        out.append({
            'id': d,
            'notify': base64.b64encode(notify.encode()).decode(),
            'running': running,
            'notified': os.path.isfile(os.path.join(p, 'notified')),
            'outSize': sz('stdout.log'),
            'errSize': sz('stderr.log'),
        })
print(json.dumps(out))
`;
		let entries: Array<{ id: string; notify: string; running: boolean; notified: boolean; outSize: number; errSize: number }>;
		try {
			const r = await runRemoteCommand(t, `python3 -c ${shQuote(script)} ${shQuote(root)}`, { timeout: 20, login: false });
			if (r.code !== 0) return;
			entries = JSON.parse(r.stdout.toString() || "[]");
		} catch {
			return;
		}
		for (const e of entries) {
			if (pollers.has(e.id) || e.notified) continue;
			let cfg: NotifyConfig;
			try {
				cfg = JSON.parse(Buffer.from(e.notify, "base64").toString());
			} catch {
				continue;
			}
			startPoller({
				procId: e.id,
				name: cfg.name?.trim() || e.id,
				dir: `${root}/${e.id}`,
				target: t,
				alertOnSuccess: cfg.alertOnSuccess ?? false,
				alertOnFailure: cfg.alertOnFailure ?? true,
				alertOnKill: cfg.alertOnKill ?? false,
				watches: buildWatchStates(cfg.watches),
				offsets: { stdout: e.outSize, stderr: e.errSize },
			});
		}
	}

	return {
		startPoller,
		stopPoller,
		stopAll,
		has: (procId) => pollers.has(procId),
		repointAll: (t) => {
			for (const p of pollers.values()) p.target = t;
		},
		markLatestStart: (name, procId) => {
			latestStartByName.set(name, procId);
		},
		rehydrate,
	};
}
