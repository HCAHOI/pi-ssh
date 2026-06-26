// ---------------------------------------------------------------------------
// ssh_process background notification poller
// ---------------------------------------------------------------------------
// Each tracked job gets a PollerState with its own SshTarget. A per-job
// setInterval polls the job's status and fires a completion notification when it
// ends. State is re-armed from each job's persisted notify.json after
// reconnect/restart. Log-line monitoring lives in the monitor subsystem
// (src/monitor.ts), no longer in the poller.

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { PollerState, SshTarget, WatchSpec } from "./types";
import { formatDuration, shQuote } from "./utils";
import { runRemoteCommand } from "./ssh/transport";
import { processRoot } from "./process-queries";
import { sendProcessMessage } from "./notify";

const POLL_INTERVAL_MS = 3000;

// Persisted per-job notify config (notify.json). `watches` is retained for the
// ssh_process logWatches sugar: it is the on-disk home of process-bound monitors,
// which the monitor subsystem desugars on (re)connect (MONITOR_PLAN §6). The
// completion poller itself no longer reads it.
export interface NotifyConfig {
	name?: string;
	alertOnSuccess?: boolean;
	alertOnFailure?: boolean;
	alertOnKill?: boolean;
	watches?: WatchSpec[];
}

export interface StartPollerArgs {
	procId: string;
	name: string;
	dir: string;
	target: SshTarget;
	alertOnSuccess: boolean;
	alertOnFailure: boolean;
	alertOnKill: boolean;
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
			if (status === "running") return;
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
		if (!args.alertOnSuccess && !args.alertOnFailure && !args.alertOnKill) return;
		if (pollers.has(args.procId)) return; // already tracked
		const state: PollerState = {
			...args,
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
	// completion notifications survive reconnect, ssh_connect switching, and full pi
	// restarts. Jobs already tracked or already notified are skipped. A finished,
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
        out.append({
            'id': d,
            'notify': base64.b64encode(notify.encode()).decode(),
            'running': running,
            'notified': os.path.isfile(os.path.join(p, 'notified')),
        })
print(json.dumps(out))
`;
		let entries: Array<{ id: string; notify: string; running: boolean; notified: boolean }>;
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
