// ---------------------------------------------------------------------------
// ssh_process: background remote jobs with completion / log-watch notifications
// ---------------------------------------------------------------------------

import { randomBytes } from "node:crypto";
import { Type } from "typebox";
import type { WatchSpec } from "../types";
import type { SshContext } from "../context";
import { shQuote } from "../utils";
import { runRemoteCommand, sshFailureMessage } from "../ssh/transport";
import { createRemoteBashOps } from "../remote-ops";
import type { NotifyConfig } from "../poller";
import { validateWatchPatterns } from "../monitor";
import {
	buildClearCommand,
	buildKillCommand,
	formatProcRows,
	listProcesses,
	processRoot,
	processRunScript,
} from "../process-queries";

export function setupProcessTool(ssh: SshContext): void {
	const { pi, localCwd, requireTarget, poller, monitors, render } = ssh;
	const { str, sshTitle } = render;

	pi.registerTool({
		name: "ssh_process",
		label: "ssh_process",
		description: "Manage long-running processes on the active SSH remote. Starts commands in the background with logs under remote .pi-ssh-processes/<id>/; supports start/list/output/logs/kill/clear/attach. output accepts followSeconds to live-stream new log lines; attach re-arms completion/log-watch notifications for an existing job. list and output report the captured exit code once a job finishes; clear prunes finished jobs. On start, alertOnSuccess/alertOnFailure/alertOnKill and logWatches push a notification (re-engaging the agent) when the job ends or a log line matches — so you never poll. Notifications survive Mac sleep AND reconnect/pi-restart: each job persists its notify config and pollers are re-armed on connect, sweeping missed completions/log lines.",
		promptSnippet: "Manage long-running remote SSH processes",
		promptGuidelines: [
			"Use ssh_process start for long-running remote jobs such as training, dev servers, and log tails instead of blocking ssh_bash.",
			"Use ssh_process output (optionally followSeconds) to inspect stdout/stderr and ssh_process kill to stop remote jobs.",
			"Background jobs push a notification when they finish or a logWatch matches — rely on it instead of polling list/output. After a reconnect, use ssh_process attach <id> to resume notifications for a job started earlier.",
		],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("start"), Type.Literal("list"), Type.Literal("output"), Type.Literal("logs"), Type.Literal("kill"), Type.Literal("clear"), Type.Literal("attach")]),
			name: Type.Optional(Type.String({ description: "Friendly process name for start" })),
			command: Type.Optional(Type.String({ description: "Command to start on the remote" })),
			id: Type.Optional(Type.String({ description: "Remote process id returned by start/list" })),
			cwd: Type.Optional(Type.String({ description: "Remote working directory, defaults to remote cwd" })),
			env: Type.Optional(Type.Record(Type.String(), Type.String(), { description: "Environment variables exported before command" })),
			commandPrefix: Type.Optional(Type.String({ description: "Shell code run before command" })),
			lines: Type.Optional(Type.Number({ description: "Number of recent log lines for output (default 80)" })),
			followSeconds: Type.Optional(Type.Number({ description: "output: stream new stdout/stderr lines live for this many seconds before returning" })),
			alertOnSuccess: Type.Optional(Type.Boolean({ description: "start: notify when the job exits 0 (default false)" })),
			alertOnFailure: Type.Optional(Type.Boolean({ description: "start: notify when the job exits non-zero (default true)" })),
			alertOnKill: Type.Optional(Type.Boolean({ description: "start: notify when the job is killed by a signal (default false)" })),
			logWatches: Type.Optional(Type.Array(Type.Object({
				pattern: Type.String({ description: "Regex matched per log line" }),
				stream: Type.Optional(Type.Union([Type.Literal("stdout"), Type.Literal("stderr"), Type.Literal("both")], { description: "Which stream to watch (default both)" })),
				repeat: Type.Optional(Type.Boolean({ description: "Fire every match (default false: one-shot)" })),
			}), { description: "start: notify when a log line matches; re-engages the agent" })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const a = str(args?.action) || "?";
			let rest: string;
			if (a === "start") {
				const nm = args?.name ? ` ${theme.fg("accent", `"${args.name}"`)}` : "";
				const cmd = str(args?.command);
				rest = `${theme.fg("accent", "start")}${nm}${cmd ? ` ${theme.fg("muted", `$ ${cmd}`)}` : ""}`;
			} else if (a === "kill" || a === "output" || a === "logs" || a === "attach") {
				rest = `${theme.fg("accent", a)}${args?.id ? ` ${theme.fg("muted", str(args.id))}` : ""}`;
			} else {
				rest = theme.fg("accent", a);
			}
			return sshTitle("process", rest, theme, context);
		},
		async execute(_id, params: {
			action: "start" | "list" | "output" | "logs" | "kill" | "clear" | "attach";
			name?: string;
			command?: string;
			id?: string;
			cwd?: string;
			env?: Record<string, string>;
			commandPrefix?: string;
			lines?: number;
			followSeconds?: number;
			alertOnSuccess?: boolean;
			alertOnFailure?: boolean;
			alertOnKill?: boolean;
			logWatches?: WatchSpec[];
		}, signal, onUpdate) {
			const t = requireTarget();
			const root = processRoot(t);
			if (params.action === "start") {
				if (!params.command?.trim()) throw new Error("ssh_process start requires command");
				validateWatchPatterns(params.logWatches); // fail fast on a bad regex before launching
				const procId = `${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
				const dir = `${root}/${procId}`;
				const name = params.name?.trim() || procId;
				const script = processRunScript(t, { command: params.command, cwd: params.cwd, env: params.env, commandPrefix: params.commandPrefix }, localCwd);
				// Persist the notification config so pollers can be re-armed after a
				// reconnect / pi restart (poller.rehydrate reads this).
				const notifyJson = JSON.stringify({
					name,
					alertOnSuccess: params.alertOnSuccess ?? false,
					alertOnFailure: params.alertOnFailure ?? true,
					alertOnKill: params.alertOnKill ?? false,
					watches: params.logWatches ?? [],
				} satisfies NotifyConfig);
				const cmd = [
					`mkdir -p ${shQuote(dir)}`,
					`printf %s ${shQuote(name)} > ${shQuote(`${dir}/name`)}`,
					`printf %s ${shQuote(params.command)} > ${shQuote(`${dir}/command`)}`,
					`printf %s ${shQuote(notifyJson)} > ${shQuote(`${dir}/notify.json`)}`,
					`cat > ${shQuote(`${dir}/run.sh`)}`,
					`chmod +x ${shQuote(`${dir}/run.sh`)}`,
					// Group with { ...; } so `&` backgrounds only nohup; echo $! then captures
					// ITS pid. Without the braces, `&` would background the whole `&&` setup
					// chain and `echo $! > pid` would run before mkdir created the dir.
					`{ nohup setsid bash ${shQuote(`${dir}/run.sh`)} > ${shQuote(`${dir}/stdout.log`)} 2> ${shQuote(`${dir}/stderr.log`)} < /dev/null & echo $! > ${shQuote(`${dir}/pid`)}; }`,
					`printf %s ${shQuote(procId)}`,
				].join(" && ");
				const r = await runRemoteCommand(t, cmd, { stdin: script, signal });
				if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
				// Record this as the most recent run of `name` so an older run's late
				// completion alert can be flagged as superseded.
				poller.markLatestStart(name, procId);
				poller.startPoller({
					procId,
					name,
					dir,
					target: t,
					alertOnSuccess: params.alertOnSuccess ?? false,
					alertOnFailure: params.alertOnFailure ?? true,
					alertOnKill: params.alertOnKill ?? false,
					startedAt: Date.now(),
				});
				// logWatches is sugar for process-bound monitors: arm them from the fresh
				// (empty) logs so they sweep from the top, exactly as before. The watch
				// specs persist in notify.json and rehydrate via the monitor subsystem.
				const watchCount = params.logWatches?.length ?? 0;
				if (watchCount) monitors.armSugarWatches(t, procId, params.logWatches ?? [], { name });
				// Point-of-need discovery: this job already alerts on failure; nudge the
				// agent toward success/log-watch notifications only when it did not opt in,
				// so it stops polling list/output. Suppressed once the feature is used.
				const optedIntoNotify = (params.alertOnSuccess ?? false) || (params.alertOnKill ?? false) || watchCount > 0;
				const tip = optedIntoNotify
					? ""
					: "\nWill notify you automatically if it fails — do not poll. Pass alertOnSuccess and/or logWatches to also be notified on success or when a log line matches.";
				return { content: [{ type: "text" as const, text: `Started remote process ${procId} (${name})\nstdout: ${dir}/stdout.log\nstderr: ${dir}/stderr.log${tip}` }], details: { id: procId, name, stdout: `${dir}/stdout.log`, stderr: `${dir}/stderr.log` } };
			}

			if (params.action === "list") {
				const rows = await listProcesses(t, signal);
				const text = rows.length ? formatProcRows(rows) : "No remote processes.";
				return { content: [{ type: "text" as const, text }] };
			}

			if (params.action === "clear") {
				const r = await runRemoteCommand(t, buildClearCommand(root), { signal });
				if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
				return { content: [{ type: "text" as const, text: r.stdout.toString().trim() }] };
			}

			if (!params.id?.trim()) throw new Error(`ssh_process ${params.action} requires id`);
			const dir = `${root}/${params.id}`;

			if (params.action === "attach") {
				if (poller.has(params.id)) {
					return { content: [{ type: "text" as const, text: `Already watching ${params.id}.` }] };
				}
				const probe = `d=${shQuote(dir)}; if [ ! -d "$d" ]; then echo 'process not found' >&2; exit 2; fi; o=$(wc -c < "$d/stdout.log" 2>/dev/null || echo 0); e=$(wc -c < "$d/stderr.log" 2>/dev/null || echo 0); n=$(base64 < "$d/notify.json" 2>/dev/null | tr -d '\n'); printf '%s\t%s\t%s\n' "$o" "$e" "$n"`;
				const pr = await runRemoteCommand(t, probe, { signal, login: false });
				if (pr.code !== 0) throw new Error(`${sshFailureMessage(pr)}: ${pr.stderr.toString().trim() || pr.stdout.toString().trim()}`);
				const [outStr = "0", errStr = "0", notifyB64 = ""] = pr.stdout.toString().trim().split("\t");
				let saved: NotifyConfig = {};
				if (notifyB64) {
					try { saved = JSON.parse(Buffer.from(notifyB64, "base64").toString()); } catch { /* ignore corrupt config */ }
				}
				const cfg: Required<NotifyConfig> = {
					name: params.name?.trim() || saved.name || params.id,
					alertOnSuccess: params.alertOnSuccess ?? saved.alertOnSuccess ?? true,
					alertOnFailure: params.alertOnFailure ?? saved.alertOnFailure ?? true,
					alertOnKill: params.alertOnKill ?? saved.alertOnKill ?? false,
					watches: params.logWatches ?? saved.watches ?? [],
				};
				// Fail fast on a bad regex BEFORE persisting cfg to notify.json, so an
				// invalid pattern can never poison the job's persisted config (which would
				// then trip every future rehydrate).
				validateWatchPatterns(cfg.watches);
				// Persist merged prefs and clear the notified marker so a finished job
				// re-fires its completion exactly once for this explicit attach.
				const persist = `d=${shQuote(dir)}; printf %s ${shQuote(JSON.stringify(cfg))} > "$d/notify.json"; rm -f "$d/notified"`;
				await runRemoteCommand(t, persist, { signal, login: false }).catch(() => {});
				poller.startPoller({
					procId: params.id,
					name: cfg.name,
					dir,
					target: t,
					alertOnSuccess: cfg.alertOnSuccess,
					alertOnFailure: cfg.alertOnFailure,
					alertOnKill: cfg.alertOnKill,
				});
				// Re-arm the job's logWatch monitors, seeking to current EOF so historical
				// lines are not re-matched (mirrors the monitor rehydrate rule).
				if (cfg.watches.length) {
					monitors.armSugarWatches(t, params.id, cfg.watches, { offsets: { stdout: Number.parseInt(outStr, 10) || 0, stderr: Number.parseInt(errStr, 10) || 0 }, name: cfg.name });
				}
				return { content: [{ type: "text" as const, text: `Watching ${params.id} (${cfg.name}). Will notify on completion${cfg.watches.length ? " and matching log lines" : ""}.` }] };
			}

			if (params.action === "output") {
				const lines = Math.max(1, Math.floor(params.lines ?? 80));
				const follow = params.followSeconds && params.followSeconds > 0 ? Math.floor(params.followSeconds) : 0;
				if (follow > 0) {
					// Live stream new lines from both logs for `follow` seconds (bounded by the
					// remote `timeout`), pushing incremental output to the tool view.
					const ops = createRemoteBashOps(t, localCwd);
					const MAX = 8 * 1024;
					let acc = "";
					const onData = (d: Buffer) => {
						acc += d.toString();
						if (acc.length > MAX) acc = `…${acc.slice(-MAX)}`;
						onUpdate?.({ content: [{ type: "text", text: acc }], details: undefined });
					};
					await ops
						.exec(`timeout ${follow} tail -n ${lines} -F stdout.log stderr.log 2>/dev/null`, dir, { onData, signal, timeout: follow + 15 })
						.catch(() => {});
				}
				const cmd = `d=${shQuote(dir)}; test -d "$d" || { echo 'process not found' >&2; exit 2; }; echo '--- stdout ---'; tail -n ${lines} "$d/stdout.log" 2>/dev/null || true; echo '--- stderr ---'; tail -n ${lines} "$d/stderr.log" 2>/dev/null || true; pid=$(cat "$d/pid" 2>/dev/null || true); if [ -f "$d/exit_code" ]; then echo "--- exited, code: $(cat "$d/exit_code") ---"; elif [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then echo "--- still running (pid $pid) ---"; else echo '--- exited (no exit code recorded) ---'; fi`;
				const r = await runRemoteCommand(t, cmd, { signal });
				if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
				return { content: [{ type: "text" as const, text: r.stdout.toString() || "(no output)" }] };
			}

			if (params.action === "logs") {
				return { content: [{ type: "text" as const, text: `stdout: ${dir}/stdout.log\nstderr: ${dir}/stderr.log\nrun script: ${dir}/run.sh` }], details: { stdout: `${dir}/stdout.log`, stderr: `${dir}/stderr.log`, script: `${dir}/run.sh` } };
			}

			// Agent-initiated kill: drop the poller and any process-bound monitors first
			// so they do not fire a spurious completion/watch notification for an expected
			// teardown.
			poller.stopPoller(params.id);
			monitors.stopForProcess(params.id);
			const r = await runRemoteCommand(t, buildKillCommand(dir), { signal });
			if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
			return { content: [{ type: "text" as const, text: r.stdout.toString().trim() }] };
		},
	});
}
