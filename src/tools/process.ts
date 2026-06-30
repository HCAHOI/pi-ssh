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
import { assertPolicyMatchesPattern, compilePattern, validateWatchPatterns, type ProcessWatch } from "../monitor";
import { parseNotifyPolicy } from "../notify-policy";
import {
	buildClearCommand,
	buildKillCommand,
	formatProcRows,
	listProcesses,
	processRoot,
	processRunScript,
	resolveProcessRef,
} from "../process-queries";

export function setupProcessTool(ssh: SshContext): void {
	const { pi, localCwd, requireTarget, poller, monitors, render } = ssh;
	const { str, sshTitle } = render;

	pi.registerTool({
		name: "ssh_process",
		label: "ssh_process",
		description: "Manage long-running remote jobs: start/list/output/logs/kill/clear/attach. Supports notifications; see /ssh help monitor for advanced watches.",
		promptSnippet: "Manage long-running remote SSH processes",
		promptGuidelines: [
			"Use ssh_process start for long-running remote jobs; use output/logs to inspect and kill to stop.",
			"For ssh_process output/logs/kill/attach, prefer name over id when the job was started with a stable name; name resolves to the newest matching run (kill prefers a running run).",
			"Completion/logWatch notifications re-engage the agent, so do not poll. For file/probe/stall/digest monitors, see /ssh help monitor.",
		],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("start"), Type.Literal("list"), Type.Literal("output"), Type.Literal("logs"), Type.Literal("kill"), Type.Literal("clear"), Type.Literal("attach")]),
			name: Type.Optional(Type.String({ description: "Friendly process name for start; for output/logs/kill/attach, resolves to the newest matching run when id is omitted" })),
			command: Type.Optional(Type.String({ description: "Command to start on the remote" })),
			id: Type.Optional(Type.String({ description: "Remote process id returned by start/list. For output/logs/kill/attach, you may pass name instead." })),
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
				stream: Type.Optional(Type.Union([Type.Literal("stdout"), Type.Literal("stderr"), Type.Literal("both")], { description: "Stream to watch (default both)" })),
				repeat: Type.Optional(Type.Boolean({ description: "Fire repeatedly when allowed by notify policy" })),
				notify: Type.Optional(Type.String({ description: "Policy: every-match | every-n:N | throttle:DUR | digest:DUR | milestone:f1,f2,…" })),
				template: Type.Optional(Type.String({ description: "Notification template using named regex captures" })),
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
				const ref = args?.id ? str(args.id) : args?.name ? `name:${str(args.name)}` : "";
				rest = `${theme.fg("accent", a)}${ref ? ` ${theme.fg("muted", ref)}` : ""}`;
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
				// Resolve each logWatch to a first-class monitor spec, validating BEFORE we
				// launch (bad regex / bad policy / milestone-without-total fail fast).
				const watchSpecs: ProcessWatch[] = (params.logWatches ?? []).map((w) => {
					compilePattern(w.pattern);
					const notify = parseNotifyPolicy(w.notify);
					assertPolicyMatchesPattern(notify, w.pattern);
					return { pattern: w.pattern, stream: w.stream, repeat: w.repeat, notify, template: w.template };
				});
				const procId = `${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
				const dir = `${root}/${procId}`;
				const name = params.name?.trim() || procId;
				const script = processRunScript(t, { command: params.command, cwd: params.cwd, env: params.env, commandPrefix: params.commandPrefix }, localCwd);
				// Persist the completion-notification config so pollers re-arm after a
				// reconnect / pi restart (poller.rehydrate reads this). logWatches are NOT
				// written here anymore — each becomes its own standalone monitor file below.
				const notifyJson = JSON.stringify({
					name,
					alertOnSuccess: params.alertOnSuccess ?? false,
					alertOnFailure: params.alertOnFailure ?? true,
					alertOnKill: params.alertOnKill ?? false,
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
				// Each logWatch becomes a first-class, persisted standalone monitor bound to
				// this job (== ssh_monitor create). They sweep the fresh logs from the top
				// and auto-remove when the job ends; manageable via ssh_monitor.
				const watchCount = watchSpecs.length;
				if (watchCount) await monitors.createForProcess(t, procId, watchSpecs, name);
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

			const rowsForNameResolution = params.id?.trim() ? [] : await listProcesses(t, signal);
			const resolved = resolveProcessRef(rowsForNameResolution, { id: params.id, name: params.name }, { preferRunning: params.action === "kill" });
			const procId = resolved.id;
			const resolvedNote = resolved.matchedBy === "name" ? `Resolved process name "${params.name?.trim()}" -> ${procId}.\n` : "";
			const dir = `${root}/${procId}`;

			if (params.action === "attach") {
				if (poller.has(procId)) {
					return { content: [{ type: "text" as const, text: `${resolvedNote}Already watching ${procId}.` }] };
				}
				const probe = `d=${shQuote(dir)}; if [ ! -d "$d" ]; then echo 'process not found' >&2; exit 2; fi; o=$(wc -c < "$d/stdout.log" 2>/dev/null || echo 0); e=$(wc -c < "$d/stderr.log" 2>/dev/null || echo 0); n=$(base64 < "$d/notify.json" 2>/dev/null | tr -d '\n'); printf '%s\t%s\t%s\n' "$o" "$e" "$n"`;
				const pr = await runRemoteCommand(t, probe, { signal, login: false });
				if (pr.code !== 0) throw new Error(`${sshFailureMessage(pr)}: ${pr.stderr.toString().trim() || pr.stdout.toString().trim()}`);
				const [outStr = "0", errStr = "0", notifyB64 = ""] = pr.stdout.toString().trim().split("\t");
				let saved: NotifyConfig = {};
				if (notifyB64) {
					try { saved = JSON.parse(Buffer.from(notifyB64, "base64").toString()); } catch { /* ignore corrupt config */ }
				}
				const name = params.name?.trim() || saved.name || procId;
				const offsets = { stdout: Number.parseInt(outStr, 10) || 0, stderr: Number.parseInt(errStr, 10) || 0 };
				// Explicit logWatches at attach are first-class (== ssh_monitor create),
				// seeking EOF since the job is already running. Validate before persisting.
				const userWatches: ProcessWatch[] = (params.logWatches ?? []).map((w) => {
					compilePattern(w.pattern);
					const notify = parseNotifyPolicy(w.notify);
					assertPolicyMatchesPattern(notify, w.pattern);
					return { pattern: w.pattern, stream: w.stream, repeat: w.repeat, notify, template: w.template };
				});
				// saved.watches only exist for pre-upgrade jobs; keep them in notify.json and
				// re-arm via the legacy shim. New jobs have none → no `watches` key is
				// written (preserving the first-class invariant).
				const legacyWatches = saved.watches ?? [];
				validateWatchPatterns(legacyWatches);
				const persistCfg: NotifyConfig = {
					name,
					alertOnSuccess: params.alertOnSuccess ?? saved.alertOnSuccess ?? true,
					alertOnFailure: params.alertOnFailure ?? saved.alertOnFailure ?? true,
					alertOnKill: params.alertOnKill ?? saved.alertOnKill ?? false,
					...(legacyWatches.length ? { watches: legacyWatches } : {}),
				};
				// Persist merged prefs and clear the notified marker so a finished job
				// re-fires its completion exactly once for this explicit attach.
				const persist = `d=${shQuote(dir)}; printf %s ${shQuote(JSON.stringify(persistCfg))} > "$d/notify.json"; rm -f "$d/notified"`;
				await runRemoteCommand(t, persist, { signal, login: false }).catch(() => {});
				poller.startPoller({
					procId,
					name,
					dir,
					target: t,
					alertOnSuccess: persistCfg.alertOnSuccess ?? true,
					alertOnFailure: persistCfg.alertOnFailure ?? true,
					alertOnKill: persistCfg.alertOnKill ?? false,
				});
				if (legacyWatches.length) monitors.armLegacyWatches(t, procId, legacyWatches, { offsets, name });
				if (userWatches.length) await monitors.createForProcess(t, procId, userWatches, name, offsets);
				const watching = legacyWatches.length + userWatches.length;
				return { content: [{ type: "text" as const, text: `${resolvedNote}Watching ${procId} (${name}). Will notify on completion${watching ? " and matching log lines" : ""}.` }] };
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
				return { content: [{ type: "text" as const, text: `${resolvedNote}${r.stdout.toString() || "(no output)"}` }] };
			}

			if (params.action === "logs") {
				return { content: [{ type: "text" as const, text: `${resolvedNote}stdout: ${dir}/stdout.log\nstderr: ${dir}/stderr.log\nrun script: ${dir}/run.sh` }], details: { stdout: `${dir}/stdout.log`, stderr: `${dir}/stderr.log`, script: `${dir}/run.sh` } };
			}

			// Agent-initiated kill: drop the poller and any process-bound monitors first
			// so they do not fire a spurious completion/watch notification for an expected
			// teardown.
			poller.stopPoller(procId);
			monitors.stopForProcess(procId);
			const r = await runRemoteCommand(t, buildKillCommand(dir), { signal });
			if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
			return { content: [{ type: "text" as const, text: `${resolvedNote}${r.stdout.toString().trim()}` }] };
		},
	});
}
