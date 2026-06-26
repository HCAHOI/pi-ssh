// ---------------------------------------------------------------------------
// ssh_monitor: runtime-managed log monitors, decoupled from ssh_process.
// A monitor binds to a remote process stream and fires a notification when a
// log line matches its regex. Unlike ssh_process logWatches (frozen at start),
// monitors can be created/updated/paused/removed mid-run and bind to ANY running
// job. See docs/MONITOR_PLAN.md.
// ---------------------------------------------------------------------------

import { Type } from "typebox";
import type { SshContext } from "../context";
import { parseSource, type MonitorRow } from "../monitor";
import { parseNotifyPolicy } from "../notify-policy";

function formatRows(rows: MonitorRow[]): string {
	if (!rows.length) return "No monitors.";
	return rows
		.map((r) => {
			const state = r.paused ? "paused" : r.fired && !r.repeat ? "fired" : "active";
			const name = r.name ? ` ${r.name}` : "";
			const kind = r.kind === "legacy" ? " (legacy)" : "";
			// Probe rows have no regex — show the predicate; regex rows append it when set.
			const what = r.pattern ? `/${r.pattern}/${r.notifyWhen ? ` when ${r.notifyWhen}` : ""}` : `notifyWhen ${r.notifyWhen ?? ""}`;
			return `${r.id}${name}${kind}\t${r.source}\t${what}\t${r.notify}\t${state}\tmatches=${r.matchCount}`;
		})
		.join("\n");
}

export function setupMonitorTool(ssh: SshContext): void {
	const { pi, requireTarget, monitors, render } = ssh;
	const { str, sshTitle } = render;

	pi.registerTool({
		name: "ssh_monitor",
		label: "ssh_monitor",
		description:
			"Runtime-managed log monitors on the active SSH remote, decoupled from ssh_process. A monitor watches a running job's stdout/stderr for a regex and notifies you (re-engaging the agent) when a line matches — created/changed/paused/removed at any time, bound to any job's id (not frozen at start like ssh_process logWatches). A notify policy controls HOW matches map to notifications, killing spam: every-match (default) | every-n:N | throttle:DUR | digest:DUR | milestone:f1,f2,… (milestone needs a (?<total>…) capture). Named regex groups (e.g. (?<n>\\d+)/(?<total>\\d+)) feed an optional {token} template and digest/milestone ETA. Standalone monitors persist and re-arm on reconnect/pi-restart; matching seeks to the source's current end so history does not re-fire. Sources: process:<procId>[:stdout|stderr|both] (from ssh_process list); file:<absolute-remote-path> to watch any remote file (logrotate-safe, never self-removes); probe:<command> to poll a command every intervalMs and fire when a notifyWhen predicate over its {value,exitCode} holds (optionally for N consecutive samples). notifyWhen is a safe expression (==, !=, <, <=, >, >=, &&, ||, !, +-*/, parens) over named captures / value / exitCode / matchCount / elapsedMs. expectEveryMs adds silence/stall detection: fire once if no match arrives within the window (catches hangs that never exit). Actions: create/list/update/pause/resume/remove/attach.",
		promptSnippet: "Manage runtime log monitors on the SSH remote",
		promptGuidelines: [
			"Use ssh_monitor create source=process:<id>:stderr pattern='<regex>' to watch a running ssh_process job for a log line without restarting it; use source=file:/abs/path to watch any remote file (logrotate-safe, never self-removes).",
			"For a noisy/progress line, add a notify policy to avoid spam: notify=digest:5m, notify=every-n:50, notify=throttle:60s, or notify=milestone:0.25,0.5,1.0 (needs a (?<total>…) capture).",
			"Poll a command with source='probe:nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits' intervalMs=60000 notifyWhen='value==0' consecutive=3 to fire when a metric crosses a threshold (probe requires notifyWhen; add consecutive to debounce). notifyWhen also filters regex matches, e.g. pattern='loss=(?<loss>[\\d.]+)' notifyWhen='loss>10'.",
			"Detect a stalled/hung job with expectEveryMs: e.g. pattern='epoch' expectEveryMs=300000 fires once if no 'epoch' line appears for 5 minutes (re-armed when the next match arrives).",
			"Monitors notify you when the policy fires — rely on the notification instead of polling ssh_process output. Use list/pause/resume/remove to manage them; standalone monitors survive reconnect.",
		],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("create"), Type.Literal("list"), Type.Literal("update"), Type.Literal("pause"), Type.Literal("resume"), Type.Literal("remove"), Type.Literal("attach")]),
			source: Type.Optional(Type.String({ description: "create: signal source — process:<procId>[:stdout|stderr|both] (stream defaults to both) or file:<absolute-remote-path>" })),
			pattern: Type.Optional(Type.String({ description: "create/update: regex matched per log line; named groups (?<x>…) become {x} template/ETA vars" })),
			repeat: Type.Optional(Type.Boolean({ description: "create/update: fire on every match (default false: one-shot). Ignored unless notify=every-match." })),
			name: Type.Optional(Type.String({ description: "create/update: friendly label for the monitor" })),
			notify: Type.Optional(Type.String({ description: "create/update: notify policy — every-match | every-n:N | throttle:DUR | digest:DUR | milestone:f1,f2,… (DUR like 90s/5m/1h)" })),
			template: Type.Optional(Type.String({ description: "create/update: notification body template; {token} substitutes named captures + {count}/{matchCount}/{line}/{total}/{pct}/{eta}" })),
			notifyWhen: Type.Optional(Type.String({ description: "create/update: safe predicate gating a fire — probe: REQUIRED (over value/exitCode), process/file: optional filter over captures. e.g. value==0, loss>10, value==0 && exitCode==0" })),
			intervalMs: Type.Optional(Type.Number({ description: "probe create: how often (ms) to run the probe command (>=1000; rounded up to the ~3s scheduler tick)" })),
			consecutive: Type.Optional(Type.Number({ description: "probe create: fire only after N consecutive notifyWhen-true samples (default 1; debounces e.g. GPU idle)" })),
			expectEveryMs: Type.Optional(Type.Number({ description: "create/update: silence/stall window (ms) — fire once if no match arrives within it (catches hangs); update with 0 to clear" })),
			id: Type.Optional(Type.String({ description: "monitor id (mon_…) for update/pause/resume/remove" })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const a = str(args?.action) || "?";
			let rest: string;
			if (a === "create") {
				const src = str(args?.source);
				const pat = str(args?.pattern);
				rest = `${theme.fg("accent", "create")}${src ? ` ${theme.fg("muted", src)}` : ""}${pat ? ` ${theme.fg("muted", `/${pat}/`)}` : ""}`;
			} else if (a === "update" || a === "pause" || a === "resume" || a === "remove") {
				rest = `${theme.fg("accent", a)}${args?.id ? ` ${theme.fg("muted", str(args.id))}` : ""}`;
			} else {
				rest = theme.fg("accent", a);
			}
			return sshTitle("monitor", rest, theme, context);
		},
		async execute(_id, params: {
			action: "create" | "list" | "update" | "pause" | "resume" | "remove" | "attach";
			source?: string;
			pattern?: string;
			repeat?: boolean;
			name?: string;
			notify?: string;
			template?: string;
			notifyWhen?: string;
			intervalMs?: number;
			consecutive?: number;
			expectEveryMs?: number;
			id?: string;
		}) {
			const t = requireTarget();

			if (params.action === "create") {
				if (!params.source?.trim()) throw new Error("ssh_monitor create requires source (e.g. process:<procId>:stderr, file:/var/log/app.log, or probe:<command>)");
				// parseSource enforces probe intervalMs; create() enforces pattern (process/file)
				// vs notifyWhen (probe), so don't pre-require pattern here.
				const source = parseSource(params.source, { intervalMs: params.intervalMs, consecutive: params.consecutive });
				const notify = parseNotifyPolicy(params.notify);
				const m = await monitors.create(t, { source, pattern: params.pattern, repeat: params.repeat, name: params.name, notify, template: params.template, notifyWhen: params.notifyWhen, expectEveryMs: params.expectEveryMs });
				const gating = source.kind === "probe" ? `notifyWhen ${m.notifyWhen}` : `matching /${m.pattern}/${m.notifyWhen ? ` when ${m.notifyWhen}` : ""}`;
				return {
					content: [{ type: "text" as const, text: `Created monitor ${m.id} on ${params.source} ${gating} (notify: ${params.notify?.trim() || "every-match"}). Will notify when it fires.` }],
					details: { id: m.id, source: params.source, pattern: m.pattern, repeat: m.repeat, notify: params.notify?.trim() || "every-match", notifyWhen: m.notifyWhen },
				};
			}

			if (params.action === "list") {
				return { content: [{ type: "text" as const, text: formatRows(monitors.list()) }], details: { count: monitors.list().length } };
			}

			if (params.action === "attach") {
				// Re-scan the monitor store + running jobs' logWatches and re-arm anything
				// not currently live (e.g. after a manual remove, or to force re-sync).
				await monitors.rehydrate(t);
				return { content: [{ type: "text" as const, text: `Re-armed monitors from the remote store.\n${formatRows(monitors.list())}` }], details: { count: monitors.list().length } };
			}

			if (!params.id?.trim()) throw new Error(`ssh_monitor ${params.action} requires id (use ssh_monitor list)`);

			if (params.action === "update") {
				if (params.pattern === undefined && params.repeat === undefined && params.name === undefined && params.notify === undefined && params.template === undefined && params.notifyWhen === undefined && params.expectEveryMs === undefined) {
					throw new Error("ssh_monitor update requires at least one of pattern, repeat, name, notify, template, notifyWhen, expectEveryMs");
				}
				const notify = params.notify === undefined ? undefined : parseNotifyPolicy(params.notify);
				const m = await monitors.update(params.id, { pattern: params.pattern, repeat: params.repeat, name: params.name, notify, template: params.template, notifyWhen: params.notifyWhen, expectEveryMs: params.expectEveryMs });
				return { content: [{ type: "text" as const, text: `Updated monitor ${m.id}: /${m.pattern}/.` }], details: { id: m.id, pattern: m.pattern, repeat: m.repeat } };
			}

			if (params.action === "pause") {
				await monitors.pause(params.id);
				return { content: [{ type: "text" as const, text: `Paused monitor ${params.id}.` }], details: { id: params.id } };
			}

			if (params.action === "resume") {
				await monitors.resume(params.id);
				return { content: [{ type: "text" as const, text: `Resumed monitor ${params.id}.` }], details: { id: params.id } };
			}

			// remove
			await monitors.remove(params.id);
			return { content: [{ type: "text" as const, text: `Removed monitor ${params.id}.` }], details: { id: params.id } };
		},
	});
}
