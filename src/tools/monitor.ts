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
			"Advanced runtime monitors for SSH jobs/files/probes. Supports create/list/update/pause/resume/remove/attach, notification policies, predicates, and stall detection. Full examples: /ssh help monitor.",
		promptSnippet: "Manage runtime log monitors on the SSH remote",
		promptGuidelines: [
			"Use ssh_monitor for advanced runtime watches (file/probe/stall/digest/milestone). For simple process completion or one-shot log matches, prefer ssh_process. See /ssh help monitor for examples.",
		],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("create"), Type.Literal("list"), Type.Literal("update"), Type.Literal("pause"), Type.Literal("resume"), Type.Literal("remove"), Type.Literal("attach")]),
			source: Type.Optional(Type.String({ description: "create: process:<id>[:stream], file:/abs/path, or probe:<command>" })),
			pattern: Type.Optional(Type.String({ description: "create/update: regex to match" })),
			repeat: Type.Optional(Type.Boolean({ description: "Allow repeated fires" })),
			name: Type.Optional(Type.String({ description: "Friendly label" })),
			notify: Type.Optional(Type.String({ description: "Policy: every-match | every-n:N | throttle:DUR | digest:DUR | milestone:f1,f2,…" })),
			template: Type.Optional(Type.String({ description: "Notification template using named captures" })),
			notifyWhen: Type.Optional(Type.String({ description: "Predicate over captures/value/exitCode/matchCount/elapsedMs" })),
			intervalMs: Type.Optional(Type.Number({ description: "probe interval in ms" })),
			consecutive: Type.Optional(Type.Number({ description: "probe debounce count" })),
			expectEveryMs: Type.Optional(Type.Number({ description: "stall window in ms; 0 clears" })),
			id: Type.Optional(Type.String({ description: "monitor id" })),
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
					details: { id: m.id, source: params.source, pattern: m.pattern, repeat: m.repeat, notify: params.notify?.trim() || "every-match", notifyWhen: m.notifyWhen } as unknown,
				};
			}

			if (params.action === "list") {
				return { content: [{ type: "text" as const, text: formatRows(monitors.list()) }], details: { count: monitors.list().length } as unknown };
			}

			if (params.action === "attach") {
				// Re-scan the monitor store + running jobs' logWatches and re-arm anything
				// not currently live (e.g. after a manual remove, or to force re-sync).
				await monitors.rehydrate(t);
				return { content: [{ type: "text" as const, text: `Re-armed monitors from the remote store.\n${formatRows(monitors.list())}` }], details: { count: monitors.list().length } as unknown };
			}

			if (!params.id?.trim()) throw new Error(`ssh_monitor ${params.action} requires id (use ssh_monitor list)`);

			if (params.action === "update") {
				if (params.pattern === undefined && params.repeat === undefined && params.name === undefined && params.notify === undefined && params.template === undefined && params.notifyWhen === undefined && params.expectEveryMs === undefined) {
					throw new Error("ssh_monitor update requires at least one of pattern, repeat, name, notify, template, notifyWhen, expectEveryMs");
				}
				const notify = params.notify === undefined ? undefined : parseNotifyPolicy(params.notify);
				const m = await monitors.update(params.id, { pattern: params.pattern, repeat: params.repeat, name: params.name, notify, template: params.template, notifyWhen: params.notifyWhen, expectEveryMs: params.expectEveryMs });
				return { content: [{ type: "text" as const, text: `Updated monitor ${m.id}: /${m.pattern}/.` }], details: { id: m.id, pattern: m.pattern, repeat: m.repeat } as unknown };
			}

			if (params.action === "pause") {
				await monitors.pause(params.id);
				return { content: [{ type: "text" as const, text: `Paused monitor ${params.id}.` }], details: { id: params.id } as unknown };
			}

			if (params.action === "resume") {
				await monitors.resume(params.id);
				return { content: [{ type: "text" as const, text: `Resumed monitor ${params.id}.` }], details: { id: params.id } as unknown };
			}

			// remove
			await monitors.remove(params.id);
			return { content: [{ type: "text" as const, text: `Removed monitor ${params.id}.` }], details: { id: params.id } as unknown };
		},
	});
}
