// ---------------------------------------------------------------------------
// Agent-facing SSH situational report
// ---------------------------------------------------------------------------
// Human users have the /ssh dashboard. This helper gives agents the same compact
// one-call picture without TUI colors: connection, processes, monitors, tunnels,
// and sync. Each section degrades independently so a transient remote query does
// not make the whole status unusable.

import type { SshContext } from "./context";
import type { MonitorRow } from "./monitor";
import { listProcesses, type ProcRow } from "./process-queries";
import type { SshTarget } from "./types";
import { formatDuration } from "./utils";

function shortId(id: string): string {
	return id.length > 12 ? id.slice(0, 12) : id;
}

function procRuntime(row: ProcRow, now: number): string {
	return row.startedMs ? formatDuration(Math.max(0, now - row.startedMs)) : "?";
}

export function formatProcessSection(rows: ProcRow[], now = Date.now()): string[] {
	if (!rows.length) return ["Processes: none"];
	const running = rows.filter((r) => r.status === "running").length;
	const finished = rows.length - running;
	const lines = [`Processes (${running} running, ${finished} finished):`];
	for (const r of rows) {
		const mark = r.status === "running" ? "▶" : r.code === 0 ? "✓" : r.code === null ? "?" : "✗";
		const status = r.status === "running" ? `running pid=${r.pid || "?"}` : `exit=${r.code === null ? "?" : r.code}`;
		lines.push(`  ${mark} ${r.name || r.id} (${shortId(r.id)}) ${status} age=${procRuntime(r, now)}`);
	}
	return lines;
}

export function formatMonitorSection(rows: MonitorRow[]): string[] {
	if (!rows.length) return ["Monitors: none"];
	const lines = [`Monitors (${rows.length}):`];
	for (const r of rows) {
		const state = r.paused ? "paused" : r.fired && !r.repeat ? "fired" : "active";
		const name = r.name ? ` ${r.name}` : "";
		const kind = r.kind === "legacy" ? " legacy" : "";
		const what = r.pattern ? `/${r.pattern}/${r.notifyWhen ? ` when ${r.notifyWhen}` : ""}` : `notifyWhen ${r.notifyWhen ?? ""}`;
		lines.push(`  ${r.id}${name}${kind} ${state} ${r.source} ${what} ${r.notify} matches=${r.matchCount}`);
	}
	return lines;
}

export function formatTunnelSection(rows: Array<{ localPort: number; remoteHost: string; remotePort: number; saved?: boolean }>): string[] {
	if (!rows.length) return ["Tunnels: none"];
	return [`Tunnels (${rows.length}):`, ...rows.map((t) => `  localhost:${t.localPort} -> ${t.remoteHost}:${t.remotePort}${t.saved ? " [saved]" : ""}`)];
}

export function formatSyncSection(state: ReturnType<SshContext["sync"]["getState"]>): string[] {
	if (!state) return ["Sync: not running"];
	return [`Sync: watching ${state.localSource} -> ${state.remote}:${state.remoteDest} (${state.count} syncs)`];
}

export async function formatSshSitrep(ctx: SshContext, t: SshTarget): Promise<string> {
	const lines: string[] = [ctx.connectedText(t)];
	try {
		const profiles = ctx.profileNames();
		if (profiles.length) lines.push(`Saved profiles: ${profiles.map((n) => `@${n}`).join(", ")}`);
	} catch (e) {
		lines.push(`Saved profiles: ⚠️ ${e instanceof Error ? e.message : String(e)}`);
	}

	lines.push("");
	try {
		lines.push(...formatProcessSection(await listProcesses(t)));
	} catch (e) {
		lines.push(`Processes: ⚠️ ${e instanceof Error ? e.message : String(e)}`);
	}

	try {
		lines.push(...formatMonitorSection(ctx.monitors.list()));
	} catch (e) {
		lines.push(`Monitors: ⚠️ ${e instanceof Error ? e.message : String(e)}`);
	}

	try {
		lines.push(...formatTunnelSection(ctx.tunnels.list()));
	} catch (e) {
		lines.push(`Tunnels: ⚠️ ${e instanceof Error ? e.message : String(e)}`);
	}

	try {
		lines.push(...formatSyncSection(ctx.sync.getState()));
	} catch (e) {
		lines.push(`Sync: ⚠️ ${e instanceof Error ? e.message : String(e)}`);
	}

	return lines.join("\n");
}
