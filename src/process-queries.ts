// ---------------------------------------------------------------------------
// Remote process registry queries (used by ssh_process AND the /ssh dashboard)
// ---------------------------------------------------------------------------
// All functions take an explicit SshTarget; nothing here depends on the active
// connection, so both the tool and the dashboard share one source of truth.

import type { SshTarget } from "./types";
import { buildEnvExports, shQuote, toRemotePath } from "./utils";
import { runRemoteCommand, sshFailureMessage } from "./ssh/transport";

export function processRoot(t: SshTarget): string {
	return `${t.remoteCwd}/.pi-ssh-processes`;
}

export interface ProcRow {
	id: string;
	name: string;
	status: "running" | "exited";
	code: number | null;
	pid: string;
	/** Epoch ms decoded from the id prefix (base36 Date.now() at start), or null. */
	startedMs: number | null;
}

// Single source of truth for the process listing. The structured parse below and
// the ssh_process `list` text output both derive from this one command.
export function buildProcessListCommand(root: string): string {
	return `root=${shQuote(root)}; if [ ! -d "$root" ]; then echo 'No remote processes.'; exit 0; fi; found=0; for d in "$root"/*; do [ -d "$d" ] || continue; found=1; id=$(basename "$d"); pid=$(cat "$d/pid" 2>/dev/null || true); name=$(cat "$d/name" 2>/dev/null || true); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then status=running; code='-'; else status=exited; code=$(cat "$d/exit_code" 2>/dev/null || true); [ -n "$code" ] || code='?'; fi; printf '%s\t%s\texit=%s\tpid=%s\t%s\n' "$id" "$status" "$code" "$pid" "$name"; done; [ "$found" -eq 0 ] && echo 'No remote processes.' || true`;
}

export function parseProcRows(text: string): ProcRow[] {
	const rows: ProcRow[] = [];
	for (const line of text.split("\n")) {
		if (!line.trim() || line.trim() === "No remote processes.") continue;
		const parts = line.split("\t");
		if (parts.length < 5) continue;
		const [id, statusRaw, exitField, pidField] = parts;
		const name = parts.slice(4).join("\t");
		const status = statusRaw === "running" ? "running" : "exited";
		const codeStr = exitField.startsWith("exit=") ? exitField.slice(5) : "";
		const code = status === "running" || codeStr === "?" || codeStr === "-" || codeStr === "" ? null : Number.parseInt(codeStr, 10);
		const pid = pidField.startsWith("pid=") ? pidField.slice(4) : pidField;
		const base = id.split("-")[0];
		const ms = Number.parseInt(base, 36);
		rows.push({ id, name, status, code: Number.isNaN(code as number) ? null : code, pid, startedMs: Number.isNaN(ms) ? null : ms });
	}
	return rows;
}

// Reproduce the exact ssh_process `list` text from structured rows, byte-identical
// to the legacy inline command output (so the agent-facing tool is unchanged).
export function formatProcRows(rows: ProcRow[]): string {
	return rows
		.map((r) => {
			const code = r.status === "running" ? "-" : r.code === null ? "?" : String(r.code);
			return `${r.id}\t${r.status}\texit=${code}\tpid=${r.pid}\t${r.name}`;
		})
		.join("\n");
}

export async function listProcesses(t: SshTarget, signal?: AbortSignal): Promise<ProcRow[]> {
	const r = await runRemoteCommand(t, buildProcessListCommand(processRoot(t)), { signal });
	if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
	return parseProcRows(r.stdout.toString());
}

export interface ProcessRefResolution {
	id: string;
	row?: ProcRow;
	matchedBy: "id" | "name";
}

export function resolveProcessRef(
	rows: ProcRow[],
	ref: { id?: string; name?: string },
	opts?: { preferRunning?: boolean },
): ProcessRefResolution {
	const id = ref.id?.trim();
	if (id) return { id, matchedBy: "id" };
	const name = ref.name?.trim();
	if (!name) throw new Error("ssh_process requires id or name for this action");
	const matches = rows.filter((r) => r.name === name);
	if (!matches.length) throw new Error(`No remote process named "${name}". Use ssh_process list to see available jobs.`);
	matches.sort((a, b) => {
		if (opts?.preferRunning && a.status !== b.status) return a.status === "running" ? -1 : 1;
		const byStart = (b.startedMs ?? 0) - (a.startedMs ?? 0);
		return byStart || b.id.localeCompare(a.id);
	});
	return { id: matches[0].id, row: matches[0], matchedBy: "name" };
}

export function buildKillCommand(dir: string): string {
	return `d=${shQuote(dir)}; test -d "$d" || { echo 'process not found' >&2; exit 2; }; pid=$(cat "$d/pid" 2>/dev/null || true); test -n "$pid" || { echo 'pid not found' >&2; exit 2; }; kill "$pid" 2>/dev/null || true; sleep 1; kill -9 "$pid" 2>/dev/null || true; printf 'killed %s\n' "$pid"`;
}

export function buildClearCommand(root: string): string {
	return `root=${shQuote(root)}; if [ ! -d "$root" ]; then echo 'No remote processes.'; exit 0; fi; removed=0; kept=0; for d in "$root"/*; do [ -d "$d" ] || continue; pid=$(cat "$d/pid" 2>/dev/null || true); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then kept=$((kept+1)); continue; fi; rm -rf "$d" && removed=$((removed+1)); done; rmdir "$root" 2>/dev/null || true; printf 'Cleared %s finished process(es); %s still running.\n' "$removed" "$kept"`;
}

export function processRunScript(
	t: SshTarget,
	params: {
		command: string;
		cwd?: string;
		env?: Record<string, string>;
		commandPrefix?: string;
	},
	localCwd: string,
): string {
	const cwd = params.cwd?.trim() ? toRemotePath(params.cwd, localCwd, t.remoteCwd) : t.remoteCwd;
	const lines = [
		"#!/usr/bin/env bash",
		// Resolve the process dir (where run.sh lives) so we can record the exit code there.
		`__pi_dir="$(cd "$(dirname "\${BASH_SOURCE[0]}")" && pwd)"`,
		// Record the final exit status on ANY exit (normal end, explicit `exit N`,
		// or error under set -e). A trailing capture line would be skipped whenever
		// the command itself calls exit, so an EXIT trap is the robust choice.
		`trap '__pi_rc=$?; printf %s "$__pi_rc" > "$__pi_dir/exit_code"' EXIT`,
		// Record our own pid authoritatively. The launcher seeds pid with `echo $!`,
		// but that is the `setsid` pid which on some systems is a short-lived parent;
		// $$ here is the real job pid that list/output/kill rely on. Atomic mv avoids
		// a torn read against the launcher's seed write.
		`printf %s "$$" > "$__pi_dir/pid.tmp" && mv -f "$__pi_dir/pid.tmp" "$__pi_dir/pid"`,
		`cd -- ${shQuote(cwd)}`,
	];
	lines.push(...buildEnvExports({ ...t.defaultEnv, ...params.env }));
	if (t.defaultCommandPrefix?.trim()) lines.push(t.defaultCommandPrefix);
	if (params.commandPrefix?.trim()) lines.push(params.commandPrefix);
	lines.push(params.command);
	return `${lines.join("\n")}\n`;
}
