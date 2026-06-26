// ---------------------------------------------------------------------------
// rsync transfer core (used by ssh_push, ssh_pull, and ssh_sync)
// ---------------------------------------------------------------------------

import { spawn } from "node:child_process";
import type { AgentToolUpdateCallback } from "@earendil-works/pi-coding-agent";
import type { SshTarget } from "./types";
import { shQuote } from "./utils";
import { baseSshOptions } from "./ssh/transport";

export async function runLocalProcess(
	localCwd: string,
	command: string,
	args: string[],
	signal?: AbortSignal,
	onData?: (chunk: Buffer) => void,
): Promise<{ code: number | null; stdout: Buffer; stderr: Buffer }> {
	return new Promise((resolve, reject) => {
		if (signal?.aborted) {
			reject(new Error("aborted"));
			return;
		}
		const child = spawn(command, args, { cwd: localCwd, stdio: ["ignore", "pipe", "pipe"] });
		const stdout: Buffer[] = [];
		const stderr: Buffer[] = [];
		child.stdout.on("data", (d) => {
			stdout.push(d);
			onData?.(d);
		});
		child.stderr.on("data", (d) => {
			stderr.push(d);
			onData?.(d);
		});
		const onAbort = () => child.kill();
		signal?.addEventListener("abort", onAbort, { once: true });
		child.on("error", (e) => {
			signal?.removeEventListener("abort", onAbort);
			reject(e);
		});
		child.on("close", (code) => {
			signal?.removeEventListener("abort", onAbort);
			resolve({ code, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr) });
		});
	});
}

export function rsyncSshCommand(t: SshTarget): string {
	return ["ssh", ...t.sshOptions, ...baseSshOptions(t.socket)].map(shQuote).join(" ");
}

// rsync >= 3.1 supports --info=progress2 (single clean progress line); the
// rsync Apple ships by default (2.6.9) rejects it with a cryptic usage dump.
// Probe once and fall back to --progress so push/pull work out of the box on
// stock macOS. Cached for the session.
let rsyncProgressFlagCache: string | null = null;
export async function rsyncProgressFlag(localCwd: string): Promise<string> {
	if (rsyncProgressFlagCache) return rsyncProgressFlagCache;
	try {
		const r = await runLocalProcess(localCwd, "rsync", ["--version"]);
		const m = r.stdout.toString().match(/rsync\s+version\s+(\d+)\.(\d+)/i);
		const major = m ? Number.parseInt(m[1], 10) : 0;
		const minor = m ? Number.parseInt(m[2], 10) : 0;
		const modern = major > 3 || (major === 3 && minor >= 1);
		rsyncProgressFlagCache = modern ? "--info=progress2" : "--progress";
	} catch (e) {
		if ((e as NodeJS.ErrnoException)?.code === "ENOENT") {
			throw new Error("rsync not found on this machine. Install it (macOS: `brew install rsync`) to use ssh_push/ssh_pull/ssh_sync.");
		}
		rsyncProgressFlagCache = "--progress"; // unknown version: assume old/safe
	}
	return rsyncProgressFlagCache;
}

export function ensureTrailingSlash(path: string): string {
	return path.endsWith("/") ? path : `${path}/`;
}

// Stream rsync output to the tool result view as it arrives so large transfers
// are not a black box. rsync --info=progress2 uses \r to rewrite the progress
// line; we keep the running buffer and push it on each chunk.
export function rsyncStreamer(onUpdate?: AgentToolUpdateCallback): ((chunk: Buffer) => void) | undefined {
	if (!onUpdate) return undefined;
	// Keep only a trailing window so streaming a huge transfer (one itemized
	// line per file) does not grow unbounded or churn O(n^2) to the UI.
	const MAX = 8 * 1024;
	let acc = "";
	return (chunk: Buffer) => {
		acc += chunk.toString();
		if (acc.length > MAX) acc = `…${acc.slice(-MAX)}`;
		onUpdate({ content: [{ type: "text", text: acc }], details: undefined });
	};
}

// Shared rsync push/pull core (used by ssh_push, ssh_pull, and ssh_sync).
export async function runRsyncTransfer(
	localCwd: string,
	t: SshTarget,
	source: string,
	dest: string,
	opts: { delete?: boolean; dryRun?: boolean; excludes?: string[]; gitignore?: boolean; quiet?: boolean; verbose?: boolean },
	signal?: AbortSignal,
	onData?: (chunk: Buffer) => void,
): Promise<{ stdout: string; stderr: string; elapsedMs: number }> {
	// quiet   -> no output (ssh_sync background runs)
	// verbose -> per-file itemized list + progress + full stats (opt-in)
	// default -> stats only, condensed to one line by the caller (summarizeRsync)
	const progress = opts.quiet
		? []
		: opts.verbose
			? [await rsyncProgressFlag(localCwd), "--itemize-changes", "--human-readable", "--stats"]
			: ["--human-readable", "--stats"];
	const args = ["-az", ...progress];
	if (opts.gitignore) args.push("--filter=:- .gitignore", "--exclude", ".git/");
	args.push("-e", rsyncSshCommand(t));
	for (const exclude of opts.excludes ?? []) args.push("--exclude", exclude);
	if (opts.delete) args.push("--delete");
	if (opts.dryRun) args.push("--dry-run");
	args.push(source, dest);
	const started = Date.now();
	const r = await runLocalProcess(localCwd, "rsync", args, signal, onData);
	if (r.code !== 0) throw new Error(`rsync failed (${r.code ?? "unknown"}): ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
	return { stdout: r.stdout.toString().trim(), stderr: r.stderr.toString().trim(), elapsedMs: Date.now() - started };
}
