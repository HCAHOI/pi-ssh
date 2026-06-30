// ---------------------------------------------------------------------------
// ssh_sync: debounced rsync of the local workspace to the remote on change
// ---------------------------------------------------------------------------

import { type FSWatcher, watch } from "node:fs";
import { resolve } from "node:path";
import { Type } from "typebox";
import type { SshTarget } from "./types";
import type { SshContext } from "./context";
import { toRemotePath } from "./utils";
import { ensureTrailingSlash, runRsyncTransfer } from "./transfer";

interface SyncState {
	watcher: FSWatcher;
	localSource: string;
	remoteDest: string;
	remote: string;
	debounceMs: number;
	delete: boolean;
	excludes?: string[];
	timer: NodeJS.Timeout | null;
	syncing: boolean;
	pending: boolean;
	count: number;
}

export interface SyncManager {
	stop(): void;
	startSync(
		t: SshTarget,
		params: { localPath?: string; remotePath?: string; debounceMs?: number; delete?: boolean; excludes?: string[] },
	): Promise<{ localSource: string; remoteDest: string; debounceMs: number; initialTail: string }>;
	getState(): { localSource: string; remote: string; remoteDest: string; count: number } | null;
}

/** Create the sync manager and register the ssh_sync tool. */
export function createSyncManager(ctx: SshContext): SyncManager {
	const { pi, localCwd, requireTarget, emit, render } = ctx;
	const { str, sshTitle } = render;
	let syncState: SyncState | null = null;

	function stop(): void {
		if (!syncState) return;
		if (syncState.timer) clearTimeout(syncState.timer);
		try { syncState.watcher.close(); } catch { /* already closed */ }
		syncState = null;
	}

	async function runSync(s: SyncState): Promise<void> {
		const target = ctx.getTarget();
		if (!target || target.remote !== s.remote) { stop(); return; }
		if (s.syncing) { s.pending = true; return; }
		s.syncing = true;
		try {
			await runRsyncTransfer(localCwd, target, s.localSource, `${s.remote}:${s.remoteDest}`, { delete: s.delete, excludes: s.excludes, gitignore: true, quiet: true });
			s.count += 1;
		} catch (e) {
			emit(`⚠️ ssh_sync failed: ${e instanceof Error ? e.message : String(e)}`, { kind: "sync-error" });
		} finally {
			s.syncing = false;
			if (s.pending) { s.pending = false; scheduleSync(s); }
		}
	}

	function scheduleSync(s: SyncState): void {
		if (s.timer) clearTimeout(s.timer);
		s.timer = setTimeout(() => { s.timer = null; void runSync(s); }, s.debounceMs);
		s.timer.unref?.();
	}

	// Start (or restart) the auto-sync watcher. Shared by the ssh_sync tool and the
	// /ssh dashboard's sync toggle so there is one copy of the rsync+watch setup.
	async function startSync(
		t: SshTarget,
		params: { localPath?: string; remotePath?: string; debounceMs?: number; delete?: boolean; excludes?: string[] },
	): Promise<{ localSource: string; remoteDest: string; debounceMs: number; initialTail: string }> {
		stop();
		const localSource = ensureTrailingSlash(resolve(localCwd, params.localPath ?? "."));
		const remoteDest = ensureTrailingSlash(params.remotePath ? toRemotePath(params.remotePath, localCwd, t.remoteCwd) : t.remoteCwd);
		// Initial full sync so the remote starts in lockstep.
		const initial = await runRsyncTransfer(localCwd, t, localSource, `${t.remote}:${remoteDest}`, { delete: params.delete, excludes: params.excludes, gitignore: true }, undefined);
		const debounceMs = Math.max(50, Math.floor(params.debounceMs ?? 400));
		let watcher: FSWatcher;
		try {
			watcher = watch(resolve(localCwd, params.localPath ?? "."), { recursive: true });
		} catch (e) {
			throw new Error(`ssh_sync could not watch ${localSource}: ${e instanceof Error ? e.message : String(e)} (recursive fs.watch requires macOS/Windows; on Linux use ssh_push)`);
		}
		const s: SyncState = { watcher, localSource, remoteDest, remote: t.remote, debounceMs, delete: params.delete ?? false, excludes: params.excludes, timer: null, syncing: false, pending: false, count: 0 };
		watcher.on("change", (_event, file) => {
			// Cheap noise filter: skip VCS/build churn that rsync would exclude anyway.
			const name = typeof file === "string" ? file : file?.toString() ?? "";
			if (/(^|\/)\.git\/|(^|\/)node_modules\/|~$|\.swp$/.test(name)) return;
			scheduleSync(s);
		});
		watcher.on("error", (e) => emit(`⚠️ ssh_sync watcher error: ${e instanceof Error ? e.message : String(e)}`, { kind: "sync-error" }));
		syncState = s;
		return { localSource, remoteDest, debounceMs, initialTail: initial.stdout.split("\n").slice(-3).join("\n") };
	}

	pi.registerTool({
		name: "ssh_sync",
		label: "ssh_sync",
		description: "Auto-sync local files to the active SSH remote on change via debounced rsync. Actions: start | stop | status.",
		promptSnippet: "Continuously rsync local edits to the remote on change",
		promptGuidelines: ["Use ssh_sync start for edit-locally/run-remotely loops; stop it when done."],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("start"), Type.Literal("stop"), Type.Literal("status")]),
			localPath: Type.Optional(Type.String({ description: "Local path to watch+sync, defaults to current workspace" })),
			remotePath: Type.Optional(Type.String({ description: "Remote destination path, defaults to remote cwd" })),
			debounceMs: Type.Optional(Type.Number({ description: "Quiet period after a change before syncing (default 400)" })),
			delete: Type.Optional(Type.Boolean({ description: "Mirror local deletions to the remote (default false)" })),
			excludes: Type.Optional(Type.Array(Type.String(), { description: "Additional rsync exclude patterns" })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const a = str(args?.action) || "?";
			const where = a === "start" ? ` ${theme.fg("muted", str(args?.localPath) || ".")}` : "";
			return sshTitle("sync", `${theme.fg("accent", a)}${where}`, theme, context);
		},
		async execute(_id, params: { action: "start" | "stop" | "status"; localPath?: string; remotePath?: string; debounceMs?: number; delete?: boolean; excludes?: string[] }) {
			const t = requireTarget();
			if (params.action === "status") {
				if (!syncState) return { content: [{ type: "text" as const, text: "ssh_sync: not running." }] };
				return { content: [{ type: "text" as const, text: `ssh_sync: watching ${syncState.localSource} -> ${syncState.remote}:${syncState.remoteDest} (${syncState.count} syncs so far)` }] };
			}
			if (params.action === "stop") {
				const was = syncState ? `${syncState.count}` : null;
				stop();
				return { content: [{ type: "text" as const, text: was === null ? "ssh_sync was not running." : `ssh_sync stopped (${was} syncs).` }] };
			}
			// start
			const { localSource, remoteDest, debounceMs, initialTail } = await startSync(t, params);
			return { content: [{ type: "text" as const, text: `ssh_sync started: ${localSource} -> ${t.remote}:${remoteDest} (debounce ${debounceMs}ms).\n${initialTail}` }] };
		},
	});

	return {
		stop,
		startSync,
		getState: () =>
			syncState ? { localSource: syncState.localSource, remote: syncState.remote, remoteDest: syncState.remoteDest, count: syncState.count } : null,
	};
}
