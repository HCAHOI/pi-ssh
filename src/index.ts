/**
 * SSH Remote Execution Extension (enhanced)
 *
 * Adds explicit ssh_read/ssh_write/ssh_edit/ssh_bash tools for remote
 * operations via SSH. Local read/write/edit/bash tools remain local, so the
 * common workflow of editing locally and testing remotely stays unambiguous.
 *
 * Connect at runtime:
 *   /ssh user@host                                      # use remote pwd as cwd
 *   /ssh user@host:/remote/path                         # explicit remote cwd
 *   /ssh -i /path/to/key.pem root@host                  # identity file / ssh options
 *   /ssh -i key root@host:/path --activate 'source .venv/bin/activate'
 *   /ssh root@host --env PYTHONPATH=/src --env CUDA_VISIBLE_DEVICES=0
 *   /ssh off                                           # disconnect
 *   /ssh                                               # show current status
 *
 * --activate <cmd> and --env KEY=VALUE (repeatable) attach a persistent shell
 * prefix / environment that is applied to EVERY ssh_bash and ssh_process run,
 * so you do not have to re-source a venv or re-export vars on each call.
 *
 * Agents can also call ssh_connect/ssh_disconnect/ssh_status directly.
 *
 * Or at startup:
 *   pi -e ./ssh/index.ts --ssh "-i /path/to/key.pem root@host[:/path]"
 *
 * Enhancements over the bundled example:
 *   1. SSH connection reuse via OpenSSH ControlMaster multiplexing — all
 *      ssh invocations share one persistent master connection, so no
 *      repeated auth/TCP handshake per tool call.
 *   2. Real remote in-place edit — oldText/newText are shipped to the remote
 *      and applied by python3 there; only the diff comes back. Falls back to
 *      read-rewrite-write when python3 is unavailable on the remote.
 *   3. Persistent per-connection activation/env (--activate / --env) applied
 *      to ssh_bash and ssh_process so venv/env setup is not repeated.
 *   4. Background remote processes (ssh_process) capture their exit code and
 *      support a clear action to prune finished jobs.
 *
 * Requirements:
 *   - SSH key-based auth (BatchMode=yes; no password prompts)
 *   - bash on remote; python3 on remote for efficient in-place edit
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Key, matchesKey, type SelectItem, SelectList, truncateToWidth } from "@earendil-works/pi-tui";

import type { Activation, SshTarget } from "./types";
import { buildEnvExports, formatDuration, shQuote, toRemotePath } from "./utils";
import { createRemoteBashOps } from "./remote-ops";
import { setReconnectNotifier } from "./ssh/reconnect";
import { closeMaster, runRemoteCommand, sshFailureMessage } from "./ssh/transport";
import { resolveTarget } from "./ssh/target";
import { sendProcessMessage } from "./notify";
import { createRender } from "./render";
import type { SshContext } from "./context";
import { createTunnelManager, type TunnelManager } from "./tunnels";
import { createSyncManager, type SyncManager } from "./sync";
import { setupConnectionTools } from "./tools/connection";
import { setupFsTools } from "./tools/fs";
import { setupBashTool } from "./tools/bash";
import { setupProcessTool } from "./tools/process";
import { setupTransferTools } from "./tools/transfer";
import { createPollerManager } from "./poller";
import {
	buildClearCommand,
	buildKillCommand,
	listProcesses,
	processRoot,
	type ProcRow,
} from "./process-queries";


// ---------------------------------------------------------------------------
// Extension
// ---------------------------------------------------------------------------

export default function (pi: ExtensionAPI) {
	pi.registerFlag("ssh", { description: "SSH remote, e.g. user@host[:/path], -i key user@host, optionally with --activate <cmd> / --env K=V", type: "string" });

	const localCwd = process.cwd();

	// Background ssh_process notification poller (owns pollers + latestStartByName).
	const poller = createPollerManager(pi);
	// Agent-facing notification sink shared by the poller and the sync watcher.
	const emit = (content: string, details: Record<string, unknown>): void => sendProcessMessage(pi, content, details);

	let target: SshTarget | null = null;
	const get = () => target;

	function statusLabel(t: SshTarget | null): string {
		if (!t) return "";
		const act = t.defaultCommandPrefix ? ` ⚡${t.defaultCommandPrefix.length > 28 ? `${t.defaultCommandPrefix.slice(0, 27)}…` : t.defaultCommandPrefix}` : "";
		return `SSH: ${t.remote}:${t.remoteCwd}${t.hasPython ? "" : " (no python3)"}${act}`;
	}

	// Last seen ui handle, captured from any ctx, so the connection-level widget
	// poller can update the footer widget without a live command/tool ctx.
	let uiRef: { setStatus: (k: string, v?: string) => void; setWidget: (k: string, v?: unknown, o?: unknown) => void; notify: (msg: string, type?: "info" | "warning" | "error") => void; theme: { fg: (c: string, s: string) => string } } | null = null;
	let widgetTimer: NodeJS.Timeout | null = null;
	const WIDGET_POLL_MS = 5000;

	function stopWidgetPoller(): void {
		if (widgetTimer) {
			clearInterval(widgetTimer);
			widgetTimer = null;
		}
		uiRef?.setWidget("ssh-procs", undefined);
	}

	function startWidgetPoller(): void {
		if (widgetTimer || !uiRef) return;
		let busy = false;
		const tickWidget = async () => {
			if (busy || !target || !uiRef) return;
			busy = true;
			try {
				const rows = await listProcesses(target);
				const running = rows.filter((r) => r.status === "running").length;
				uiRef.setWidget("ssh-procs", running > 0 ? [uiRef.theme.fg("accent", `ssh: ${running} running`)] : undefined);
			} catch {
				/* transient: keep the last widget value, retry next tick */
			} finally {
				busy = false;
			}
		};
		widgetTimer = setInterval(() => void tickWidget(), WIDGET_POLL_MS);
		widgetTimer.unref?.();
		void tickWidget();
	}

	function refreshStatus(ctx: any) {
		if (ctx?.ui) uiRef = ctx.ui;
		if (uiRef) {
			const label = statusLabel(target);
			uiRef.setStatus("ssh", label ? uiRef.theme.fg("accent", label) : "");
		}
		// Drive the running-process widget by connection state.
		if (target && uiRef) startWidgetPoller();
		else stopWidgetPoller();
	}

	// Surface backoff-reconnection progress in the status line; notify on the outcome.
	// Reads uiRef/target lazily at call time, so a single assignment stays current.
	setReconnectNotifier((phase, info) => {
		if (!uiRef) return;
		if (phase === "retrying") {
			uiRef.setStatus("ssh", uiRef.theme.fg("warning", `Reconnecting ${info.remote} \u2014 attempt ${info.attempt}/${info.max}, retry in ${Math.round(info.delayMs / 1000)}s\u2026`));
			return;
		}
		const label = statusLabel(target);
		uiRef.setStatus("ssh", label ? uiRef.theme.fg("accent", label) : "");
		if (phase === "recovered") uiRef.notify(`SSH reconnected: ${info.remote}`, "info");
		else uiRef.notify(`SSH reconnect to ${info.remote} failed after ${info.max} attempts`, "error");
	});

	function tokenizeSshArgs(input: string): string[] {
		const tokens: string[] = [];
		let current = "";
		let quote: "'" | '"' | undefined;
		let escaping = false;
		for (const ch of input) {
			if (escaping) {
				current += ch;
				escaping = false;
				continue;
			}
			if (ch === "\\" && quote !== "'") {
				escaping = true;
				continue;
			}
			if ((ch === "'" || ch === '"') && (!quote || quote === ch)) {
				quote = quote ? undefined : ch;
				continue;
			}
			if (!quote && /\s/.test(ch)) {
				if (current) {
					tokens.push(current);
					current = "";
				}
				continue;
			}
			current += ch;
		}
		if (quote) throw new Error("Unclosed quote in /ssh arguments");
		if (escaping) current += "\\";
		if (current) tokens.push(current);
		return tokens;
	}

	function parseConnectArg(arg: string): { remote: string; path?: string; sshOptions: string[]; activation: Activation } {
		const tokens = tokenizeSshArgs(arg);
		if (tokens[0] === "ssh") tokens.shift();
		if (tokens.length === 0) throw new Error("Missing SSH destination");

		// Extract our own --activate / --env flags before treating the rest as ssh
		// options + destination. Values may be attached (--env=K=V) or separate.
		let commandPrefix: string | undefined;
		const env: Record<string, string> = {};
		const rest: string[] = [];
		for (let i = 0; i < tokens.length; i++) {
			const tk = tokens[i];
			if (tk === "--activate") {
				commandPrefix = tokens[++i];
				if (commandPrefix === undefined) throw new Error("--activate requires a command, e.g. --activate 'source .venv/bin/activate'");
				continue;
			}
			if (tk.startsWith("--activate=")) {
				commandPrefix = tk.slice("--activate=".length);
				continue;
			}
			if (tk === "--env" || tk.startsWith("--env=")) {
				const kv = tk.startsWith("--env=") ? tk.slice("--env=".length) : tokens[++i];
				if (kv === undefined) throw new Error("--env requires KEY=VALUE");
				const eq = kv.indexOf("=");
				if (eq <= 0) throw new Error(`--env expects KEY=VALUE, got: ${kv}`);
				const key = kv.slice(0, eq);
				if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) throw new Error(`Invalid env var name: ${key}`);
				env[key] = kv.slice(eq + 1);
				continue;
			}
			rest.push(tk);
		}
		if (rest.length === 0) throw new Error("Missing SSH destination");
		const destination = rest[rest.length - 1];
		const sshOptions = rest.slice(0, -1);
		const activation: Activation = { commandPrefix, env: Object.keys(env).length ? env : undefined };
		const homePath = destination.match(/^(.+):(~(?:\/.*)?)$/);
		if (homePath) {
			throw new Error("SSH remote cwd must be an absolute path; use /ssh -i key user@host:/absolute/path");
		}
		const match = destination.match(/^(.+):(\/.*)$/);
		if (!match) {
			return { remote: destination, sshOptions, activation };
		}
		return { remote: match[1], path: match[2], sshOptions, activation };
	}

	// --- connection profiles (~/.pi/ssh-profiles.json) ---
	function profilesPath(): string {
		return join(homedir(), ".pi", "ssh-profiles.json");
	}

	function loadProfiles(): Record<string, string> {
		let raw: string;
		try {
			raw = readFileSync(profilesPath(), "utf8");
		} catch {
			return {}; // missing file is fine
		}
		// A corrupt file must NOT silently reset to {} (that would let `save`
		// clobber every existing entry). Surface it.
		let parsed: unknown;
		try {
			parsed = JSON.parse(raw);
		} catch (e) {
			throw new Error(`Corrupt SSH profiles file ${profilesPath()}: ${e instanceof Error ? e.message : String(e)}`);
		}
		return parsed && typeof parsed === "object" ? (parsed as Record<string, string>) : {};
	}

	function profileNames(): string[] {
		return Object.keys(loadProfiles()).sort();
	}

	// Expand a leading @name into the saved target string; trailing tokens override.
	function expandProfile(arg: string): string {
		const trimmed = arg.trim();
		if (!trimmed.startsWith("@")) return arg;
		const name = trimmed.slice(1).split(/\s+/)[0];
		if (!name) throw new Error("Missing profile name after @");
		const rest = trimmed.slice(1 + name.length).trim();
		const base = loadProfiles()[name];
		if (!base) throw new Error(`SSH profile not found: @${name} (define it in ${profilesPath()} or use /ssh save ${name})`);
		return rest ? `${base} ${rest}` : base;
	}

	function saveProfile(name: string): void {
		const t = requireTarget();
		if (!t.originArg) throw new Error("No connection string to save for the active SSH target");
		if (!/^[A-Za-z0-9._-]+$/.test(name)) throw new Error(`Invalid profile name: ${name}`);
		const profiles = loadProfiles();
		profiles[name] = t.originArg;
		mkdirSync(dirname(profilesPath()), { recursive: true });
		writeFileSync(profilesPath(), `${JSON.stringify(profiles, null, 2)}\n`);
	}

	async function connect(arg: string): Promise<SshTarget> {
		const expanded = expandProfile(arg);
		const { remote, path, sshOptions, activation } = parseConnectArg(expanded);
		const t = await resolveTarget(remote, path, sshOptions, activation);
		t.originArg = expanded.trim();
		return t;
	}

	async function switchTarget(arg: string): Promise<SshTarget> {
		const next = await connect(arg);
		const prev = target;
		// Reconnecting to the SAME host+cwd (same .pi-ssh-processes registry): keep the
		// in-memory pollers and just repoint them at the new connection, so a reconnect
		// never silently drops a still-pending completion / log-watch notification.
		if (prev && prev.remote === next.remote && prev.remoteCwd === next.remoteCwd) {
			poller.repointAll(next);
		} else {
			poller.stopAll();
		}
		if (prev) await closeMaster(prev);
		target = next;
		ctx.tunnels.stopAll();
		await poller.rehydrate(next);
		return next;
	}

	async function disconnect(): Promise<void> {
		poller.stopAll();
		ctx.sync.stop();
		if (target) {
			ctx.tunnels.stopAll();
			await closeMaster(target);
		}
		target = null;
	}


	function requireTarget(): SshTarget {
		if (!target) {
			throw new Error("SSH is not connected. Use /ssh [-i key] user@host or call ssh_connect first.");
		}
		return target;
	}

	function connectedText(t: SshTarget): string {
		const lines = [`SSH connected: ${t.remote}:${t.remoteCwd}${t.hasPython ? "" : " (no python3; ssh_edit uses fallback)"}`];
		if (t.defaultCommandPrefix) lines.push(`  activation (every ssh_bash/ssh_process): ${t.defaultCommandPrefix}`);
		if (t.defaultEnv && Object.keys(t.defaultEnv).length) lines.push(`  env: ${Object.keys(t.defaultEnv).join(", ")}`);
		return lines.join("\n");
	}

	function buildSshBashCommand(t: SshTarget, params: {
		command: string;
		cwd?: string;
		delaySeconds?: number;
		env?: Record<string, string>;
		commandPrefix?: string;
	}): string {
		// Order mirrors processRunScript: cd -> env -> activation -> per-call prefix -> command.
		const parts: string[] = [];
		if (params.cwd?.trim()) parts.push(`cd -- ${shQuote(toRemotePath(params.cwd, localCwd, t.remoteCwd))}`);
		parts.push(...buildEnvExports({ ...t.defaultEnv, ...params.env }));
		if (t.defaultCommandPrefix?.trim()) parts.push(t.defaultCommandPrefix);
		if (params.commandPrefix?.trim()) parts.push(params.commandPrefix);
		if (params.delaySeconds !== undefined) {
			if (!Number.isFinite(params.delaySeconds) || params.delaySeconds < 0) {
				throw new Error("ssh_bash delaySeconds must be a non-negative number");
			}
			if (params.delaySeconds > 0) parts.push(`sleep ${Math.floor(params.delaySeconds)}`);
		}
		parts.push(params.command);
		return parts.join("\n");
	}

	// Tool-call rendering helpers, bound to the active target getter.
	const render = createRender(get, localCwd);
	const { str, remoteDisplayPath, accentRemotePath, readLineRange, sshTitle, renderEditDiffResult } = render;

	// Shared context handed to every subsystem/tool module. Managers are attached
	// just below (they capture ctx lazily, so the forward reference is safe).
	const ctx: SshContext = {
		pi,
		localCwd,
		getTarget: get,
		requireTarget,
		poller,
		emit,
		render,
		connect,
		switchTarget,
		disconnect,
		refreshStatus,
		connectedText,
		statusLabel,
		profileNames,
		saveProfile,
		expandProfile,
		buildSshBashCommand,
		tunnels: undefined as unknown as TunnelManager,
		sync: undefined as unknown as SyncManager,
	};
	ctx.tunnels = createTunnelManager(ctx);
	ctx.sync = createSyncManager(ctx);

	// --- agent tools (registered through the shared context) ---
	setupConnectionTools(ctx);
	setupFsTools(ctx);
	setupBashTool(ctx);
	setupProcessTool(ctx);
	setupTransferTools(ctx);




	// --- startup flag ---
	pi.on("session_start", async (_event, ctx) => {
		const arg = pi.getFlag("ssh") as string | undefined;
		if (arg) {
			try {
				const next = await switchTarget(arg);
				ctx.ui.notify(connectedText(next), "info");
			} catch (e) {
				ctx.ui.notify(`SSH connect failed: ${e instanceof Error ? e.message : String(e)}`, "error");
			}
		}
		refreshStatus(ctx);
	});

	// --- teardown ---
	pi.on("session_shutdown", async () => {
		await disconnect();
	});

	// --- remind the agent that local tools stay local ---
	pi.on("before_agent_start", async (event) => {
		const t = get();
		const guidance = t
			? `\n\nSSH remote connected: ${t.remote}:${t.remoteCwd}. Local tools (read/write/edit/bash) operate on the local workspace. Use ssh_bash, ssh_read, ssh_write, and ssh_edit only for remote operations.`
			: "\n\nSSH tools are available but not connected. Use ssh_connect to connect or switch remotes when remote execution is needed. Local tools remain local.";
		return { systemPrompt: event.systemPrompt + guidance };
	});

	// ---------------------------------------------------------------------------
	// /ssh interactive dashboard (human-facing TUI; agent tools are unchanged)
	// ---------------------------------------------------------------------------
	function padRight(s: string, n: number): string {
		if (s.length === n) return s;
		return s.length > n ? s.slice(0, n) : s + " ".repeat(n - s.length);
	}

	const selectListTheme = (theme: any) => ({
		selectedPrefix: (t: string) => theme.fg("accent", t),
		selectedText: (t: string) => theme.fg("accent", t),
		description: (t: string) => theme.fg("muted", t),
		scrollInfo: (t: string) => theme.fg("dim", t),
		noMatch: (t: string) => theme.fg("warning", t),
	});

	type DashMode = "main" | "connect" | "output";
	const DASH_MAX_ROWS = 12;
	const DASH_OUT_MAX_BYTES = 16 * 1024;

	class SshDashboard {
		private mode: DashMode = "main";
		private rows: ProcRow[] = [];
		private sel = 0;
		private top = 0;
		private err: string | null = null;
		private pollTimer: NodeJS.Timeout | null = null;
		private polling = false;
		// connect sub-view
		private connectList: SelectList | null = null;
		// output sub-view
		private outId: string | null = null;
		private outName = "";
		private outBuf = "";
		private outAbort: AbortController | null = null;

		constructor(
			private tui: { requestRender: () => void },
			private theme: any,
			private ctx: any,
			private close: () => void,
		) {
			this.pollTimer = setInterval(() => void this.poll(), 2000);
			this.pollTimer.unref?.();
			void this.poll();
		}

		stop(): void {
			if (this.pollTimer) clearInterval(this.pollTimer);
			this.pollTimer = null;
			this.outAbort?.abort();
			this.outAbort = null;
		}

		private async poll(force = false): Promise<void> {
			if ((this.polling && !force) || !target) return;
			this.polling = true;
			try {
				this.rows = await listProcesses(target);
				this.err = null;
				if (this.sel >= this.rows.length) this.sel = Math.max(0, this.rows.length - 1);
			} catch (e) {
				this.err = e instanceof Error ? e.message : String(e);
			} finally {
				this.polling = false;
				this.tui.requestRender();
			}
		}

		// ---- rendering ----
		private rule(width: number, label?: string): string {
			if (!label) return this.theme.fg("accent", "\u2500".repeat(Math.max(0, width)));
			const left = `\u2500\u2500 ${label} `;
			return this.theme.fg("accent", left + "\u2500".repeat(Math.max(0, width - left.length)));
		}

		render(width: number): string[] {
			if (this.mode === "output") return this.renderOutput(width);
			if (this.mode === "connect") return this.renderConnect(width);
			return this.renderMain(width);
		}

		private renderMain(width: number): string[] {
			const T = this.theme;
			const out: string[] = [];
			const push = (s = "") => out.push(truncateToWidth(s, width));
			push(this.rule(width, "SSH"));
			const t = target;
			if (!t) {
				push("");
				push(`  ${T.fg("warning", "\u25cb not connected")}`);
				push("");
				push(`  ${T.fg("dim", "n connect \u00b7 q close")}`);
				push(this.rule(width));
				return out;
			}
			push("");
			push(`  ${T.fg("success", "\u25cf")} ${T.fg("text", "connected")}  ${T.fg("accent", `${t.remote} : ${t.remoteCwd}`)}${t.hasPython ? "" : T.fg("warning", "  (no python3)")}`);
			if (t.defaultCommandPrefix) push(`    ${T.fg("dim", "\u26a1")} ${T.fg("muted", t.defaultCommandPrefix)}`);
			if (t.defaultEnv && Object.keys(t.defaultEnv).length) push(`    ${T.fg("dim", "env")}  ${T.fg("muted", Object.keys(t.defaultEnv).join(", "))}`);
			const activeTunnels = ctx.tunnels.list();
			if (activeTunnels.length) {
				const tn = activeTunnels.map((x) => `localhost:${x.localPort}\u2192${x.remoteHost}:${x.remotePort}`).join(", ");
				push(`    ${T.fg("dim", "tunnels")}  ${T.fg("muted", tn)}`);
			}
			const syncStatus = ctx.sync.getState();
			if (syncStatus) push(`    ${T.fg("dim", "sync")}  ${T.fg("muted", `watching ${syncStatus.localSource} (${syncStatus.count} syncs)`)}`);
			push("");
			const statusHint = this.err ? T.fg("warning", `\u26a0 ${this.err}`) : T.fg("dim", "\u21bb live");
			push(`  ${T.fg("text", `Processes (${this.rows.length})`)}   ${statusHint}`);
			if (this.rows.length === 0) {
				push(`    ${T.fg("dim", "no processes")}`);
			} else {
				// window around selection
				if (this.sel < this.top) this.top = this.sel;
				if (this.sel >= this.top + DASH_MAX_ROWS) this.top = this.sel - DASH_MAX_ROWS + 1;
				const end = Math.min(this.rows.length, this.top + DASH_MAX_ROWS);
				for (let i = this.top; i < end; i++) {
					const r = this.rows[i];
					const sel = i === this.sel;
					const marker = r.status === "running" ? T.fg("success", "\u25cf") : r.code === 0 ? T.fg("success", "\u2713") : T.fg("error", "\u2717");
					const statusText = r.status === "running" ? "running" : `exited ${r.code ?? "?"}`;
					const rt = r.status === "running" && r.startedMs ? formatDuration(Date.now() - r.startedMs) : "";
					const namePart = sel ? T.fg("accent", padRight(r.name, 14)) : T.fg("text", padRight(r.name, 14));
					const prefix = sel ? T.fg("accent", "> ") : "  ";
					push(`  ${prefix}${marker} ${namePart} ${T.fg("muted", padRight(statusText, 11))} ${T.fg("dim", padRight(rt, 7))} ${T.fg("dim", `pid ${r.pid}`)}`);
				}
				if (end < this.rows.length || this.top > 0) push(`    ${T.fg("dim", `showing ${this.top + 1}-${end} of ${this.rows.length}`)}`);
			}
			push("");
			push(`  ${T.fg("dim", "enter output \u00b7 k kill \u00b7 c clear \u00b7 r refresh")}`);
			push(`  ${T.fg("dim", "n connect \u00b7 d disconnect \u00b7 w cwd \u00b7 y sync \u00b7 q close")}`);
			push(this.rule(width));
			return out;
		}

		private renderConnect(width: number): string[] {
			const T = this.theme;
			const out: string[] = [];
			out.push(truncateToWidth(this.rule(width, "SSH \u203a connect"), width));
			if (this.connectList) for (const l of this.connectList.render(width)) out.push(truncateToWidth(l, width));
			out.push(truncateToWidth(`  ${T.fg("dim", "\u2191\u2193 navigate \u00b7 enter select \u00b7 esc back")}`, width));
			out.push(truncateToWidth(this.rule(width), width));
			return out;
		}

		private renderOutput(width: number): string[] {
			const T = this.theme;
			const out: string[] = [];
			out.push(truncateToWidth(this.rule(width, `output \u203a ${this.outName}`), width));
			const lines = this.outBuf.split("\n");
			const visible = lines.slice(-(DASH_MAX_ROWS + 6));
			if (visible.length === 0 || (visible.length === 1 && visible[0] === "")) out.push(truncateToWidth(`  ${T.fg("dim", "waiting for output\u2026")}`, width));
			else for (const l of visible) out.push(truncateToWidth(l, width));
			out.push(truncateToWidth(this.rule(width), width));
			out.push(truncateToWidth(`  ${T.fg("dim", "esc back \u00b7 streaming live")}`, width));
			return out;
		}

		invalidate(): void {
			this.connectList?.invalidate?.();
		}

		// ---- input ----
		handleInput(data: string): void {
			if (this.mode === "output") return this.handleOutputInput(data);
			if (this.mode === "connect") return this.handleConnectInput(data);
			this.handleMainInput(data);
		}

		private handleMainInput(data: string): void {
			if (matchesKey(data, Key.up)) {
				if (this.sel > 0) this.sel--;
			} else if (matchesKey(data, Key.down)) {
				if (this.sel < this.rows.length - 1) this.sel++;
			} else if (matchesKey(data, Key.enter)) {
				const r = this.rows[this.sel];
				if (r) this.enterOutput(r);
			} else if (data === "k") {
				void this.killSelected();
			} else if (data === "c") {
				void this.clearFinished();
			} else if (data === "r") {
				void this.poll(true);
			} else if (data === "n") {
				this.enterConnect();
			} else if (data === "d") {
				void this.doDisconnect();
			} else if (data === "w") {
				if (target) {
					this.ctx.ui.setEditorText("/ssh cd ");
					this.close();
				}
			} else if (data === "y") {
				void this.toggleSync();
			} else if (data === "q" || matchesKey(data, Key.escape)) {
				this.close();
			}
		}

		private enterOutput(r: ProcRow): void {
			const t = target;
			if (!t) return;
			this.mode = "output";
			this.outId = r.id;
			this.outName = `${r.name} (${r.id})`;
			this.outBuf = "";
			this.outAbort = new AbortController();
			const dir = `${processRoot(t)}/${r.id}`;
			const ops = createRemoteBashOps(t, localCwd);
			ops
				.exec("timeout 3600 tail -n 200 -F stdout.log stderr.log 2>/dev/null", dir, {
					onData: (d: Buffer) => {
						this.outBuf += d.toString();
						if (this.outBuf.length > DASH_OUT_MAX_BYTES) this.outBuf = `\u2026${this.outBuf.slice(-DASH_OUT_MAX_BYTES)}`;
						this.tui.requestRender();
					},
					signal: this.outAbort.signal,
					timeout: 3600 + 15,
				})
				.catch(() => {
					/* aborted on leaving output, or tail ended: ignore */
				});
		}

		private handleOutputInput(data: string): void {
			if (matchesKey(data, Key.escape) || data === "q") {
				this.outAbort?.abort();
				this.outAbort = null;
				this.outId = null;
				this.mode = "main";
				void this.poll(true);
			}
		}

		private enterConnect(): void {
			let names: string[] = [];
			try {
				names = profileNames();
			} catch {
				/* corrupt profiles file: offer manual connect only */
			}
			const items: SelectItem[] = names.map((n) => ({ value: `@${n}`, label: `@${n}`, description: "saved profile" }));
			items.push({ value: "__new__", label: "type new connection\u2026", description: "prefill /ssh in the editor" });
			if (target) items.push({ value: "__off__", label: "disconnect", description: "close the active SSH connection" });
			const list = new SelectList(items, Math.min(items.length, 10), selectListTheme(this.theme));
			list.onSelect = (item: SelectItem) => void this.onConnectSelect(item.value);
			list.onCancel = () => {
				this.mode = "main";
				this.connectList = null;
				this.tui.requestRender();
			};
			this.connectList = list;
			this.mode = "connect";
		}

		private handleConnectInput(data: string): void {
			this.connectList?.handleInput(data);
		}

		private async onConnectSelect(value: string): Promise<void> {
			if (value === "__new__") {
				this.ctx.ui.setEditorText("/ssh ");
				this.close();
				return;
			}
			this.mode = "main";
			this.connectList = null;
			if (value === "__off__") {
				await this.doDisconnect();
				return;
			}
			try {
				await switchTarget(value);
				refreshStatus({ ui: this.ctx.ui });
				this.sel = 0;
				this.top = 0;
				await this.poll(true);
			} catch (e) {
				this.err = e instanceof Error ? e.message : String(e);
				this.tui.requestRender();
			}
		}

		private async doDisconnect(): Promise<void> {
			await disconnect();
			refreshStatus({ ui: this.ctx.ui });
			this.rows = [];
			this.tui.requestRender();
		}

		private async killSelected(): Promise<void> {
			const t = target;
			const r = this.rows[this.sel];
			if (!t || !r) return;
			poller.stopPoller(r.id);
			try {
				await runRemoteCommand(t, buildKillCommand(`${processRoot(t)}/${r.id}`));
			} catch (e) {
				this.err = e instanceof Error ? e.message : String(e);
			}
			await this.poll(true);
		}

		private async clearFinished(): Promise<void> {
			const t = target;
			if (!t) return;
			try {
				await runRemoteCommand(t, buildClearCommand(processRoot(t)));
			} catch (e) {
				this.err = e instanceof Error ? e.message : String(e);
			}
			await this.poll(true);
		}

		private async toggleSync(): Promise<void> {
			const t = target;
			if (!t) return;
			if (ctx.sync.getState()) {
				ctx.sync.stop();
				this.tui.requestRender();
				return;
			}
			try {
				await ctx.sync.startSync(t, {});
			} catch (e) {
				this.err = e instanceof Error ? e.message : String(e);
			}
			this.tui.requestRender();
		}
	}

	async function openSshDashboard(ctx: any): Promise<void> {
		if (typeof ctx?.ui?.custom !== "function") {
			ctx.ui.notify(target ? statusLabel(target) : "SSH: not connected", "info");
			return;
		}
		await ctx.ui.custom(
			(tui: any, theme: any, _kb: any, done: (v: null) => void) => {
				const dash = new SshDashboard(tui, theme, ctx, () => {
					dash.stop();
					done(null);
				});
				return {
					render: (w: number) => dash.render(w),
					invalidate: () => dash.invalidate(),
					handleInput: (d: string) => {
						dash.handleInput(d);
						tui.requestRender();
					},
				};
			},
			{ overlay: true, overlayOptions: { width: "72%", minWidth: 56, maxHeight: "85%" } },
		);
	}

	// --- runtime connect/disconnect/status ---
	pi.registerCommand("ssh", {
		description: "SSH remote: /ssh [-i key] user@host[:/path] [--activate <cmd>] [--env K=V] | /ssh @profile | /ssh cd <dir> | /ssh save <name> | /ssh profiles | /ssh off | /ssh",
		handler: async (args, ctx) => {
			const arg = args.trim();

			if (!arg) {
				await openSshDashboard(ctx);
				return;
			}

			if (arg === "off" || arg === "disconnect") {
				await disconnect();
				refreshStatus(ctx);
				ctx.ui.notify("SSH disconnected. Local tools remain local.", "info");
				return;
			}

			// /ssh cd <dir>: move the remote cwd without reconnecting (keeps socket,
			// python probe, activation, env). <dir> may be absolute or relative to cwd.
			if (arg === "cd" || arg.startsWith("cd ")) {
				try {
					const t = requireTarget();
					const dest = arg.slice(2).trim();
					// Empty or ~-prefixed targets need shell expansion, so leave them unquoted;
					// everything else is quoted to survive spaces.
					const cdTarget = dest === "" ? "cd ~" : dest.startsWith("~") ? `cd ${dest}` : `cd ${shQuote(dest)}`;
					const r = await runRemoteCommand(t, `cd -- ${shQuote(t.remoteCwd)} && ${cdTarget} && pwd -P`);
					if (r.code !== 0) throw new Error(r.stderr.toString().trim() || sshFailureMessage(r));
					t.remoteCwd = r.stdout.toString().trim();
					refreshStatus(ctx);
					ctx.ui.notify(`SSH cwd -> ${t.remoteCwd}`, "info");
				} catch (e) {
					ctx.ui.notify(`SSH cd failed: ${e instanceof Error ? e.message : String(e)}`, "error");
				}
				return;
			}

			// /ssh profiles: list saved connection handles.
			if (arg === "profiles" || arg === "ls") {
				try {
					const names = profileNames();
					ctx.ui.notify(names.length ? `Saved SSH profiles: ${names.map((n) => `@${n}`).join(", ")}` : `No saved profiles. Connect, then /ssh save <name> (stored in ${profilesPath()}).`, "info");
				} catch (e) {
					ctx.ui.notify(`SSH profiles error: ${e instanceof Error ? e.message : String(e)}`, "error");
				}
				return;
			}

			// /ssh save <name>: persist the active connection string as a profile.
			if (arg.startsWith("save ")) {
				try {
					const name = arg.slice(5).trim();
					if (!name) throw new Error("Usage: /ssh save <name>");
					saveProfile(name);
					ctx.ui.notify(`Saved SSH profile @${name} -> ${profilesPath()}. Reconnect later with /ssh @${name}`, "info");
				} catch (e) {
					ctx.ui.notify(`SSH save failed: ${e instanceof Error ? e.message : String(e)}`, "error");
				}
				return;
			}

			try {
				const next = await switchTarget(arg);
				refreshStatus(ctx);
				ctx.ui.notify(connectedText(next), "info");
			} catch (e) {
				ctx.ui.notify(`SSH connect failed: ${e instanceof Error ? e.message : String(e)}`, "error");
			}
		},
	});
}
