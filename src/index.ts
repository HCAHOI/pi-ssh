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

import type { Activation, SshTarget } from "./types";
import { buildEnvExports, shQuote, toRemotePath } from "./utils";
import { setReconnectNotifier } from "./ssh/reconnect";
import { closeMaster } from "./ssh/transport";
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
import { setupMonitorTool } from "./tools/monitor";
import { setupTransferTools } from "./tools/transfer";
import { setupHooks } from "./hooks";
import { setupDashboard } from "./dashboard";
import { createPollerManager } from "./poller";
import { createMonitorManager } from "./monitor";
import {
	listProcesses,
} from "./process-queries";


// ---------------------------------------------------------------------------
// Extension
// ---------------------------------------------------------------------------

export default function (pi: ExtensionAPI) {
	pi.registerFlag("ssh", { description: "SSH remote, e.g. user@host[:/path], -i key user@host, optionally with --activate <cmd> / --env K=V", type: "string" });

	const localCwd = process.cwd();

	// Background ssh_process notification poller (owns pollers + latestStartByName).
	const poller = createPollerManager(pi);
	// Runtime-managed log monitors (decoupled from ssh_process; owns its own store).
	const monitors = createMonitorManager(pi);
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
			monitors.repointAll(next);
		} else {
			poller.stopAll();
			monitors.stopAll();
		}
		if (prev) await closeMaster(prev);
		target = next;
		ctx.tunnels.stopAll();
		await poller.rehydrate(next);
		await monitors.rehydrate(next);
		return next;
	}

	async function disconnect(): Promise<void> {
		poller.stopAll();
		monitors.stopAll();
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
		monitors,
		emit,
		render,
		connect,
		switchTarget,
		disconnect,
		refreshStatus,
		connectedText,
		statusLabel,
		profileNames,
		profilesPath,
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
	setupMonitorTool(ctx);
	setupTransferTools(ctx);

	// --- session hooks + /ssh dashboard command ---
	setupHooks(ctx);
	setupDashboard(ctx);
}
