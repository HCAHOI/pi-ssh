// ---------------------------------------------------------------------------
// ssh_tunnel: port-forwarding over the shared ControlMaster connection
// ---------------------------------------------------------------------------

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { request } from "node:http";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { Type } from "typebox";
import type { RunResult, SshTarget } from "./types";
import type { SshContext } from "./context";
import { baseSshOptions, runSsh, sshFailureMessage } from "./ssh/transport";

interface TunnelState {
	localPort: number;
	remoteHost: string;
	remotePort: number;
	spec: string;
	saved: boolean;
}

interface SavedTunnel {
	localPort: number;
	remoteHost: string;
	remotePort: number;
}

interface TunnelStore {
	version: 1;
	tunnels: Record<string, SavedTunnel[]>;
}

export interface TunnelManager {
	/** Cancel every active forward (best-effort) and clear the in-memory registry. Saved tunnels remain saved. */
	stopAll(): void;
	/** Re-issue every currently active forward (e.g. after a reconnect respawns the master). */
	restoreAll(): Promise<{ restored: number; failed: number }>;
	/** Load saved forwards for a target and open them. */
	restoreSaved(t: SshTarget): Promise<{ restored: number; failed: number }>;
	/** Active forwards, for status display (e.g. the /ssh dashboard). */
	list(): Array<{ localPort: number; remoteHost: string; remotePort: number; saved: boolean }>;
}

/** Create the tunnel manager and register the ssh_tunnel tool. */
export function createTunnelManager(ctx: SshContext): TunnelManager {
	const { pi, requireTarget, render } = ctx;
	const { str, sshTitle } = render;
	const tunnels = new Map<number, TunnelState>();

	function tunnelStorePath(): string {
		return join(homedir(), ".pi", "ssh-tunnels.json");
	}

	function tunnelKey(t: SshTarget): string {
		return `${t.remote}\t${t.remoteCwd}`;
	}

	function readStore(): TunnelStore {
		const path = tunnelStorePath();
		if (!existsSync(path)) return { version: 1, tunnels: {} };
		let parsed: unknown;
		try {
			parsed = JSON.parse(readFileSync(path, "utf8"));
		} catch (e) {
			throw new Error(`Corrupt SSH tunnel store ${path}: ${e instanceof Error ? e.message : String(e)}`);
		}
		if (!parsed || typeof parsed !== "object") return { version: 1, tunnels: {} };
		const obj = parsed as Partial<TunnelStore>;
		return { version: 1, tunnels: obj.tunnels && typeof obj.tunnels === "object" ? obj.tunnels : {} };
	}

	function writeStore(store: TunnelStore): void {
		const path = tunnelStorePath();
		mkdirSync(dirname(path), { recursive: true });
		writeFileSync(path, `${JSON.stringify(store, null, 2)}\n`, { mode: 0o600 });
	}

	function savedFor(t: SshTarget): SavedTunnel[] {
		return readStore().tunnels[tunnelKey(t)] ?? [];
	}

	function saveTunnel(t: SshTarget, tn: SavedTunnel): void {
		const store = readStore();
		const key = tunnelKey(t);
		const rows = (store.tunnels[key] ?? []).filter((x) => x.localPort !== tn.localPort);
		rows.push(tn);
		rows.sort((a, b) => a.localPort - b.localPort);
		store.tunnels[key] = rows;
		writeStore(store);
	}

	function removeSavedTunnel(t: SshTarget, localPort: number): boolean {
		const store = readStore();
		const key = tunnelKey(t);
		const before = store.tunnels[key] ?? [];
		const after = before.filter((x) => x.localPort !== localPort);
		if (after.length === before.length) return false;
		if (after.length) store.tunnels[key] = after;
		else delete store.tunnels[key];
		writeStore(store);
		return true;
	}

	function tunnelControl(t: SshTarget, action: "forward" | "cancel", spec: string): Promise<RunResult> {
		// `ssh -O forward/cancel -L <spec> host` asks the running ControlMaster to open
		// or close a forward without spawning a new session.
		return runSsh([...t.sshOptions, ...baseSshOptions(t.socket), "-O", action, "-L", spec, "--", t.remote], { timeout: 10 });
	}

	function validatePort(value: number, name: string, allowZero = false): number {
		if (!Number.isInteger(value) || value < (allowZero ? 0 : 1) || value > 65535) {
			throw new Error(`${name} must be an integer ${allowZero ? "0-65535" : "1-65535"}, got ${value}`);
		}
		return value;
	}

	function freeLocalPort(): Promise<number> {
		return new Promise((resolve, reject) => {
			const server = createServer();
			server.unref?.();
			server.on("error", reject);
			server.listen(0, "127.0.0.1", () => {
				const address = server.address();
				const port = typeof address === "object" && address ? address.port : 0;
				server.close((err) => (err ? reject(err) : resolve(port)));
			});
		});
	}

	function httpProbe(localPort: number, probePath: string): Promise<boolean> {
		return new Promise((resolve) => {
			const req = request({ host: "127.0.0.1", port: localPort, path: probePath, method: "GET", timeout: 1000 }, (res) => {
				res.resume();
				// Any HTTP response below 500 means the service behind the tunnel is reachable;
				// auth redirects/401/404 still prove readiness for a generic web UI.
				resolve((res.statusCode ?? 599) < 500);
			});
			req.on("timeout", () => {
				req.destroy();
				resolve(false);
			});
			req.on("error", () => resolve(false));
			req.end();
		});
	}

	async function waitHttpReady(localPort: number, probePath: string, timeoutSeconds: number, signal?: AbortSignal): Promise<boolean> {
		const deadline = Date.now() + timeoutSeconds * 1000;
		while (!signal?.aborted && Date.now() <= deadline) {
			if (await httpProbe(localPort, probePath)) return true;
			await new Promise((resolve) => setTimeout(resolve, 300));
		}
		return false;
	}

	function stopAll(): void {
		const target = ctx.getTarget();
		if (!target) {
			tunnels.clear();
			return;
		}
		for (const tn of tunnels.values()) void tunnelControl(target, "cancel", tn.spec).catch(() => {});
		tunnels.clear();
	}

	// Re-issue every tracked forward against the (re)connected master. The master
	// loses its -L forwards when it dies, so after a reconnect the registry is the
	// source of truth: cancel any stale forward (best-effort), then forward again.
	async function restoreAll(): Promise<{ restored: number; failed: number }> {
		const target = ctx.getTarget();
		if (!target || tunnels.size === 0) return { restored: 0, failed: 0 };
		let restored = 0;
		let failed = 0;
		for (const tn of tunnels.values()) {
			await tunnelControl(target, "cancel", tn.spec).catch(() => {});
			try {
				const r = await tunnelControl(target, "forward", tn.spec);
				if (r.code === 0) restored++;
				else failed++;
			} catch {
				failed++;
			}
		}
		return { restored, failed };
	}

	async function restoreSaved(t: SshTarget): Promise<{ restored: number; failed: number }> {
		let saved: SavedTunnel[] = [];
		try {
			saved = savedFor(t);
		} catch {
			return { restored: 0, failed: 1 };
		}
		let restored = 0;
		let failed = 0;
		for (const row of saved) {
			const localPort = validatePort(row.localPort, "saved localPort");
			const remotePort = validatePort(row.remotePort, "saved remotePort");
			const remoteHost = row.remoteHost?.trim() || "127.0.0.1";
			const spec = `${localPort}:${remoteHost}:${remotePort}`;
			if (tunnels.has(localPort)) continue;
			try {
				const r = await tunnelControl(t, "forward", spec);
				if (r.code === 0) {
					tunnels.set(localPort, { localPort, remoteHost, remotePort, spec, saved: true });
					restored++;
				} else {
					failed++;
				}
			} catch {
				failed++;
			}
		}
		return { restored, failed };
	}

	pi.registerTool({
		name: "ssh_tunnel",
		label: "ssh_tunnel",
		description: "Forward a remote port to localhost over the shared SSH connection. Actions: open | close | list; saved tunnels restore on reconnect.",
		promptSnippet: "Forward a remote port to localhost over SSH",
		promptGuidelines: ["Use ssh_tunnel open for remote web UIs/dev servers; close it when done. Tunnels are saved by default and restore on reconnect; pass save:false for temporary tunnels."],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("open"), Type.Literal("close"), Type.Literal("list")]),
			localPort: Type.Optional(Type.Number({ description: "Local port to bind (open/close). Defaults to remotePort. Use 0 to auto-pick a free local port." })),
			remotePort: Type.Optional(Type.Number({ description: "Remote port to forward (required for open)" })),
			remoteHost: Type.Optional(Type.String({ description: "Remote-side host the port lives on (default 127.0.0.1)" })),
			save: Type.Optional(Type.Boolean({ description: "Persist this tunnel and restore it on reconnect (default true). open only." })),
			wait: Type.Optional(Type.Boolean({ description: "After opening, poll http://127.0.0.1:<localPort><probePath> until reachable or timeout. Default false." })),
			probePath: Type.Optional(Type.String({ description: "HTTP readiness probe path for wait=true (default /)." })),
			waitTimeoutSeconds: Type.Optional(Type.Number({ description: "HTTP readiness timeout in seconds for wait=true (default 15)." })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const a = str(args?.action) || "?";
			if (a === "open" || a === "close") {
				const lp = args?.localPort ?? args?.remotePort;
				const spec = `${lp ?? "?"}:${args?.remoteHost ?? "127.0.0.1"}:${args?.remotePort ?? "?"}`;
				return sshTitle("tunnel", `${theme.fg("accent", a)} ${theme.fg("muted", spec)}`, theme, context);
			}
			return sshTitle("tunnel", theme.fg("accent", a), theme, context);
		},
		async execute(_id, params: { action: "open" | "close" | "list"; localPort?: number; remotePort?: number; remoteHost?: string; save?: boolean; wait?: boolean; probePath?: string; waitTimeoutSeconds?: number }, signal) {
			const t = requireTarget();
			if (params.action === "list") {
				if (tunnels.size === 0) return { content: [{ type: "text" as const, text: "No active tunnels." }], details: undefined };
				const text = [...tunnels.values()].map((tn) => `localhost:${tn.localPort} -> ${t.remote} ${tn.remoteHost}:${tn.remotePort}${tn.saved ? " [saved]" : ""}`).join("\n");
				return { content: [{ type: "text" as const, text }], details: undefined };
			}
			const remoteHost = params.remoteHost?.trim() || "127.0.0.1";
			if (params.action === "open") {
				if (!params.remotePort) throw new Error("ssh_tunnel open requires remotePort");
				const remotePort = validatePort(params.remotePort, "remotePort");
				const requestedLocalPort = validatePort(params.localPort ?? remotePort, "localPort", true);
				const localPort = requestedLocalPort === 0 ? await freeLocalPort() : requestedLocalPort;
				const spec = `${localPort}:${remoteHost}:${remotePort}`;
				if (tunnels.has(localPort)) throw new Error(`Local port ${localPort} already forwarded; close it first.`);
				const r = await tunnelControl(t, "forward", spec);
				if (r.code !== 0) throw new Error(`Tunnel open failed: ${r.stderr.toString().trim() || r.stdout.toString().trim() || sshFailureMessage(r)}`);
				const saved = params.save !== false;
				tunnels.set(localPort, { localPort, remoteHost, remotePort, spec, saved });
				if (saved) saveTunnel(t, { localPort, remoteHost, remotePort });
				let readiness = "";
				if (params.wait) {
					const probePath = params.probePath?.trim() ? (params.probePath.startsWith("/") ? params.probePath : `/${params.probePath}`) : "/";
					const timeout = Math.max(1, Math.floor(params.waitTimeoutSeconds ?? 15));
					const ready = await waitHttpReady(localPort, probePath, timeout, signal);
					readiness = ready
						? `\nReady: http://localhost:${localPort}${probePath}`
						: `\nTunnel is open, but HTTP readiness did not pass within ${timeout}s: http://localhost:${localPort}${probePath}`;
				}
				return { content: [{ type: "text" as const, text: `Tunnel open${saved ? " (saved)" : ""}: http://localhost:${localPort} -> ${t.remote} ${remoteHost}:${remotePort}${readiness}` }], details: { localPort, remoteHost, remotePort, saved } };
			}
			// close
			const localPort = validatePort(params.localPort ?? params.remotePort ?? 0, "localPort");
			const tn = tunnels.get(localPort);
			if (!tn) {
				const removed = removeSavedTunnel(t, localPort);
				if (removed) return { content: [{ type: "text" as const, text: `Removed saved tunnel on localhost:${localPort}.` }], details: undefined };
				throw new Error(`No tunnel on local port ${localPort}.`);
			}
			const r = await tunnelControl(t, "cancel", tn.spec);
			tunnels.delete(localPort);
			removeSavedTunnel(t, localPort);
			if (r.code !== 0) throw new Error(`Tunnel close reported: ${r.stderr.toString().trim() || sshFailureMessage(r)}`);
			return { content: [{ type: "text" as const, text: `Tunnel closed: localhost:${localPort}` }], details: undefined };
		},
	});

	return {
		stopAll,
		restoreAll,
		restoreSaved,
		list: () => [...tunnels.values()].map((tn) => ({ localPort: tn.localPort, remoteHost: tn.remoteHost, remotePort: tn.remotePort, saved: tn.saved })),
	};
}
