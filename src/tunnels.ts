// ---------------------------------------------------------------------------
// ssh_tunnel: port-forwarding over the shared ControlMaster connection
// ---------------------------------------------------------------------------

import { Type } from "typebox";
import type { RunResult, SshTarget } from "./types";
import type { SshContext } from "./context";
import { baseSshOptions, runSsh, sshFailureMessage } from "./ssh/transport";

interface TunnelState {
	localPort: number;
	remoteHost: string;
	remotePort: number;
	spec: string;
}

export interface TunnelManager {
	/** Cancel every active forward (best-effort) and clear the registry. */
	stopAll(): void;
	/** Re-issue every tracked forward (e.g. after a reconnect respawns the master). */
	restoreAll(): Promise<{ restored: number; failed: number }>;
	/** Active forwards, for status display (e.g. the /ssh dashboard). */
	list(): Array<{ localPort: number; remoteHost: string; remotePort: number }>;
}

/** Create the tunnel manager and register the ssh_tunnel tool. */
export function createTunnelManager(ctx: SshContext): TunnelManager {
	const { pi, requireTarget, render } = ctx;
	const { str, sshTitle } = render;
	const tunnels = new Map<number, TunnelState>();

	function tunnelControl(t: SshTarget, action: "forward" | "cancel", spec: string): Promise<RunResult> {
		// `ssh -O forward/cancel -L <spec> host` asks the running ControlMaster to open
		// or close a forward without spawning a new session.
		return runSsh([...t.sshOptions, ...baseSshOptions(t.socket), "-O", action, "-L", spec, "--", t.remote], { timeout: 10 });
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

	pi.registerTool({
		name: "ssh_tunnel",
		label: "ssh_tunnel",
		description: "Forward a remote port to localhost over the shared SSH connection. Actions: open | close | list; tunnels close on disconnect.",
		promptSnippet: "Forward a remote port to localhost over SSH",
		promptGuidelines: ["Use ssh_tunnel open for remote web UIs/dev servers; close it when done."],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("open"), Type.Literal("close"), Type.Literal("list")]),
			localPort: Type.Optional(Type.Number({ description: "Local port to bind (open/close). Defaults to remotePort." })),
			remotePort: Type.Optional(Type.Number({ description: "Remote port to forward (required for open)" })),
			remoteHost: Type.Optional(Type.String({ description: "Remote-side host the port lives on (default 127.0.0.1)" })),
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
		async execute(_id, params: { action: "open" | "close" | "list"; localPort?: number; remotePort?: number; remoteHost?: string }) {
			const t = requireTarget();
			if (params.action === "list") {
				if (tunnels.size === 0) return { content: [{ type: "text" as const, text: "No active tunnels." }] };
				const text = [...tunnels.values()].map((tn) => `localhost:${tn.localPort} -> ${t.remote} ${tn.remoteHost}:${tn.remotePort}`).join("\n");
				return { content: [{ type: "text" as const, text }] };
			}
			const remoteHost = params.remoteHost?.trim() || "127.0.0.1";
			if (params.action === "open") {
				if (!params.remotePort) throw new Error("ssh_tunnel open requires remotePort");
				const localPort = params.localPort ?? params.remotePort;
				const spec = `${localPort}:${remoteHost}:${params.remotePort}`;
				if (tunnels.has(localPort)) throw new Error(`Local port ${localPort} already forwarded; close it first.`);
				const r = await tunnelControl(t, "forward", spec);
				if (r.code !== 0) throw new Error(`Tunnel open failed: ${r.stderr.toString().trim() || r.stdout.toString().trim() || sshFailureMessage(r)}`);
				tunnels.set(localPort, { localPort, remoteHost, remotePort: params.remotePort, spec });
				return { content: [{ type: "text" as const, text: `Tunnel open: http://localhost:${localPort} -> ${t.remote} ${remoteHost}:${params.remotePort}` }] };
			}
			// close
			const localPort = params.localPort ?? params.remotePort;
			if (!localPort) throw new Error("ssh_tunnel close requires localPort (or remotePort)");
			const tn = tunnels.get(localPort);
			if (!tn) throw new Error(`No tunnel on local port ${localPort}.`);
			const r = await tunnelControl(t, "cancel", tn.spec);
			tunnels.delete(localPort);
			if (r.code !== 0) throw new Error(`Tunnel close reported: ${r.stderr.toString().trim() || sshFailureMessage(r)}`);
			return { content: [{ type: "text" as const, text: `Tunnel closed: localhost:${localPort}` }] };
		},
	});

	return {
		stopAll,
		restoreAll,
		list: () => [...tunnels.values()].map((tn) => ({ localPort: tn.localPort, remoteHost: tn.remoteHost, remotePort: tn.remotePort })),
	};
}
