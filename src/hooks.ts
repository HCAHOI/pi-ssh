// ---------------------------------------------------------------------------
// Session hooks: startup --ssh flag, teardown, and the local-tools reminder
// ---------------------------------------------------------------------------

import type { SshContext } from "./context";

export function setupHooks(ctx: SshContext): void {
	const { pi, switchTarget, connectedText, refreshStatus, disconnect, getTarget } = ctx;

	// --- startup flag ---
	pi.on("session_start", async (_event, toolCtx) => {
		const arg = pi.getFlag("ssh") as string | undefined;
		if (arg) {
			try {
				const next = await switchTarget(arg);
				toolCtx.ui.notify(connectedText(next), "info");
			} catch (e) {
				toolCtx.ui.notify(`SSH connect failed: ${e instanceof Error ? e.message : String(e)}`, "error");
			}
		}
		refreshStatus(toolCtx);
	});

	// --- teardown ---
	pi.on("session_shutdown", async () => {
		await disconnect();
	});

	// --- remind the agent that local tools stay local ---
	pi.on("before_agent_start", async (event) => {
		const t = getTarget();
		const guidance = t
			? `\n\nSSH remote connected: ${t.remote}:${t.remoteCwd}. Local tools (read/write/edit/bash) operate on the local workspace. Use ssh_bash, ssh_read, ssh_write, and ssh_edit only for remote operations. ssh_process output/logs/kill/attach accept name as well as id. ssh_tunnel open supports localPort=0, wait/probePath, and saved tunnels restore on reconnect.`
			: "\n\nSSH tools are available but not connected. Use ssh_connect to connect or switch remotes when remote execution is needed. Local tools remain local. After connecting, saved tunnels restore automatically; ssh_tunnel supports localPort=0 and wait/probePath.";
		return { systemPrompt: event.systemPrompt + guidance };
	});
}
