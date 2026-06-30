// ---------------------------------------------------------------------------
// Agent-callable connection management: ssh_connect / ssh_disconnect / ssh_status
// ---------------------------------------------------------------------------

import { Type } from "typebox";
import type { SshContext } from "../context";
import { formatSshSitrep } from "../sitrep";

export function setupConnectionTools(ssh: SshContext): void {
	const { pi, getTarget, switchTarget, disconnect, refreshStatus, connectedText, profileNames, render } = ssh;
	const { str, sshTitle } = render;

	pi.registerTool({
		name: "ssh_connect",
		label: "ssh_connect",
		description: "Connect, reconnect, or switch the active SSH remote for ssh_* tools. Same target syntax as /ssh; supports --activate and repeated --env.",
		promptSnippet: "Connect or switch the active SSH remote used by ssh_* tools",
		promptGuidelines: [
			"Use ssh_connect when the user asks to connect, disconnect, or switch SSH servers.",
			"After connecting, use ssh_* tools for remote operations and local tools for local work.",
		],
		parameters: Type.Object({
			target: Type.String({ description: "SSH target: user@host[:/abs/path], ssh options, @profile, plus optional --activate/--env" }),
		}),
		renderCall(args: any, theme: any, context: any) {
			return sshTitle("connect", theme.fg("accent", str(args?.target)), theme, context);
		},
		async execute(_id, params: { target: string }, _signal, _onUpdate, ctx) {
			const next = await switchTarget(params.target);
			refreshStatus(ctx);
			return { content: [{ type: "text" as const, text: connectedText(next) }] };
		},
	});

	pi.registerTool({
		name: "ssh_disconnect",
		label: "ssh_disconnect",
		description: "Disconnect the active SSH remote. Local tools are unaffected.",
		promptSnippet: "Disconnect the active SSH remote",
		parameters: Type.Object({}),
		renderCall(_args: any, theme: any, context: any) {
			return sshTitle("disconnect", "", theme, context);
		},
		async execute(_id, _params, _signal, _onUpdate, ctx) {
			await disconnect();
			refreshStatus(ctx);
			return { content: [{ type: "text" as const, text: "SSH disconnected. Local tools remain local." }] };
		},
	});

	pi.registerTool({
		name: "ssh_status",
		label: "ssh_status",
		description: "Show the active SSH remote used by ssh_* tools. Pass verbose:true for processes, monitors, tunnels, and sync state.",
		promptSnippet: "Show active SSH connection status",
		parameters: Type.Object({
			verbose: Type.Optional(Type.Boolean({ description: "Include processes, monitors, tunnels, and sync state (default false)" })),
		}),
		renderCall(_args: any, theme: any, context: any) {
			return sshTitle("status", "", theme, context);
		},
		async execute(_id, params: { verbose?: boolean }) {
			const t = getTarget();
			if (!t) return { content: [{ type: "text" as const, text: "SSH: not connected" }] };
			if (params.verbose) return { content: [{ type: "text" as const, text: await formatSshSitrep(ssh, t) }] };
			const base = connectedText(t);
			let profiles: string[] = [];
			try { profiles = profileNames(); } catch { /* corrupt profiles file: ignore for status */ }
			const profileLine = profiles.length ? `\nSaved profiles (reconnect with ssh_connect '@name'): ${profiles.map((n) => `@${n}`).join(", ")}` : "";
			return { content: [{ type: "text" as const, text: base + profileLine }] };
		},
	});
}
