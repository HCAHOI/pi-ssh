// ---------------------------------------------------------------------------
// ssh_bash: run a bash command on the remote (cd -> env -> activation -> command)
// ---------------------------------------------------------------------------

import { Type } from "typebox";
import { createBashTool } from "@earendil-works/pi-coding-agent";
import type { SshContext } from "../context";
import { createRemoteBashOps } from "../remote-ops";

export function setupBashTool(ssh: SshContext): void {
	const { pi, localCwd, requireTarget, buildSshBashCommand, render } = ssh;
	const { str, sshTitle } = render;
	const localBash = createBashTool(localCwd);

	pi.registerTool({
		...localBash,
		name: "ssh_bash",
		label: "ssh_bash",
		description: `Execute a bash command on the active SSH remote in the remote cwd. Supports optional env, commandPrefix, and delaySeconds. Use local bash for local commands. ${localBash.description}`,
		promptSnippet: "Execute bash commands on the active SSH remote",
		promptGuidelines: [
			"Use ssh_bash for remote testing or GPU/server commands. Use bash for local commands and local file operations.",
			"For long-running work (downloads, training, dev servers), prefer ssh_process. An ssh_bash timeout closes the SSH connection and the remote command is likely terminated (SIGHUP); ssh_process survives disconnects and notifies you on completion.",
			"Large remote output is truncated to the last 50KB and the FULL output is saved to a local temp file (footer 'Full output: /tmp/pi-bash-*.log'); page that file with the local read tool's offset/limit instead of re-running the command — and scope commands with head/tail/grep/sed when you only need part.",
		],
		parameters: Type.Object({
			command: Type.String({ description: "Bash command to execute on the active SSH remote" }),
			timeout: Type.Optional(Type.Number({ description: "Timeout in seconds (include delaySeconds in this budget)" })),
			cwd: Type.Optional(Type.String({ description: "Working directory for this command, remote absolute path or local workspace path mapped under the remote cwd" })),
			delaySeconds: Type.Optional(Type.Number({ description: "Seconds to sleep on the remote before running command (increase timeout to cover it)" })),
			env: Type.Optional(Type.Record(Type.String(), Type.String(), { description: "Environment variables exported before command, e.g. {\"PYTHONSRC\": \"/path\"}" })),
			commandPrefix: Type.Optional(Type.String({ description: "Shell code run before command, e.g. 'source .venv/bin/activate'" })),
			tty: Type.Optional(Type.Boolean({ description: "Allocate a remote pty (ssh -tt) for commands that need a terminal (progress bars, some CLIs). Default false." })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const cmd = str(args?.command);
			// `$` bold in the default text color; command in green (success), not bold —
			// matches the bashMode tool-display where executed commands render green.
			const dollar = theme.fg("toolTitle", theme.bold("$"));
			const body = cmd ? `${dollar} ${theme.fg("success", cmd)}` : `${dollar} ${theme.fg("toolOutput", "...")}`;
			const timeout = args?.timeout ? theme.fg("muted", ` (timeout ${args.timeout}s)`) : "";
			return sshTitle("", `${body}${timeout}`, theme, context);
		},
		async execute(
			id,
			params: { command: string; timeout?: number; cwd?: string; delaySeconds?: number; env?: Record<string, string>; commandPrefix?: string; tty?: boolean },
			signal,
			onUpdate,
			_ctx,
		) {
			const t = requireTarget();
			const tool = createBashTool(localCwd, { operations: createRemoteBashOps(t, localCwd, { tty: params.tty }) });
			try {
				return await tool.execute(
					id,
					{ command: buildSshBashCommand(t, params), timeout: params.timeout },
					signal,
					onUpdate,
					_ctx,
				);
			} catch (e) {
				// Make the timeout outcome unambiguous: say it WAS a timeout, what it did
				// to the remote, and what to use instead. The SDK formats the timeout as
				// "Command timed out after N seconds"; we append the consequence + remedy.
				const msg = e instanceof Error ? e.message : String(e);
				if (/Command timed out after/.test(msg)) {
					throw new Error(
						`${msg}\n[ssh_bash] The timeout closed the local SSH connection, so the remote command was most likely terminated (SIGHUP). If it must keep running, re-run it with ssh_process (it survives disconnects and notifies you on completion) rather than ssh_bash with a timeout.`,
					);
				}
				throw e;
			}
		},
	});
}
