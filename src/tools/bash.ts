// ---------------------------------------------------------------------------
// ssh_bash: run a bash command on the remote (cd -> env -> activation -> command)
// ---------------------------------------------------------------------------

import { Type } from "typebox";
import { createBashTool } from "@earendil-works/pi-coding-agent";
import type { SshContext } from "../context";
import { createRemoteBashOps } from "../remote-ops";
import { compactNoisyOutput } from "../output-filter";

// Apply compactNoisyOutput to every text block of a bash result, appending a
// one-line note when anything was collapsed so the agent knows output was elided.
function compactBashResult(result: any, command: string): any {
	if (!result || !Array.isArray(result.content)) return result;
	let note: string | null = null;
	const content = result.content.map((block: any) => {
		if (block?.type !== "text" || typeof block.text !== "string") return block;
		const { text, stats } = compactNoisyOutput(command, block.text);
		if (stats.compacted) note = `[ssh_bash] collapsed routine output: ${stats.original} \u2192 ${stats.kept} lines (verbose:true for raw).`;
		return { ...block, text };
	});
	if (note) content.push({ type: "text", text: note });
	return { ...result, content };
}

export function setupBashTool(ssh: SshContext): void {
	const { pi, localCwd, requireTarget, buildSshBashCommand, render } = ssh;
	const { str, sshTitle } = render;
	const localBash = createBashTool(localCwd);

	pi.registerTool({
		...localBash,
		name: "ssh_bash",
		label: "ssh_bash",
		description: "Execute a bash command on the active SSH remote. Use local bash for local commands; use ssh_process for long-running remote jobs.",
		promptSnippet: "Execute bash commands on the active SSH remote",
		promptGuidelines: [
			"Use ssh_bash for short remote commands. Use local bash for local commands and ssh_process for long-running remote work.",
			"Routine apt/pip/npm/docker progress lines are auto-collapsed to keep errors, warnings, summaries, and a tail; pass verbose:true to see raw output. If output is large/truncated, read the reported full log path instead of rerunning.",
		],
		parameters: Type.Object({
			command: Type.String({ description: "Bash command to execute on the active SSH remote" }),
			timeout: Type.Optional(Type.Number({ description: "Timeout in seconds (include delaySeconds in this budget)" })),
			cwd: Type.Optional(Type.String({ description: "Working directory for this command, remote absolute path or local workspace path mapped under the remote cwd" })),
			delaySeconds: Type.Optional(Type.Number({ description: "Seconds to sleep on the remote before running command (increase timeout to cover it)" })),
			env: Type.Optional(Type.Record(Type.String(), Type.String(), { description: "Environment variables exported before command, e.g. {\"PYTHONSRC\": \"/path\"}" })),
			commandPrefix: Type.Optional(Type.String({ description: "Shell code run before command, e.g. 'source .venv/bin/activate'" })),
			tty: Type.Optional(Type.Boolean({ description: "Allocate a remote pty (ssh -tt) for commands that need a terminal (progress bars, some CLIs). Default false." })),
			verbose: Type.Optional(Type.Boolean({ description: "Skip auto-compaction of noisy apt/pip/npm/docker output and return raw text. Default false." })),
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
			params: { command: string; timeout?: number; cwd?: string; delaySeconds?: number; env?: Record<string, string>; commandPrefix?: string; tty?: boolean; verbose?: boolean },
			signal,
			onUpdate,
			_ctx,
		) {
			const t = requireTarget();
			const tool = createBashTool(localCwd, { operations: createRemoteBashOps(t, localCwd, { tty: params.tty }) });
			try {
				const result = await tool.execute(
					id,
					{ command: buildSshBashCommand(t, params), timeout: params.timeout },
					signal,
					onUpdate,
					_ctx,
				);
				if (params.verbose) return result;
				// Collapse routine apt/pip/npm/docker progress noise in the text blocks,
				// keeping errors/warnings/summaries/tail. Other commands pass through.
				return compactBashResult(result, params.command);
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
