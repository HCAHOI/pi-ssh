// ---------------------------------------------------------------------------
// Remote filesystem tools: ssh_read / ssh_ls / ssh_find / ssh_grep / ssh_write /
// ssh_secret_write / ssh_edit. Each wraps the SDK's local tool with remote ops.
// ---------------------------------------------------------------------------

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { Type } from "typebox";
import {
	createEditTool,
	createFindTool,
	createGrepTool,
	createLsTool,
	createReadTool,
	createReadToolDefinition,
	createWriteTool,
} from "@earendil-works/pi-coding-agent";
import type { SshContext } from "../context";
import { shQuote, toRemotePath, withFileLock } from "../utils";
import { withReconnect } from "../ssh/reconnect";
import { runRemoteCommand, sshFailureMessage } from "../ssh/transport";
import {
	createRemoteEditOps,
	createRemoteFindOps,
	createRemoteLsOps,
	createRemoteReadOps,
	createRemoteWriteOps,
	remotePatchEdit,
	runRemoteGrep,
} from "../remote-ops";

export function setupFsTools(ssh: SshContext): void {
	const { pi, localCwd, requireTarget, render } = ssh;
	const { str, accentRemotePath, readLineRange, remoteDisplayPath, sshTitle, renderEditDiffResult } = render;

	const localRead = createReadTool(localCwd);
	const localReadDef = createReadToolDefinition(localCwd);
	const localLs = createLsTool(localCwd);
	const localFind = createFindTool(localCwd);
	const localGrep = createGrepTool(localCwd);
	const localWrite = createWriteTool(localCwd);
	const localEdit = createEditTool(localCwd);

	// --- remote read ---
	pi.registerTool({
		...localRead,
		name: "ssh_read",
		label: "ssh_read",
		description: "Read a file from the active SSH remote. Relative paths resolve under the remote cwd; use local read for local files.",
		promptSnippet: "Read files from the active SSH remote",
		promptGuidelines: [
			"Use ssh_read only for remote files. Use read for local files in the current local workspace.",
			"For large remote files, page with offset/limit rather than reading the whole file (same as the local read tool) so you do not flood context.",
		],
		renderCall(args: any, theme: any, context: any) {
			return sshTitle("read", `${accentRemotePath(args?.path, theme)}${readLineRange(args, theme)}`, theme, context);
		},
		renderResult: localReadDef.renderResult,
		async execute(id, params, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const tool = createReadTool(localCwd, { operations: createRemoteReadOps(t, localCwd) });
			return withReconnect(() => tool.execute(id, params, signal, onUpdate, _ctx));
		},
	});

	// --- remote ls ---
	pi.registerTool({
		...localLs,
		name: "ssh_ls",
		label: "ssh_ls",
		description: "List a directory on the active SSH remote. Relative paths resolve under the remote cwd; use local ls/find for local files.",
		promptSnippet: "List directories on the active SSH remote",
		promptGuidelines: ["Use ssh_ls only for remote directory listings. Use local ls/find tools for local workspace exploration."],
		renderCall(args: any, theme: any, context: any) {
			return sshTitle("ls", accentRemotePath(args?.path, theme), theme, context);
		},
		async execute(id, params, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const tool = createLsTool(localCwd, { operations: createRemoteLsOps(t, localCwd) });
			return withReconnect(() => tool.execute(id, params, signal, onUpdate, _ctx));
		},
	});

	// --- remote find ---
	pi.registerTool({
		...localFind,
		name: "ssh_find",
		label: "ssh_find",
		description: "Find files on the active SSH remote. Relative paths resolve under the remote cwd; use local find/fffind for local files.",
		promptSnippet: "Find files on the active SSH remote",
		promptGuidelines: ["Use ssh_find only for remote file discovery. Use local find/fffind for local workspace files."],
		renderCall(args: any, theme: any, context: any) {
			const pat = theme.fg("accent", str(args?.pattern) || "...");
			const where = args?.path ? ` ${theme.fg("muted", `in ${remoteDisplayPath(str(args.path))}`)}` : "";
			return sshTitle("find", `${pat}${where}`, theme, context);
		},
		async execute(id, params, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const tool = createFindTool(localCwd, { operations: createRemoteFindOps(t, localCwd) });
			return withReconnect(() => tool.execute(id, params, signal, onUpdate, _ctx));
		},
	});

	// --- remote grep ---
	pi.registerTool({
		...localGrep,
		name: "ssh_grep",
		label: "ssh_grep",
		description: "Search file contents on the active SSH remote. Uses remote rg when available, otherwise grep; use local grep/ffgrep for local files.",
		promptSnippet: "Search file contents on the active SSH remote",
		promptGuidelines: ["Use ssh_grep only for remote content search. Use local grep/ffgrep for local workspace files."],
		renderCall(args: any, theme: any, context: any) {
			const pat = theme.fg("accent", str(args?.pattern) || "...");
			const where = args?.path ? ` ${theme.fg("muted", `in ${remoteDisplayPath(str(args.path))}`)}` : "";
			return sshTitle("grep", `${pat}${where}`, theme, context);
		},
		async execute(
			_id,
			params: { pattern: string; path?: string; glob?: string; ignoreCase?: boolean; literal?: boolean; context?: number; limit?: number },
			signal,
		) {
			const t = requireTarget();
			const result = await withReconnect(() => runRemoteGrep(t, localCwd, params, signal));
			return { content: [{ type: "text" as const, text: result.text }], details: result.details };
		},
	});

	// --- remote write ---
	pi.registerTool({
		...localWrite,
		name: "ssh_write",
		label: "ssh_write",
		description: "Write a file on the active SSH remote. Relative paths resolve under the remote cwd; use local write for local files.",
		promptSnippet: "Write files on the active SSH remote",
		promptGuidelines: ["Use ssh_write only for remote files. Use write for local files in the current local workspace."],
		renderCall(args: any, theme: any, context: any) {
			return sshTitle("write", accentRemotePath(args?.path, theme), theme, context);
		},
		async execute(id, params, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const tool = createWriteTool(localCwd, { operations: createRemoteWriteOps(t, localCwd) });
			return withReconnect(() => tool.execute(id, params, signal, onUpdate, _ctx));
		},
	});

	// --- remote secret write (value never enters the tool-call record) ---
	pi.registerTool({
		name: "ssh_secret_write",
		label: "ssh_secret_write",
		description: "Write a secret to a remote file without logging the value. Reads from a LOCAL env var or file and streams over stdin; use for API keys/tokens/credentials.",
		promptSnippet: "Write a secret to a remote file without logging its value",
		promptGuidelines: [
			"Use ssh_secret_write for API keys/tokens/credentials so the value never enters the tool-call record. Pass the LOCAL env var name (fromEnv) or local file path (fromFile), never the secret literal. Do not paste secrets into ssh_write/ssh_bash.",
		],
		parameters: Type.Object({
			remotePath: Type.String({ description: "Destination path on the remote (absolute, or relative to remote cwd)" }),
			fromEnv: Type.Optional(Type.String({ description: "Name of a LOCAL environment variable holding the secret value" })),
			fromFile: Type.Optional(Type.String({ description: "Path to a LOCAL file whose contents are the secret value" })),
			mode: Type.Optional(Type.String({ description: "Octal file mode for the remote file (default 600)" })),
			appendNewline: Type.Optional(Type.Boolean({ description: "Append a trailing newline to the value (default false)" })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const src = args?.fromEnv ? `\u2190 $${args.fromEnv}` : args?.fromFile ? `\u2190 ${args.fromFile}` : "";
			const rest = `${accentRemotePath(args?.remotePath, theme)}${src ? ` ${theme.fg("muted", src)}` : ""}`;
			return sshTitle("secret", rest, theme, context);
		},
		async execute(_id, params: { remotePath: string; fromEnv?: string; fromFile?: string; mode?: string; appendNewline?: boolean }) {
			const t = requireTarget();
			if ((params.fromEnv ? 1 : 0) + (params.fromFile ? 1 : 0) !== 1) {
				throw new Error("ssh_secret_write requires exactly one of fromEnv or fromFile");
			}
			let value: Buffer;
			if (params.fromEnv) {
				const v = process.env[params.fromEnv];
				if (v === undefined) throw new Error(`Local environment variable not set: ${params.fromEnv}`);
				value = Buffer.from(v);
			} else {
				value = readFileSync(resolve(localCwd, params.fromFile!));
			}
			if (params.appendNewline) value = Buffer.concat([value, Buffer.from("\n")]);
			const mode = params.mode?.trim() || "600";
			if (!/^[0-7]{3,4}$/.test(mode)) throw new Error(`Invalid octal mode: ${mode}`);
			const remotePath = toRemotePath(params.remotePath, localCwd, t.remoteCwd);
			const q = shQuote(remotePath);
			// umask 077 makes the create restrictive from the first byte; chmod sets the
			// final mode. The secret arrives only on stdin, never in argv.
			const cmd = `mkdir -p -- "$(dirname ${q})" && ( umask 077 && cat > ${q} ) && chmod ${mode} ${q}`;
			const r = await withReconnect(() => withFileLock(`${t.remote}:${remotePath}`, () => runRemoteCommand(t, cmd, { stdin: value })));
			if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
			return { content: [{ type: "text" as const, text: `Wrote secret (${value.length} bytes) to ${t.remote}:${remotePath} (mode ${mode}). Value not recorded.` }] };
		},
	});

	// --- remote edit (real remote in-place patch when python3 is available) ---
	pi.registerTool({
		...localEdit,
		name: "ssh_edit",
		label: "ssh_edit",
		description: "Edit a file on the active SSH remote. Relative paths resolve under the remote cwd; use local edit for local files.",
		promptSnippet: "Edit files on the active SSH remote",
		promptGuidelines: ["Use ssh_edit only for remote files. Use edit for local files in the current local workspace."],
		renderCall(args: any, theme: any, context: any) {
			const n = Array.isArray(args?.edits) ? args.edits.length : 0;
			const count = theme.fg("muted", `(${n} edit${n === 1 ? "" : "s"})`);
			return sshTitle("edit", `${accentRemotePath(args?.path, theme)} ${count}`, theme, context);
		},
		renderResult(result: any, _options: any, theme: any, context: any) {
			return renderEditDiffResult(result, theme, context);
		},
		async execute(id, params: { path: string; edits: Array<{ oldText: string; newText: string }> }, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const remotePath = toRemotePath(params.path, localCwd, t.remoteCwd);

			if (t.hasPython) {
				try {
					const res = await withReconnect(() => remotePatchEdit(t, remotePath, params.edits, signal));
					return {
						content: [
							{
								type: "text" as const,
								text: `Edited remote ${params.path} on ${t.remote}:\n${res.diff || "(no textual changes)"}`,
							},
						],
						details: { diff: res.diff, patch: res.patch, firstChangedLine: res.firstChangedLine },
					};
				} catch (e) {
					// If python3 vanished mid-session, fall through to rewrite path.
					if (!/python3: (not found|command not found)/.test(String(e))) {
						throw e;
					}
					t.hasPython = false;
				}
			}

			// Fallback: read-rewrite-write via the edit tool's own diff engine.
			const tool = createEditTool(localCwd, { operations: createRemoteEditOps(t, localCwd, false) });
			return withReconnect(() => withFileLock(`${t.remote}:${remotePath}`, () => tool.execute(id, params, signal, onUpdate, _ctx)));
		},
	});
}
