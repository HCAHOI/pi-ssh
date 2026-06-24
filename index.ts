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

import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { dirname, isAbsolute, join, posix, relative, resolve, sep } from "node:path";
import { randomBytes } from "node:crypto";
import { Type } from "typebox";
import type { AgentToolUpdateCallback, ExtensionAPI } from "@earendil-works/pi-coding-agent";
import {
	type BashOperations,
	createBashTool,
	createEditTool,
	createFindTool,
	createGrepTool,
	createLsTool,
	createReadTool,
	createWriteTool,
	type EditOperations,
	type FindOperations,
	type LsOperations,
	type ReadOperations,
	type WriteOperations,
} from "@earendil-works/pi-coding-agent";

// ---------------------------------------------------------------------------
// SSH plumbing
// ---------------------------------------------------------------------------

interface SshTarget {
	remote: string;
	remoteCwd: string;
	socket: string;
	hasPython: boolean;
	sshOptions: string[];
	/** Shell prefix applied before every ssh_bash / ssh_process command (e.g. venv activation). */
	defaultCommandPrefix?: string;
	/** Environment exported before every ssh_bash / ssh_process command. */
	defaultEnv?: Record<string, string>;
	/** Resolved /ssh argument string used to open this connection (for `/ssh save`). */
	originArg?: string;
}

// --- ssh_process background notification poller (Phase 1) ---
const POLL_INTERVAL_MS = 3000;

interface WatchSpec {
	pattern: string;
	stream?: "stdout" | "stderr" | "both";
	repeat?: boolean;
}

interface WatchState {
	re: RegExp;
	pattern: string;
	stream: "stdout" | "stderr" | "both";
	repeat: boolean;
	fired: boolean;
}

interface PollerState {
	procId: string;
	name: string;
	dir: string;
	target: SshTarget;
	alertOnSuccess: boolean;
	alertOnFailure: boolean;
	alertOnKill: boolean;
	watches: WatchState[];
	off: { stdout: number; stderr: number };
	timer: NodeJS.Timeout | null;
	busy: boolean;
	finished: boolean;
}

interface Activation {
	commandPrefix?: string;
	env?: Record<string, string>;
}

interface RunResult {
	code: number | null;
	signal: NodeJS.Signals | null;
	stdout: Buffer;
	stderr: Buffer;
	timedOut: boolean;
}

interface RunOptions {
	timeout?: number;
	stdin?: string | Buffer;
	signal?: AbortSignal;
	/** Use a login shell (bash -lc) so profile/activation apply. Default true.
	 * Set false for internal, machine-parsed reads (status/log deltas) so a
	 * remote profile banner cannot contaminate stdout or byte offsets. */
	login?: boolean;
}

function runSsh(args: string[], opts?: RunOptions): Promise<RunResult> {
	return new Promise((resolve, reject) => {
		if (opts?.signal?.aborted) {
			reject(new Error("aborted"));
			return;
		}
		const child = spawn("ssh", args, { stdio: [opts?.stdin === undefined ? "ignore" : "pipe", "pipe", "pipe"] });
		const out: Buffer[] = [];
		const err: Buffer[] = [];
		let settled = false;
		let timedOut = false;
		child.stdout.on("data", (d) => out.push(d));
		child.stderr.on("data", (d) => err.push(d));
		let timer: NodeJS.Timeout | undefined;
		if (opts?.timeout) {
			timer = setTimeout(() => {
				timedOut = true;
				child.kill();
			}, opts.timeout * 1000);
		}
		const onAbort = () => child.kill();
		opts?.signal?.addEventListener("abort", onAbort, { once: true });
		child.on("error", (e) => {
			if (settled) return;
			settled = true;
			if (timer) clearTimeout(timer);
			opts?.signal?.removeEventListener("abort", onAbort);
			reject(e);
		});
		child.on("close", (code, closeSignal) => {
			if (settled) return;
			settled = true;
			if (timer) clearTimeout(timer);
			opts?.signal?.removeEventListener("abort", onAbort);
			resolve({ code, signal: closeSignal, stdout: Buffer.concat(out), stderr: Buffer.concat(err), timedOut });
		});
		if (opts?.stdin !== undefined) {
			child.stdin.on("error", (e) => {
				if (settled) return;
				settled = true;
				if (timer) clearTimeout(timer);
				opts?.signal?.removeEventListener("abort", onAbort);
				reject(e);
			});
			child.stdin.end(opts.stdin);
		}
	});
}

function shQuote(value: string): string {
	return `'${value.replace(/'/g, `'\\''`)}'`;
}

function stripTrailingSlash(value: string): string {
	return value.length > 1 ? value.replace(/\/+$/, "") : value;
}

const fileLocks = new Map<string, Promise<void>>();

async function withFileLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
	const previous = fileLocks.get(key) ?? Promise.resolve();
	const run = previous.catch(() => undefined).then(fn);
	const tail = run.then(() => undefined, () => undefined);
	fileLocks.set(key, tail);
	try {
		return await run;
	} finally {
		if (fileLocks.get(key) === tail) {
			fileLocks.delete(key);
		}
	}
}

function toRemotePath(path: string, localCwd: string, remoteCwd: string): string {
	const normalizedRemoteCwd = stripTrailingSlash(remoteCwd);
	if (path === normalizedRemoteCwd || path.startsWith(`${normalizedRemoteCwd}/`)) {
		const normalizedPath = posix.normalize(path);
		if (normalizedPath === normalizedRemoteCwd || normalizedPath.startsWith(`${normalizedRemoteCwd}/`)) {
			return normalizedPath;
		}
		throw new Error(`SSH path mapping refused remote path outside cwd: ${path}`);
	}

	const localRoot = resolve(localCwd);
	const absolutePath = isAbsolute(path) ? resolve(path) : resolve(localRoot, path);
	const relativePath = relative(localRoot, absolutePath);
	if (relativePath === "") {
		return normalizedRemoteCwd;
	}
	if (!relativePath.startsWith("..") && !isAbsolute(relativePath)) {
		return `${normalizedRemoteCwd}/${relativePath.split(sep).join("/")}`;
	}
	if (isAbsolute(path)) {
		return posix.normalize(path);
	}
	throw new Error(`SSH path mapping refused path outside workspace: ${path}`);
}

/** Common ssh args for every invocation: BatchMode + ControlMaster multiplexing. */
function baseSshOptions(socket: string): string[] {
	return [
		"-o",
		"BatchMode=yes",
		"-o",
		"ConnectTimeout=10",
		"-o",
		"ControlMaster=auto",
		"-o",
		`ControlPath=${socket}`,
		"-o",
		"ControlPersist=600",
	];
}

function sshConnArgs(t: SshTarget): string[] {
	return [...t.sshOptions, ...baseSshOptions(t.socket), "--", t.remote];
}

function remoteShell(command: string, login = true): string {
	return `bash ${login ? "-lc" : "-c"} ${shQuote(command)}`;
}

function sshFailureMessage(r: RunResult): string {
	if (r.timedOut) return "SSH timed out";
	if (r.signal) return `SSH terminated by signal ${r.signal}`;
	return `SSH failed (${r.code ?? "unknown"})`;
}

function isRetryableSshFailure(r: RunResult): boolean {
	if (r.code !== 255 || r.timedOut || r.signal) return false;
	const message = `${r.stderr.toString()}\n${r.stdout.toString()}`;
	return /mux_client_request_session|Control socket connect|Connection reset|Connection closed|Broken pipe|Connection timed out|Connection refused|No route to host|kex_exchange_identification/i.test(message);
}

async function runRemoteCommand(t: SshTarget, command: string, opts?: RunOptions): Promise<RunResult> {
	const shell = remoteShell(command, opts?.login !== false);
	let r = await runSsh([...sshConnArgs(t), shell], opts);
	if (isRetryableSshFailure(r) && !opts?.signal?.aborted) {
		await closeMaster(t);
		r = await runSsh([...sshConnArgs(t), shell], opts);
	}
	return r;
}

async function sshExec(t: SshTarget, command: string): Promise<Buffer> {
	const r = await runRemoteCommand(t, command);
	if (r.code !== 0) {
		throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
	}
	return r.stdout;
}

async function probePython(t: SshTarget): Promise<boolean> {
	const r = await runRemoteCommand(t, "python3 --version");
	return r.code === 0;
}

async function closeMaster(t: SshTarget): Promise<void> {
	// Best-effort teardown; ignore errors.
	await runSsh(["-O", "exit", ...t.sshOptions, ...baseSshOptions(t.socket), "--", t.remote], { timeout: 5 }).catch(() => {});
}

// ---------------------------------------------------------------------------
// Remote operation factories
// ---------------------------------------------------------------------------

function createRemoteReadOps(t: SshTarget, localCwd: string): ReadOperations {
	const toRemote = (p: string) => toRemotePath(p, localCwd, t.remoteCwd);
	return {
		readFile: (p) => sshExec(t, `cat -- ${shQuote(toRemote(p))}`),
		access: async (p) => {
			const rp = toRemote(p);
			// Distinct exit codes let us surface a useful reason instead of an empty
			// "SSH failed (1)": missing vs directory vs unreadable.
			const q = shQuote(rp);
			const r = await runRemoteCommand(t, `if [ ! -e ${q} ]; then exit 11; elif [ -d ${q} ]; then exit 12; elif [ ! -r ${q} ]; then exit 13; fi`);
			if (r.code === 11) throw new Error(`No such file or directory: ${rp}`);
			if (r.code === 12) throw new Error(`Is a directory: ${rp}`);
			if (r.code === 13) throw new Error(`Permission denied: ${rp}`);
			if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || rp}`);
		},
		detectImageMimeType: async (p) => {
			try {
				const r = await sshExec(t, `file --mime-type -b -- ${shQuote(toRemote(p))}`);
				const m = r.toString().trim();
				return ["image/jpeg", "image/png", "image/gif", "image/webp"].includes(m) ? m : null;
			} catch {
				return null;
			}
		},
	};
}

function createRemoteWriteOps(t: SshTarget, localCwd: string, lockWrites = true): WriteOperations {
	const toRemote = (p: string) => toRemotePath(p, localCwd, t.remoteCwd);
	return {
		writeFile: async (p, content) => {
			const remotePath = toRemote(p);
			const write = async () => {
				const r = await runRemoteCommand(t, `cat > ${shQuote(remotePath)}`, { stdin: Buffer.from(content) });
				if (r.code !== 0) {
					throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
				}
			};
			if (lockWrites) {
				await withFileLock(`${t.remote}:${remotePath}`, write);
			} else {
				await write();
			}
		},
		mkdir: (dir) =>
			sshExec(t, `mkdir -p -- ${shQuote(toRemote(dir))}`).then(() => {
				/* void */
			}),
	};
}

function createRemoteEditOps(t: SshTarget, localCwd: string, lockWrites = true): EditOperations {
	const r = createRemoteReadOps(t, localCwd);
	const w = createRemoteWriteOps(t, localCwd, lockWrites);
	return { readFile: r.readFile, access: r.access, writeFile: w.writeFile };
}

function createRemoteLsOps(t: SshTarget, localCwd: string): LsOperations {
	const toRemote = (p: string) => toRemotePath(p, localCwd, t.remoteCwd);
	return {
		exists: async (p) => {
			const r = await runRemoteCommand(t, `test -e ${shQuote(toRemote(p))}`);
			return r.code === 0;
		},
		stat: async (p) => {
			const r = await runRemoteCommand(t, `test -d ${shQuote(toRemote(p))}`);
			return { isDirectory: () => r.code === 0 };
		},
		readdir: async (p) => {
			const script = "import json, os, sys; print(json.dumps(os.listdir(sys.argv[1])))";
			const r = await runRemoteCommand(t, `python3 -c ${shQuote(script)} ${shQuote(toRemote(p))}`);
			if (r.code !== 0) throw new Error(r.stderr.toString().trim() || sshFailureMessage(r));
			return JSON.parse(r.stdout.toString()) as string[];
		},
	};
}

function createRemoteFindOps(t: SshTarget, localCwd: string): FindOperations {
	const toRemote = (p: string) => toRemotePath(p, localCwd, t.remoteCwd);
	return {
		exists: async (p) => {
			const r = await runRemoteCommand(t, `test -e ${shQuote(toRemote(p))}`);
			return r.code === 0;
		},
		glob: async (pattern, cwd, options) => {
			const script = `
import fnmatch, json, os, sys
payload = json.load(sys.stdin)
root = payload['root']
pattern = payload['pattern']
limit = int(payload['limit'])
results = []
for dirpath, dirnames, filenames in os.walk(root):
    dirnames[:] = [d for d in dirnames if d not in {'.git', 'node_modules'}]
    for name in filenames:
        path = os.path.join(dirpath, name)
        rel = os.path.relpath(path, root).replace(os.sep, '/')
        if fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(name, pattern):
            results.append(path)
            if len(results) >= limit:
                print(json.dumps(results))
                sys.exit(0)
print(json.dumps(results))
`;
			const payload = JSON.stringify({ root: toRemote(cwd), pattern, limit: options.limit });
			const r = await runRemoteCommand(t, `python3 -c ${shQuote(script)}`, { stdin: payload });
			if (r.code !== 0) throw new Error(r.stderr.toString().trim() || sshFailureMessage(r));
			return JSON.parse(r.stdout.toString()) as string[];
		},
	};
}

function grepArgs(args: string[]): string {
	return args.map(shQuote).join(" ");
}

async function runRemoteGrep(
	t: SshTarget,
	localCwd: string,
	params: { pattern: string; path?: string; glob?: string; ignoreCase?: boolean; literal?: boolean; context?: number; limit?: number },
	signal?: AbortSignal,
): Promise<{ text: string; details?: Record<string, unknown> }> {
	const searchPath = toRemotePath(params.path || ".", localCwd, t.remoteCwd);
	const limit = Math.max(1, Math.floor(params.limit ?? 100));
	const rgArgs = ["--line-number", "--color=never", "--hidden", "--no-heading", "--glob", "!.git/**", "--glob", "!node_modules/**"];
	if (params.ignoreCase) rgArgs.push("--ignore-case");
	if (params.literal) rgArgs.push("--fixed-strings");
	if (params.context && params.context > 0) rgArgs.push("--context", String(Math.floor(params.context)));
	if (params.glob) rgArgs.push("--glob", params.glob);
	const rgCommand = `rg ${grepArgs([...rgArgs, "--", params.pattern, searchPath])}`;
	const grepFallbackArgs = ["-RIn"];
	if (params.ignoreCase) grepFallbackArgs.push("-i");
	if (params.literal) grepFallbackArgs.push("-F");
	if (params.context && params.context > 0) grepFallbackArgs.push(`-C${Math.floor(params.context)}`);
	const grepCommand = `grep ${grepArgs([...grepFallbackArgs, "--exclude-dir=.git", "--exclude-dir=node_modules", "-e", params.pattern, searchPath])}`;
	const searchCommand = `if command -v rg >/dev/null 2>&1; then ${rgCommand}; else ${grepCommand}; fi`;
	const command = `tmp=$(mktemp); if (${searchCommand}) > "$tmp"; then status=0; else status=$?; fi; if [ "$status" -ne 0 ] && [ "$status" -ne 1 ]; then rm -f "$tmp"; exit "$status"; fi; head -n ${limit} "$tmp"; rm -f "$tmp"`;
	const r = await runRemoteCommand(t, command, { signal });
	if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
	const text = r.stdout.toString().trimEnd();
	if (!text) return { text: "No matches found" };
	const maxBytes = 50 * 1024;
	const truncated = Buffer.byteLength(text) > maxBytes;
	return {
		text: truncated ? `${Buffer.from(text).subarray(0, maxBytes).toString()}\n\n[Truncated at ${maxBytes} bytes]` : text,
		details: { matchLimitReached: limit, truncation: truncated ? { truncated: true, maxBytes } : undefined },
	};
}

function createRemoteBashOps(t: SshTarget, localCwd: string, opts?: { tty?: boolean }): BashOperations {
	const toRemote = (p: string) => toRemotePath(p, localCwd, t.remoteCwd);
	// -tt must precede the destination in ssh args, so we splice it into the
	// options segment rather than appending after sshConnArgs.
	const connArgs = opts?.tty ? [...t.sshOptions, "-tt", ...baseSshOptions(t.socket), "--", t.remote] : sshConnArgs(t);
	return {
		exec: (command, cwd, { onData, signal, timeout }) =>
			new Promise((resolve, reject) => {
				const cmd = `cd -- ${shQuote(toRemote(cwd))} && ${command}`;
				const attempt = (allowRetry: boolean) => {
					if (signal?.aborted) {
						reject(new Error("aborted"));
						return;
					}
					const child = spawn("ssh", [...connArgs, remoteShell(cmd)], { stdio: ["ignore", "pipe", "pipe"] });
					const out: Buffer[] = [];
					const err: Buffer[] = [];
					let timedOut = false;
					const timer = timeout
						? setTimeout(() => {
								timedOut = true;
								child.kill();
							}, timeout * 1000)
						: undefined;
					child.stdout.on("data", (d) => {
						out.push(d);
						onData(d);
					});
					child.stderr.on("data", (d) => {
						err.push(d);
						onData(d);
					});
					child.on("error", (e) => {
						if (timer) clearTimeout(timer);
						reject(e);
					});
					const onAbort = () => child.kill();
					signal?.addEventListener("abort", onAbort, { once: true });
					child.on("close", async (code, closeSignal) => {
						if (timer) clearTimeout(timer);
						signal?.removeEventListener("abort", onAbort);
						const result: RunResult = { code, signal: closeSignal, stdout: Buffer.concat(out), stderr: Buffer.concat(err), timedOut };
						if (allowRetry && isRetryableSshFailure(result) && !signal?.aborted) {
							await closeMaster(t);
							attempt(false);
							return;
						}
						if (signal?.aborted) reject(new Error("aborted"));
						else if (timedOut) reject(new Error(`timeout:${timeout}`));
						else if (code === null) reject(new Error(`terminated by signal ${closeSignal ?? "unknown"}`));
						else resolve({ exitCode: code });
					});
				};
				attempt(true);
			}),
	};
}

// ---------------------------------------------------------------------------
// Real remote in-place edit via python3
// ---------------------------------------------------------------------------

// python3 source that patches a file in place and prints a unified diff.
// Reads the path and edits as JSON from stdin to avoid command-length limits.
const PATCH_SCRIPT = `
import difflib, json, sys
payload = json.load(sys.stdin)
p = payload['path']
edits = payload['edits']
with open(p, 'r', encoding='utf-8') as f:
    c = f.read()
regions = []
for i, edit in enumerate(edits):
    old = edit['oldText']
    new = edit['newText']
    n = c.count(old)
    if n != 1:
        sys.stderr.write('edit %d oldText found %d times (expected 1)\\n' % (i + 1, n))
        sys.exit(2)
    start = c.find(old)
    regions.append((start, start + len(old), new))
regions.sort(key=lambda r: r[0])
for prev, cur in zip(regions, regions[1:]):
    if prev[1] > cur[0]:
        sys.stderr.write('edits overlap; refusing partial write\\n')
        sys.exit(2)
out = []
pos = 0
for start, end, new in regions:
    out.append(c[pos:start])
    out.append(new)
    pos = end
out.append(c[pos:])
nc = ''.join(out)
with open(p, 'w', encoding='utf-8') as f:
    f.write(nc)
sys.stdout.write(''.join(difflib.unified_diff(
    c.splitlines(keepends=True), nc.splitlines(keepends=True),
    fromfile=p, tofile=p)))
`;

interface EditResult {
	diff: string;
	patch: string;
	firstChangedLine?: number;
}

async function remotePatchEdit(
	t: SshTarget,
	remotePath: string,
	edits: Array<{ oldText: string; newText: string }>,
	signal?: AbortSignal,
): Promise<EditResult> {
	const scriptB64 = Buffer.from(PATCH_SCRIPT).toString("base64");
	const launcher = `python3 -c "import base64;exec(base64.b64decode('${scriptB64}').decode())"`;
	const payload = JSON.stringify({ path: remotePath, edits });
	const r = await withFileLock(`${t.remote}:${remotePath}`, () =>
		runRemoteCommand(t, launcher, { stdin: payload, signal }),
	);
	if (r.code !== 0) {
		const err = r.stderr.toString().trim();
		throw new Error(err || sshFailureMessage(r));
	}

	const diff = r.stdout.toString();
	let firstChangedLine: number | undefined;
	const m = diff.match(/^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@/m);
	if (m) firstChangedLine = parseInt(m[1], 10);

	return { diff, patch: diff, firstChangedLine };
}

// ---------------------------------------------------------------------------
// Connection management
// ---------------------------------------------------------------------------

async function resolveTarget(remote: string, path?: string, sshOptions: string[] = [], activation?: Activation): Promise<SshTarget> {
	const socket = join(tmpdir(), `pi-ssh-${randomBytes(4).toString("hex")}.sock`);
	const remoteCwd = path
		? (await sshExecRaw(socket, remote, sshOptions, `cd -- ${shQuote(path)} && pwd -P`)).toString().trim()
		: (await sshExecRaw(socket, remote, sshOptions, "pwd -P")).toString().trim();
	const t: SshTarget = {
		remote,
		remoteCwd,
		socket,
		hasPython: false,
		sshOptions,
		defaultCommandPrefix: activation?.commandPrefix?.trim() || undefined,
		defaultEnv: activation?.env && Object.keys(activation.env).length ? activation.env : undefined,
	};
	t.hasPython = await probePython(t);
	return t;
}

// sshExec before we have a full target (used during resolve for pwd probe).
async function sshExecRaw(socket: string, remote: string, sshOptions: string[], command: string): Promise<Buffer> {
	const r = await runSsh([
		...sshOptions,
		...baseSshOptions(socket),
		"--",
		remote,
		remoteShell(command),
	]);
	if (r.code !== 0) {
		throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
	}
	return r.stdout;
}

// ---------------------------------------------------------------------------
// Extension
// ---------------------------------------------------------------------------

export default function (pi: ExtensionAPI) {
	pi.registerFlag("ssh", { description: "SSH remote, e.g. user@host[:/path], -i key user@host, optionally with --activate <cmd> / --env K=V", type: "string" });

	const localCwd = process.cwd();
	const localRead = createReadTool(localCwd);
	const localWrite = createWriteTool(localCwd);
	const localEdit = createEditTool(localCwd);
	const localFind = createFindTool(localCwd);
	const localGrep = createGrepTool(localCwd);
	const localLs = createLsTool(localCwd);
	const localBash = createBashTool(localCwd);

	let target: SshTarget | null = null;
	const get = () => target;

	function statusLabel(t: SshTarget | null): string {
		if (!t) return "";
		const act = t.defaultCommandPrefix ? ` ⚡${t.defaultCommandPrefix.length > 28 ? `${t.defaultCommandPrefix.slice(0, 27)}…` : t.defaultCommandPrefix}` : "";
		return `SSH: ${t.remote}:${t.remoteCwd}${t.hasPython ? "" : " (no python3)"}${act}`;
	}

	function refreshStatus(ctx: any) {
		if (!ctx?.ui) return;
		const label = statusLabel(target);
		ctx.ui.setStatus("ssh", label ? ctx.ui.theme.fg("accent", label) : "");
	}

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
		stopAllPollers();
		if (target) await closeMaster(target);
		target = next;
		return next;
	}

	async function disconnect(): Promise<void> {
		stopAllPollers();
		if (target) await closeMaster(target);
		target = null;
	}

	// --- ssh_process background notification poller (Phase 1) ---
	const pollers = new Map<string, PollerState>();

	function stopPoller(procId: string): void {
		const p = pollers.get(procId);
		if (!p) return;
		if (p.timer) clearInterval(p.timer);
		p.timer = null;
		pollers.delete(procId);
	}

	function stopAllPollers(): void {
		for (const id of [...pollers.keys()]) stopPoller(id);
	}

	function statusCmd(dir: string): string {
		return `d=${shQuote(dir)}; if [ ! -d "$d" ]; then printf 'gone\\t\\n'; else pid=$(cat "$d/pid" 2>/dev/null || true); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then printf 'running\\t\\n'; else code=$(cat "$d/exit_code" 2>/dev/null || true); printf 'done\\t%s\\n' "$code"; fi; fi`;
	}

	function neededStreams(p: PollerState): Array<"stdout" | "stderr"> {
		const set = new Set<"stdout" | "stderr">();
		for (const w of p.watches) {
			if (w.stream === "stdout" || w.stream === "both") set.add("stdout");
			if (w.stream === "stderr" || w.stream === "both") set.add("stderr");
		}
		return [...set];
	}

	// Read new complete lines appended to a log since the last byte offset. We only
	// advance the offset to the last newline, so partial lines (and multibyte chars
	// straddling the boundary) are re-read intact next tick. Survives Mac sleep:
	// offsets are byte positions in the remote file, so nothing logged while asleep
	// is lost — it is swept on the first tick after wake.
	async function fetchDeltaLines(p: PollerState, stream: "stdout" | "stderr"): Promise<string[]> {
		const file = `${p.dir}/${stream}.log`;
		const start = p.off[stream];
		const r = await runRemoteCommand(p.target, `tail -c +${start + 1} -- ${shQuote(file)} 2>/dev/null || true`, { timeout: 20, login: false });
		if (r.code !== 0) return [];
		const buf = r.stdout;
		if (buf.length === 0) return [];
		const lastNl = buf.lastIndexOf(0x0a);
		if (lastNl === -1) return []; // no complete line yet
		p.off[stream] = start + lastNl + 1;
		return buf.subarray(0, lastNl).toString("utf8").split("\n");
	}

	function emit(content: string, details: Record<string, unknown>): void {
		pi.sendMessage(
			{ customType: "ssh-process", content, display: true, details },
			{ triggerTurn: true, deliverAs: "followUp" },
		);
	}

	function runWatches(p: PollerState, stream: "stdout" | "stderr", lines: string[]): void {
		for (const w of p.watches) {
			if (w.fired && !w.repeat) continue;
			if (w.stream !== "both" && w.stream !== stream) continue;
			for (const line of lines) {
				if (!w.re.test(line)) continue;
				emit(`🔔 ssh_process "${p.name}" (${p.procId}) matched /${w.pattern}/ on ${stream}:\n${line}`, {
					kind: "watch",
					procId: p.procId,
					name: p.name,
					pattern: w.pattern,
					stream,
					line,
				});
				if (!w.repeat) {
					w.fired = true;
					break;
				}
			}
		}
	}

	async function sweepWatches(p: PollerState): Promise<void> {
		for (const stream of neededStreams(p)) {
			const lines = await fetchDeltaLines(p, stream);
			if (lines.length) runWatches(p, stream, lines);
		}
	}

	async function fireCompletion(p: PollerState, codeStr: string): Promise<void> {
		const code = codeStr === "" ? null : Number.parseInt(codeStr, 10);
		let outcome: "success" | "failure" | "killed";
		if (code === 0) outcome = "success";
		else if (code === null) outcome = "killed"; // exit_code absent => SIGKILL (EXIT trap never ran)
		else if (code >= 128) outcome = "killed"; // 128 + signal
		else outcome = "failure";
		const want = outcome === "success" ? p.alertOnSuccess : outcome === "killed" ? p.alertOnKill : p.alertOnFailure;
		if (!want) return;
		const tailCmd = `d=${shQuote(p.dir)}; echo '--- stdout (tail) ---'; tail -n 15 "$d/stdout.log" 2>/dev/null; echo '--- stderr (tail) ---'; tail -n 15 "$d/stderr.log" 2>/dev/null`;
		const tr = await runRemoteCommand(p.target, tailCmd, { timeout: 20, login: false }).catch(() => null);
		const tail = tr && tr.code === 0 ? tr.stdout.toString().trimEnd() : "";
		const emoji = outcome === "success" ? "✅" : outcome === "killed" ? "⛔" : "❌";
		const codeLabel = code === null ? "" : ` (exit ${code})`;
		emit(`${emoji} ssh_process "${p.name}" (${p.procId}) ${outcome}${codeLabel}.${tail ? `\n${tail}` : ""}`, {
			kind: "completion",
			procId: p.procId,
			name: p.name,
			outcome,
			code,
		});
	}

	async function tick(p: PollerState): Promise<void> {
		if (p.busy || p.finished) return;
		p.busy = true;
		try {
			const r = await runRemoteCommand(p.target, statusCmd(p.dir), { timeout: 20, login: false });
			if (r.code !== 0) return; // transient (e.g. stale socket post-wake); retry next tick
			// login:false avoids profile banners, but defensively take the last
			// non-empty line rather than the first.
			const out = r.stdout.toString().trim();
			if (!out) return;
			const [status, code = ""] = out.split("\n").pop()!.split("\t");
			if (status === "gone") {
				// dir removed (e.g. ssh_process clear / manual rm): orphaned poller,
				// stop silently without a spurious completion notification.
				p.finished = true;
				stopPoller(p.procId);
				return;
			}
			if (p.watches.length) await sweepWatches(p);
			if (status === "running") return;
			if (p.watches.length) await sweepWatches(p); // final sweep before we stop
			p.finished = true;
			await fireCompletion(p, code);
			stopPoller(p.procId);
		} catch {
			// Swallow: a single failed tick must never escape setInterval and kill the
			// poller. The next tick retries (critical for Mac sleep/wake recovery).
		} finally {
			p.busy = false;
		}
	}

	function startPoller(args: {
		procId: string;
		name: string;
		dir: string;
		target: SshTarget;
		alertOnSuccess: boolean;
		alertOnFailure: boolean;
		alertOnKill: boolean;
		watches: WatchState[];
	}): void {
		if (!args.alertOnSuccess && !args.alertOnFailure && !args.alertOnKill && args.watches.length === 0) return;
		const state: PollerState = {
			...args,
			off: { stdout: 0, stderr: 0 },
			timer: null,
			busy: false,
			finished: false,
		};
		pollers.set(state.procId, state);
		state.timer = setInterval(() => {
			void tick(state);
		}, POLL_INTERVAL_MS);
		state.timer.unref?.();
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

	function buildEnvExports(env: Record<string, string> | undefined): string[] {
		if (!env) return [];
		return Object.entries(env).map(([key, value]) => {
			if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
				throw new Error(`Invalid environment variable name for ssh_bash: ${key}`);
			}
			return `export ${key}=${shQuote(value)}`;
		});
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

	// --- agent-callable connection management ---
	pi.registerTool({
		name: "ssh_connect",
		label: "ssh_connect",
		description: "Connect, reconnect, or switch the active SSH remote for ssh_* tools. Accepts the same target syntax as /ssh, e.g. '-i /path/key.pem root@host[:/absolute/path]'. Optional '--activate <cmd>' sets a shell prefix (e.g. venv activation) and repeatable '--env KEY=VALUE' sets environment, both applied to every ssh_bash and ssh_process. Local tools remain local.",
		promptSnippet: "Connect or switch the active SSH remote used by ssh_* tools",
		promptGuidelines: [
			"Use ssh_connect when the user asks to connect, disconnect, or switch SSH servers from within the agent session.",
			"After ssh_connect succeeds, use ssh_bash/ssh_read/ssh_write/ssh_edit for remote operations; keep read/write/edit/bash for local work.",
		],
		parameters: Type.Object({
			target: Type.String({ description: "SSH target, e.g. '-i /path/key.pem root@host', 'user@host:/absolute/path', a saved profile '@name', or with persistent setup: 'root@host:/work --activate \"source .venv/bin/activate\" --env PYTHONPATH=/src'" }),
		}),
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
		async execute(_id, _params, _signal, _onUpdate, ctx) {
			await disconnect();
			refreshStatus(ctx);
			return { content: [{ type: "text" as const, text: "SSH disconnected. Local tools remain local." }] };
		},
	});

	pi.registerTool({
		name: "ssh_status",
		label: "ssh_status",
		description: "Show the active SSH remote used by ssh_* tools.",
		promptSnippet: "Show active SSH connection status",
		parameters: Type.Object({}),
		async execute() {
			return { content: [{ type: "text" as const, text: target ? connectedText(target) : "SSH: not connected" }] };
		},
	});

	// --- remote read ---
	pi.registerTool({
		...localRead,
		name: "ssh_read",
		label: "ssh_read",
		description: `Read a file from the active SSH remote. Relative paths are resolved under the remote cwd. Use local read for local files. ${localRead.description}`,
		promptSnippet: "Read files from the active SSH remote",
		promptGuidelines: ["Use ssh_read only for remote files. Use read for local files in the current local workspace."],
		async execute(id, params, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const tool = createReadTool(localCwd, { operations: createRemoteReadOps(t, localCwd) });
			return tool.execute(id, params, signal, onUpdate, _ctx);
		},
	});

	// --- remote ls ---
	pi.registerTool({
		...localLs,
		name: "ssh_ls",
		label: "ssh_ls",
		description: `List directory contents on the active SSH remote. Relative paths are resolved under the remote cwd. Use local ls/find for local files. ${localLs.description}`,
		promptSnippet: "List directories on the active SSH remote",
		promptGuidelines: ["Use ssh_ls only for remote directory listings. Use local ls/find tools for local workspace exploration."],
		async execute(id, params, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const tool = createLsTool(localCwd, { operations: createRemoteLsOps(t, localCwd) });
			return tool.execute(id, params, signal, onUpdate, _ctx);
		},
	});

	// --- remote find ---
	pi.registerTool({
		...localFind,
		name: "ssh_find",
		label: "ssh_find",
		description: `Find files on the active SSH remote. Relative paths are resolved under the remote cwd. Use local find for local files. ${localFind.description}`,
		promptSnippet: "Find files on the active SSH remote",
		promptGuidelines: ["Use ssh_find only for remote file discovery. Use local find/fffind for local workspace files."],
		async execute(id, params, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const tool = createFindTool(localCwd, { operations: createRemoteFindOps(t, localCwd) });
			return tool.execute(id, params, signal, onUpdate, _ctx);
		},
	});

	// --- remote grep ---
	pi.registerTool({
		...localGrep,
		name: "ssh_grep",
		label: "ssh_grep",
		description: `Search file contents on the active SSH remote. Relative paths are resolved under the remote cwd. Uses remote rg when available, otherwise grep. Use local grep for local files. ${localGrep.description}`,
		promptSnippet: "Search file contents on the active SSH remote",
		promptGuidelines: ["Use ssh_grep only for remote content search. Use local grep/ffgrep for local workspace files."],
		async execute(
			_id,
			params: { pattern: string; path?: string; glob?: string; ignoreCase?: boolean; literal?: boolean; context?: number; limit?: number },
			signal,
		) {
			const t = requireTarget();
			const result = await runRemoteGrep(t, localCwd, params, signal);
			return { content: [{ type: "text" as const, text: result.text }], details: result.details };
		},
	});

	// --- remote write ---
	pi.registerTool({
		...localWrite,
		name: "ssh_write",
		label: "ssh_write",
		description: `Write a file on the active SSH remote. Relative paths are resolved under the remote cwd. Use local write for local files. ${localWrite.description}`,
		promptSnippet: "Write files on the active SSH remote",
		promptGuidelines: ["Use ssh_write only for remote files. Use write for local files in the current local workspace."],
		async execute(id, params, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const tool = createWriteTool(localCwd, { operations: createRemoteWriteOps(t, localCwd) });
			return tool.execute(id, params, signal, onUpdate, _ctx);
		},
	});

	// --- remote edit (real remote in-place patch when python3 is available) ---
	pi.registerTool({
		...localEdit,
		name: "ssh_edit",
		label: "ssh_edit",
		description: `Edit a file on the active SSH remote. Relative paths are resolved under the remote cwd. Use local edit for local files. ${localEdit.description}`,
		promptSnippet: "Edit files on the active SSH remote",
		promptGuidelines: ["Use ssh_edit only for remote files. Use edit for local files in the current local workspace."],
		async execute(id, params: { path: string; edits: Array<{ oldText: string; newText: string }> }, signal, onUpdate, _ctx) {
			const t = requireTarget();
			const remotePath = toRemotePath(params.path, localCwd, t.remoteCwd);

			if (t.hasPython) {
				try {
					const res = await remotePatchEdit(t, remotePath, params.edits, signal);
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
			return withFileLock(`${t.remote}:${remotePath}`, () => tool.execute(id, params, signal, onUpdate, _ctx));
		},
	});

	// --- remote bash ---
	pi.registerTool({
		...localBash,
		name: "ssh_bash",
		label: "ssh_bash",
		description: `Execute a bash command on the active SSH remote in the remote cwd. Supports optional env, commandPrefix, and delaySeconds. Use local bash for local commands. ${localBash.description}`,
		promptSnippet: "Execute bash commands on the active SSH remote",
		promptGuidelines: [
			"Use ssh_bash for remote testing or GPU/server commands. Use bash for local commands and local file operations.",
			"Use ssh_bash.env for remote environment variables such as PYTHONSRC instead of hand-prefixing fragile shell strings.",
			"Use ssh_bash.delaySeconds when the user asks to wait before running a remote check; increase timeout to include the delay.",
		],
		parameters: Type.Object({
			command: Type.String({ description: "Bash command to execute on the active SSH remote" }),
			timeout: Type.Optional(Type.Number({ description: "Timeout in seconds (include delaySeconds in this budget)" })),
			cwd: Type.Optional(Type.String({ description: "Working directory for this command, remote absolute path or local workspace path mapped under the remote cwd" })),
			delaySeconds: Type.Optional(Type.Number({ description: "Seconds to sleep on the remote before running command" })),
			env: Type.Optional(Type.Record(Type.String(), Type.String(), { description: "Environment variables exported before command, e.g. {\"PYTHONSRC\": \"/path\"}" })),
			commandPrefix: Type.Optional(Type.String({ description: "Shell code run before command, e.g. 'source .venv/bin/activate'" })),
			tty: Type.Optional(Type.Boolean({ description: "Allocate a remote pty (ssh -tt) for commands that need a terminal (progress bars, some CLIs). Default false." })),
		}),
		async execute(
			id,
			params: { command: string; timeout?: number; cwd?: string; delaySeconds?: number; env?: Record<string, string>; commandPrefix?: string; tty?: boolean },
			signal,
			onUpdate,
			_ctx,
		) {
			const t = requireTarget();
			const tool = createBashTool(localCwd, { operations: createRemoteBashOps(t, localCwd, { tty: params.tty }) });
			return tool.execute(
				id,
				{ command: buildSshBashCommand(t, params), timeout: params.timeout },
				signal,
				onUpdate,
				_ctx,
			);
		},
	});

	function processRoot(t: SshTarget): string {
		return `${t.remoteCwd}/.pi-ssh-processes`;
	}

	function processRunScript(t: SshTarget, params: {
		command: string;
		cwd?: string;
		env?: Record<string, string>;
		commandPrefix?: string;
	}): string {
		const cwd = params.cwd?.trim() ? toRemotePath(params.cwd, localCwd, t.remoteCwd) : t.remoteCwd;
		const lines = [
			"#!/usr/bin/env bash",
			// Resolve the process dir (where run.sh lives) so we can record the exit code there.
			`__pi_dir="$(cd "$(dirname "\${BASH_SOURCE[0]}")" && pwd)"`,
			// Record the final exit status on ANY exit (normal end, explicit `exit N`,
			// or error under set -e). A trailing capture line would be skipped whenever
			// the command itself calls exit, so an EXIT trap is the robust choice.
			`trap '__pi_rc=$?; printf %s "$__pi_rc" > "$__pi_dir/exit_code"' EXIT`,
			// Record our own pid authoritatively. The launcher seeds pid with `echo $!`,
			// but that is the `setsid` pid which on some systems is a short-lived parent;
			// $$ here is the real job pid that list/output/kill rely on. Atomic mv avoids
			// a torn read against the launcher's seed write.
			`printf %s "$$" > "$__pi_dir/pid.tmp" && mv -f "$__pi_dir/pid.tmp" "$__pi_dir/pid"`,
			`cd -- ${shQuote(cwd)}`,
		];
		lines.push(...buildEnvExports({ ...t.defaultEnv, ...params.env }));
		if (t.defaultCommandPrefix?.trim()) lines.push(t.defaultCommandPrefix);
		if (params.commandPrefix?.trim()) lines.push(params.commandPrefix);
		lines.push(params.command);
		return `${lines.join("\n")}\n`;
	}

	async function runLocalProcess(command: string, args: string[], signal?: AbortSignal, onData?: (chunk: Buffer) => void): Promise<{ code: number | null; stdout: Buffer; stderr: Buffer }> {
		return new Promise((resolve, reject) => {
			if (signal?.aborted) {
				reject(new Error("aborted"));
				return;
			}
			const child = spawn(command, args, { cwd: localCwd, stdio: ["ignore", "pipe", "pipe"] });
			const stdout: Buffer[] = [];
			const stderr: Buffer[] = [];
			child.stdout.on("data", (d) => {
				stdout.push(d);
				onData?.(d);
			});
			child.stderr.on("data", (d) => {
				stderr.push(d);
				onData?.(d);
			});
			const onAbort = () => child.kill();
			signal?.addEventListener("abort", onAbort, { once: true });
			child.on("error", (e) => {
				signal?.removeEventListener("abort", onAbort);
				reject(e);
			});
			child.on("close", (code) => {
				signal?.removeEventListener("abort", onAbort);
				resolve({ code, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr) });
			});
		});
	}

	function rsyncSshCommand(t: SshTarget): string {
		return ["ssh", ...t.sshOptions, ...baseSshOptions(t.socket)].map(shQuote).join(" ");
	}

	function ensureTrailingSlash(path: string): string {
		return path.endsWith("/") ? path : `${path}/`;
	}

	// Stream rsync output to the tool result view as it arrives so large transfers
	// are not a black box. rsync --info=progress2 uses \r to rewrite the progress
	// line; we keep the running buffer and push it on each chunk.
	function rsyncStreamer(onUpdate?: AgentToolUpdateCallback): ((chunk: Buffer) => void) | undefined {
		if (!onUpdate) return undefined;
		// Keep only a trailing window so streaming a huge transfer (one itemized
		// line per file) does not grow unbounded or churn O(n^2) to the UI.
		const MAX = 8 * 1024;
		let acc = "";
		return (chunk: Buffer) => {
			acc += chunk.toString();
			if (acc.length > MAX) acc = `…${acc.slice(-MAX)}`;
			onUpdate({ content: [{ type: "text", text: acc }], details: undefined });
		};
	}

	pi.registerTool({
		name: "ssh_process",
		label: "ssh_process",
		description: "Manage long-running processes on the active SSH remote. Starts commands in the background with logs under remote .pi-ssh-processes/<id>/; supports start/list/output/logs/kill/clear. list and output report the captured exit code once a job finishes; clear prunes finished jobs. On start, alertOnSuccess/alertOnFailure/alertOnKill and logWatches push a notification (re-engaging the agent) when the job ends or a log line matches — so you never poll. Notifications survive Mac sleep: missed completions/log lines are swept on the first reconnect after wake.",
		promptSnippet: "Manage long-running remote SSH processes",
		promptGuidelines: [
			"Use ssh_process start for long-running remote jobs such as training, dev servers, and log tails instead of blocking ssh_bash.",
			"Use ssh_process output to inspect recent stdout/stderr and ssh_process kill to stop remote jobs.",
			"Background jobs push a notification when they finish or a logWatch matches — rely on it instead of polling list/output.",
		],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("start"), Type.Literal("list"), Type.Literal("output"), Type.Literal("logs"), Type.Literal("kill"), Type.Literal("clear")]),
			name: Type.Optional(Type.String({ description: "Friendly process name for start" })),
			command: Type.Optional(Type.String({ description: "Command to start on the remote" })),
			id: Type.Optional(Type.String({ description: "Remote process id returned by start/list" })),
			cwd: Type.Optional(Type.String({ description: "Remote working directory, defaults to remote cwd" })),
			env: Type.Optional(Type.Record(Type.String(), Type.String(), { description: "Environment variables exported before command" })),
			commandPrefix: Type.Optional(Type.String({ description: "Shell code run before command" })),
			lines: Type.Optional(Type.Number({ description: "Number of recent log lines for output (default 80)" })),
			alertOnSuccess: Type.Optional(Type.Boolean({ description: "start: notify when the job exits 0 (default false)" })),
			alertOnFailure: Type.Optional(Type.Boolean({ description: "start: notify when the job exits non-zero (default true)" })),
			alertOnKill: Type.Optional(Type.Boolean({ description: "start: notify when the job is killed by a signal (default false)" })),
			logWatches: Type.Optional(Type.Array(Type.Object({
				pattern: Type.String({ description: "Regex matched per log line" }),
				stream: Type.Optional(Type.Union([Type.Literal("stdout"), Type.Literal("stderr"), Type.Literal("both")], { description: "Which stream to watch (default both)" })),
				repeat: Type.Optional(Type.Boolean({ description: "Fire every match (default false: one-shot)" })),
			}), { description: "start: notify when a log line matches; re-engages the agent" })),
		}),
		async execute(_id, params: {
			action: "start" | "list" | "output" | "logs" | "kill" | "clear";
			name?: string;
			command?: string;
			id?: string;
			cwd?: string;
			env?: Record<string, string>;
			commandPrefix?: string;
			lines?: number;
			alertOnSuccess?: boolean;
			alertOnFailure?: boolean;
			alertOnKill?: boolean;
			logWatches?: WatchSpec[];
		}, signal) {
			const t = requireTarget();
			const root = processRoot(t);
			if (params.action === "start") {
				if (!params.command?.trim()) throw new Error("ssh_process start requires command");
				const procId = `${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
				const dir = `${root}/${procId}`;
				const name = params.name?.trim() || procId;
				const script = processRunScript(t, { command: params.command, cwd: params.cwd, env: params.env, commandPrefix: params.commandPrefix });
				const cmd = [
					`mkdir -p ${shQuote(dir)}`,
					`printf %s ${shQuote(name)} > ${shQuote(`${dir}/name`)}`,
					`printf %s ${shQuote(params.command)} > ${shQuote(`${dir}/command`)}`,
					`cat > ${shQuote(`${dir}/run.sh`)}`,
					`chmod +x ${shQuote(`${dir}/run.sh`)}`,
					// Group with { ...; } so `&` backgrounds only nohup; echo $! then captures
					// ITS pid. Without the braces, `&` would background the whole `&&` setup
					// chain and `echo $! > pid` would run before mkdir created the dir.
					`{ nohup setsid bash ${shQuote(`${dir}/run.sh`)} > ${shQuote(`${dir}/stdout.log`)} 2> ${shQuote(`${dir}/stderr.log`)} < /dev/null & echo $! > ${shQuote(`${dir}/pid`)}; }`,
					`printf %s ${shQuote(procId)}`,
				].join(" && ");
				const r = await runRemoteCommand(t, cmd, { stdin: script, signal });
				if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
				const watches: WatchState[] = (params.logWatches ?? []).map((w) => {
					let re: RegExp;
					try {
						re = new RegExp(w.pattern);
					} catch (e) {
						throw new Error(`Invalid logWatches pattern /${w.pattern}/: ${e instanceof Error ? e.message : String(e)}`);
					}
					return { re, pattern: w.pattern, stream: w.stream ?? "both", repeat: w.repeat ?? false, fired: false };
				});
				startPoller({
					procId,
					name,
					dir,
					target: t,
					alertOnSuccess: params.alertOnSuccess ?? false,
					alertOnFailure: params.alertOnFailure ?? true,
					alertOnKill: params.alertOnKill ?? false,
					watches,
				});
				// Point-of-need discovery: this job already alerts on failure; nudge the
				// agent toward success/log-watch notifications only when it did not opt in,
				// so it stops polling list/output. Suppressed once the feature is used.
				const optedIntoNotify = (params.alertOnSuccess ?? false) || (params.alertOnKill ?? false) || watches.length > 0;
				const tip = optedIntoNotify
					? ""
					: "\nWill notify you automatically if it fails — do not poll. Pass alertOnSuccess and/or logWatches to also be notified on success or when a log line matches.";
				return { content: [{ type: "text" as const, text: `Started remote process ${procId} (${name})\nstdout: ${dir}/stdout.log\nstderr: ${dir}/stderr.log${tip}` }], details: { id: procId, name, stdout: `${dir}/stdout.log`, stderr: `${dir}/stderr.log` } };
			}

			if (params.action === "list") {
				const cmd = `root=${shQuote(root)}; if [ ! -d "$root" ]; then echo 'No remote processes.'; exit 0; fi; found=0; for d in "$root"/*; do [ -d "$d" ] || continue; found=1; id=$(basename "$d"); pid=$(cat "$d/pid" 2>/dev/null || true); name=$(cat "$d/name" 2>/dev/null || true); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then status=running; code='-'; else status=exited; code=$(cat "$d/exit_code" 2>/dev/null || true); [ -n "$code" ] || code='?'; fi; printf '%s\t%s\texit=%s\tpid=%s\t%s\n' "$id" "$status" "$code" "$pid" "$name"; done; [ "$found" -eq 0 ] && echo 'No remote processes.' || true`;
				const r = await runRemoteCommand(t, cmd, { signal });
				if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
				const text = r.stdout.toString().trim() || "No remote processes.";
				return { content: [{ type: "text" as const, text }] };
			}

			if (params.action === "clear") {
				const cmd = `root=${shQuote(root)}; if [ ! -d "$root" ]; then echo 'No remote processes.'; exit 0; fi; removed=0; kept=0; for d in "$root"/*; do [ -d "$d" ] || continue; pid=$(cat "$d/pid" 2>/dev/null || true); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then kept=$((kept+1)); continue; fi; rm -rf "$d" && removed=$((removed+1)); done; rmdir "$root" 2>/dev/null || true; printf 'Cleared %s finished process(es); %s still running.\n' "$removed" "$kept"`;
				const r = await runRemoteCommand(t, cmd, { signal });
				if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
				return { content: [{ type: "text" as const, text: r.stdout.toString().trim() }] };
			}

			if (!params.id?.trim()) throw new Error(`ssh_process ${params.action} requires id`);
			const dir = `${root}/${params.id}`;

			if (params.action === "output") {
				const lines = Math.max(1, Math.floor(params.lines ?? 80));
				const cmd = `d=${shQuote(dir)}; test -d "$d" || { echo 'process not found' >&2; exit 2; }; echo '--- stdout ---'; tail -n ${lines} "$d/stdout.log" 2>/dev/null || true; echo '--- stderr ---'; tail -n ${lines} "$d/stderr.log" 2>/dev/null || true; pid=$(cat "$d/pid" 2>/dev/null || true); if [ -f "$d/exit_code" ]; then echo "--- exited, code: $(cat "$d/exit_code") ---"; elif [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then echo "--- still running (pid $pid) ---"; else echo '--- exited (no exit code recorded) ---'; fi`;
				const r = await runRemoteCommand(t, cmd, { signal });
				if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
				return { content: [{ type: "text" as const, text: r.stdout.toString() || "(no output)" }] };
			}

			if (params.action === "logs") {
				return { content: [{ type: "text" as const, text: `stdout: ${dir}/stdout.log\nstderr: ${dir}/stderr.log\nrun script: ${dir}/run.sh` }], details: { stdout: `${dir}/stdout.log`, stderr: `${dir}/stderr.log`, script: `${dir}/run.sh` } };
			}

			// Agent-initiated kill: drop the poller first so it does not fire a
			// spurious completion/kill notification for an expected teardown.
			stopPoller(params.id);
			const cmd = `d=${shQuote(dir)}; test -d "$d" || { echo 'process not found' >&2; exit 2; }; pid=$(cat "$d/pid" 2>/dev/null || true); test -n "$pid" || { echo 'pid not found' >&2; exit 2; }; kill "$pid" 2>/dev/null || true; sleep 1; kill -9 "$pid" 2>/dev/null || true; printf 'killed %s\n' "$pid"`;
			const r = await runRemoteCommand(t, cmd, { signal });
			if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
			return { content: [{ type: "text" as const, text: r.stdout.toString().trim() }] };
		},
	});

	pi.registerTool({
		name: "ssh_push",
		label: "ssh_push",
		description: "Push local files to the active SSH remote using rsync over the reused SSH connection. Respects .gitignore via rsync filter. Local read/write/edit remain local.",
		promptSnippet: "Rsync local workspace files to the active SSH remote",
		promptGuidelines: ["Use ssh_push before remote testing when local edits need to be synced to the active SSH remote."],
		parameters: Type.Object({
			localPath: Type.Optional(Type.String({ description: "Local path to push, defaults to current workspace" })),
			remotePath: Type.Optional(Type.String({ description: "Remote destination path, defaults to remote cwd" })),
			delete: Type.Optional(Type.Boolean({ description: "Delete remote files absent locally (default false)" })),
			dryRun: Type.Optional(Type.Boolean({ description: "Preview the transfer without writing (rsync --dry-run). Useful before pushing a large tree." })),
			excludes: Type.Optional(Type.Array(Type.String(), { description: "Additional rsync exclude patterns" })),
		}),
		async execute(_id, params: { localPath?: string; remotePath?: string; delete?: boolean; dryRun?: boolean; excludes?: string[] }, signal, onUpdate) {
			const t = requireTarget();
			const localSource = ensureTrailingSlash(resolve(localCwd, params.localPath ?? "."));
			const remoteDest = ensureTrailingSlash(params.remotePath ? toRemotePath(params.remotePath, localCwd, t.remoteCwd) : t.remoteCwd);
			const args = ["-az", "--info=progress2", "--itemize-changes", "--human-readable", "--stats", "--filter=:- .gitignore", "--exclude", ".git/", "-e", rsyncSshCommand(t)];
			for (const exclude of params.excludes ?? []) args.push("--exclude", exclude);
			if (params.delete) args.push("--delete");
			if (params.dryRun) args.push("--dry-run");
			args.push(localSource, `${t.remote}:${remoteDest}`);
			const r = await runLocalProcess("rsync", args, signal, rsyncStreamer(onUpdate));
			if (r.code !== 0) throw new Error(`rsync failed (${r.code ?? "unknown"}): ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
			const prefix = params.dryRun ? "[dry-run] " : "";
			return { content: [{ type: "text" as const, text: `${prefix}${r.stdout.toString().trim() || `Pushed ${localSource} -> ${t.remote}:${remoteDest}`}` }] };
		},
	});

	pi.registerTool({
		name: "ssh_pull",
		label: "ssh_pull",
		description: "Pull files from the active SSH remote to the local workspace using rsync over the reused SSH connection.",
		promptSnippet: "Rsync files from the active SSH remote to local workspace",
		parameters: Type.Object({
			remotePath: Type.Optional(Type.String({ description: "Remote source path, defaults to remote cwd" })),
			localPath: Type.Optional(Type.String({ description: "Local destination path, defaults to current workspace" })),
			delete: Type.Optional(Type.Boolean({ description: "Delete local files absent remotely (default false; use carefully)" })),
			dryRun: Type.Optional(Type.Boolean({ description: "Preview the transfer without writing (rsync --dry-run)." })),
			excludes: Type.Optional(Type.Array(Type.String(), { description: "Additional rsync exclude patterns" })),
		}),
		async execute(_id, params: { remotePath?: string; localPath?: string; delete?: boolean; dryRun?: boolean; excludes?: string[] }, signal, onUpdate) {
			const t = requireTarget();
			const remoteSource = ensureTrailingSlash(params.remotePath ? toRemotePath(params.remotePath, localCwd, t.remoteCwd) : t.remoteCwd);
			const localDest = ensureTrailingSlash(resolve(localCwd, params.localPath ?? "."));
			const args = ["-az", "--info=progress2", "--itemize-changes", "--human-readable", "--stats", "-e", rsyncSshCommand(t)];
			for (const exclude of params.excludes ?? []) args.push("--exclude", exclude);
			if (params.delete) args.push("--delete");
			if (params.dryRun) args.push("--dry-run");
			args.push(`${t.remote}:${remoteSource}`, localDest);
			const r = await runLocalProcess("rsync", args, signal, rsyncStreamer(onUpdate));
			if (r.code !== 0) throw new Error(`rsync failed (${r.code ?? "unknown"}): ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
			const prefix = params.dryRun ? "[dry-run] " : "";
			return { content: [{ type: "text" as const, text: `${prefix}${r.stdout.toString().trim() || `Pulled ${t.remote}:${remoteSource} -> ${localDest}`}` }] };
		},
	});


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

	// --- runtime connect/disconnect/status ---
	pi.registerCommand("ssh", {
		description: "SSH remote: /ssh [-i key] user@host[:/path] [--activate <cmd>] [--env K=V] | /ssh @profile | /ssh cd <dir> | /ssh save <name> | /ssh off | /ssh",
		handler: async (args, ctx) => {
			const arg = args.trim();

			if (!arg) {
				ctx.ui.notify(target ? statusLabel(target) : "SSH: not connected", "info");
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
