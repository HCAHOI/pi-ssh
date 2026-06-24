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

import { AsyncLocalStorage } from "node:async_hooks";
import { spawn } from "node:child_process";
import { type FSWatcher, mkdirSync, readFileSync, watch, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { dirname, isAbsolute, join, posix, relative, resolve, sep } from "node:path";
import { randomBytes } from "node:crypto";
import { Type } from "typebox";
import type { AgentToolUpdateCallback, ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Container, Key, matchesKey, type SelectItem, SelectList, Text, truncateToWidth } from "@earendil-works/pi-tui";
import {
	type BashOperations,
	createBashTool,
	createEditTool,
	createFindTool,
	createGrepTool,
	createLsTool,
	createReadTool,
	createReadToolDefinition,
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
	/** Epoch ms when this poller started tracking the job. Known for fresh starts
	 * (used to report run duration); absent for jobs rehydrated after a restart. */
	startedAt?: number;
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
	/** Opt in to exponential-backoff reconnection on transient transport failures.
	 * Defaults to the ambient reconnectCtx store (set by withReconnect for
	 * foreground tool calls); background pollers leave it off and retry once. */
	reconnect?: boolean;
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

function formatDuration(ms: number): string {
	const s = Math.round(ms / 1000);
	if (s < 60) return `${s}s`;
	const m = Math.floor(s / 60);
	const rem = s % 60;
	if (m < 60) return rem ? `${m}m${rem}s` : `${m}m`;
	const h = Math.floor(m / 60);
	return `${h}h${m % 60}m`;
}

// Condense rsync --stats output into a single line so a large transfer does not
// flood the agent context. Falls back to the provided message when the stats
// block is absent (e.g. nothing changed).
function summarizeRsync(stdout: string, fallback: string): string {
	const grab = (re: RegExp): string | null => {
		const m = stdout.match(re);
		return m ? m[1].trim() : null;
	};
	const transferred = grab(/Number of regular files transferred:\s*([\d,]+)/i);
	const size = grab(/Total transferred file size:\s*([\d.,]+[KMGTP]?)\s*bytes/i);
	const sent = grab(/Total bytes sent:\s*([\d.,]+[KMGTP]?)\s*bytes/i);
	if (transferred === null && size === null) return fallback;
	const parts: string[] = [`${transferred ?? "?"} file(s) transferred`];
	if (size) parts.push(`${size} bytes`);
	if (sent) parts.push(`sent ${sent} bytes`);
	return parts.join(", ");
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

// --- exponential-backoff reconnection (opt-in; foreground tools only) ---
// On a transient transport failure (isRetryableSshFailure), tear down the dead
// ControlMaster and retry the command — the next ssh invocation auto-respawns the
// master (ControlMaster=auto). With reconnect on, retry up to RECONNECT_MAX_ATTEMPTS
// with exponential backoff and a visible status each attempt; otherwise keep the
// historical retry-once. Only re-runs on transport failures (code 255 + known
// patterns), where the command almost certainly never executed, so re-running is
// safe. Background pollers must fail fast (retry next tick), so they never opt in.
const RECONNECT_MAX_ATTEMPTS = 10;
const RECONNECT_BASE_MS = 1000; // delay before the 2nd attempt
const RECONNECT_FACTOR = 2; // doubles each attempt
const RECONNECT_CAP_MS = 30_000; // cap on a single backoff delay

const reconnectCtx = new AsyncLocalStorage<{ reconnect: boolean }>();

interface ReconnectInfo {
	remote: string;
	attempt: number;
	max: number;
	delayMs: number;
}
type ReconnectPhase = "retrying" | "recovered" | "gaveup";
let reconnectNotifier: ((phase: ReconnectPhase, info: ReconnectInfo) => void) | null = null;

// Enable reconnection for every runRemoteCommand fn triggers. AsyncLocalStorage
// scopes it to fn's async tree, so concurrent background pollers (which run
// outside this context) never inherit it and never block on a long backoff.
function withReconnect<T>(fn: () => Promise<T>): Promise<T> {
	return reconnectCtx.run({ reconnect: true }, fn);
}

// Backoff (ms) before the given 1-indexed attempt number (>= 2): 1s,2s,4s,8s,16s,30s…
function backoffDelay(attempt: number): number {
	return Math.min(RECONNECT_CAP_MS, RECONNECT_BASE_MS * RECONNECT_FACTOR ** (attempt - 2));
}

// Resolves true if aborted during the wait, false if it slept the full duration.
function abortableSleep(ms: number, signal?: AbortSignal): Promise<boolean> {
	return new Promise((resolve) => {
		if (signal?.aborted) {
			resolve(true);
			return;
		}
		const onAbort = () => {
			clearTimeout(timer);
			resolve(true);
		};
		const timer = setTimeout(() => {
			signal?.removeEventListener("abort", onAbort);
			resolve(false);
		}, ms);
		signal?.addEventListener("abort", onAbort, { once: true });
	});
}

async function runRemoteCommand(t: SshTarget, command: string, opts?: RunOptions): Promise<RunResult> {
	const shell = remoteShell(command, opts?.login !== false);
	const reconnect = opts?.reconnect ?? reconnectCtx.getStore()?.reconnect ?? false;
	const maxAttempts = reconnect ? RECONNECT_MAX_ATTEMPTS : 2; // 2 => historical retry-once
	let r = await runSsh([...sshConnArgs(t), shell], opts);
	let attempt = 1;
	while (isRetryableSshFailure(r) && !opts?.signal?.aborted && attempt < maxAttempts) {
		const next = attempt + 1;
		await closeMaster(t);
		if (reconnect) {
			const delayMs = backoffDelay(next);
			reconnectNotifier?.("retrying", { remote: t.remote, attempt: next, max: maxAttempts, delayMs });
			if (await abortableSleep(delayMs, opts?.signal)) break;
		}
		r = await runSsh([...sshConnArgs(t), shell], opts);
		attempt = next;
	}
	if (reconnect && attempt > 1) {
		reconnectNotifier?.(isRetryableSshFailure(r) ? "gaveup" : "recovered", { remote: t.remote, attempt, max: maxAttempts, delayMs: 0 });
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
			// Returns paths relative to the remote search root, then prefixes the LOCAL
			// search cwd so the SDK find tool slices them back to clean relative output
			// (returning absolute remote paths makes it relativize against the local cwd
			// and emit ../../../root/... garbage). Honors .gitignore via `git check-ignore`
			// (the established tool) when the root is a git repo and git is present.
			const script = `
import fnmatch, json, os, subprocess, sys
payload = json.load(sys.stdin)
root = payload['root']
pattern = payload['pattern']
limit = int(payload['limit'])
use_gitignore = payload.get('gitignore', True)
candidates = []
for dirpath, dirnames, filenames in os.walk(root):
    dirnames[:] = [d for d in dirnames if d not in {'.git', 'node_modules'}]
    for name in filenames:
        path = os.path.join(dirpath, name)
        rel = os.path.relpath(path, root).replace(os.sep, '/')
        if fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(name, pattern):
            candidates.append(rel)
if use_gitignore and candidates and os.path.exists(os.path.join(root, '.git')):
    try:
        proc = subprocess.run(['git', '-C', root, 'check-ignore', '--stdin'],
            input='\\n'.join(candidates), capture_output=True, text=True)
        if proc.returncode in (0, 1):
            ignored = set(filter(None, proc.stdout.split('\\n')))
            candidates = [c for c in candidates if c not in ignored]
    except FileNotFoundError:
        pass
print(json.dumps(candidates[:limit]))
`;
			const payload = JSON.stringify({ root: toRemote(cwd), pattern, limit: options.limit });
			const r = await runRemoteCommand(t, `python3 -c ${shQuote(script)}`, { stdin: payload });
			if (r.code !== 0) throw new Error(r.stderr.toString().trim() || sshFailureMessage(r));
			const rels = JSON.parse(r.stdout.toString()) as string[];
			// Prefix the LOCAL search cwd so createFindTool's `p.slice(searchPath+1)`
			// yields the remote-root-relative path verbatim.
			const base = stripTrailingSlash(cwd);
			return rels.map((rel) => `${base}/${rel}`);
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
	// Definition (not the wrapped tool) so we can reuse its renderResult for ssh_read:
	// it formats the returned content with syntax highlight and does no fs access.
	const localReadDef = createReadToolDefinition(localCwd);
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
	reconnectNotifier = (phase, info) => {
		if (!uiRef) return;
		if (phase === "retrying") {
			uiRef.setStatus("ssh", uiRef.theme.fg("warning", `Reconnecting ${info.remote} \u2014 attempt ${info.attempt}/${info.max}, retry in ${Math.round(info.delayMs / 1000)}s\u2026`));
			return;
		}
		const label = statusLabel(target);
		uiRef.setStatus("ssh", label ? uiRef.theme.fg("accent", label) : "");
		if (phase === "recovered") uiRef.notify(`SSH reconnected: ${info.remote}`, "info");
		else uiRef.notify(`SSH reconnect to ${info.remote} failed after ${info.max} attempts`, "error");
	};

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
			for (const p of pollers.values()) p.target = next;
		} else {
			stopAllPollers();
		}
		if (prev) await closeMaster(prev);
		target = next;
		stopAllTunnels();
		await rehydratePollers(next);
		return next;
	}

	async function disconnect(): Promise<void> {
		stopAllPollers();
		stopSyncWatcher();
		if (target) {
			stopAllTunnels();
			await closeMaster(target);
		}
		target = null;
	}

	// --- ssh_process background notification poller (Phase 1) ---
	const pollers = new Map<string, PollerState>();
	// Most-recent start per job name. Lets a late completion notification flag that a
	// newer run of the same name was started after it, so the agent can tell stale
	// (superseded) alerts from current ones in parallel multi-run workflows. Survives
	// the finished poller's removal from `pollers`.
	const latestStartByName = new Map<string, string>();

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
		// Mark completion handled so a later reconnect/rehydrate never re-notifies for
		// this job, regardless of whether this particular alert was opted into.
		await runRemoteCommand(p.target, `touch -- ${shQuote(`${p.dir}/notified`)}`, { timeout: 20, login: false }).catch(() => {});
		const want = outcome === "success" ? p.alertOnSuccess : outcome === "killed" ? p.alertOnKill : p.alertOnFailure;
		if (!want) return;
		const tailCmd = `d=${shQuote(p.dir)}; echo '--- stdout (tail) ---'; tail -n 15 "$d/stdout.log" 2>/dev/null; echo '--- stderr (tail) ---'; tail -n 15 "$d/stderr.log" 2>/dev/null`;
		const tr = await runRemoteCommand(p.target, tailCmd, { timeout: 20, login: false }).catch(() => null);
		const tail = tr && tr.code === 0 ? tr.stdout.toString().trimEnd() : "";
		const emoji = outcome === "success" ? "✅" : outcome === "killed" ? "⛔" : "❌";
		const codeLabel = code === null ? "" : ` (exit ${code})`;
		const latest = latestStartByName.get(p.name);
		const superseded = latest !== undefined && latest !== p.procId;
		const supersedeLabel = superseded ? " [superseded: a newer run of this name was started after it]" : "";
		const ageLabel = p.startedAt ? ` after ${formatDuration(Date.now() - p.startedAt)}` : "";
		emit(`${emoji} ssh_process "${p.name}" (${p.procId})${supersedeLabel} ${outcome}${codeLabel}${ageLabel}.${tail ? `\n${tail}` : ""}`, {
			kind: "completion",
			procId: p.procId,
			name: p.name,
			outcome,
			code,
			superseded,
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
		/** Initial byte offsets. Fresh start: {0,0} (watch from the top). Rehydrate on
		 * reconnect/restart: seek to current EOF so historical log lines are not
		 * re-matched (only completion + new lines fire). */
		offsets?: { stdout: number; stderr: number };
		/** Epoch ms of the fresh start (for run-duration reporting); omit for rehydrate. */
		startedAt?: number;
	}): void {
		if (!args.alertOnSuccess && !args.alertOnFailure && !args.alertOnKill && args.watches.length === 0) return;
		if (pollers.has(args.procId)) return; // already tracked
		const state: PollerState = {
			...args,
			off: args.offsets ?? { stdout: 0, stderr: 0 },
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

	function buildWatchStates(specs: WatchSpec[] | undefined): WatchState[] {
		return (specs ?? []).map((w) => {
			let re: RegExp;
			try {
				re = new RegExp(w.pattern);
			} catch (e) {
				throw new Error(`Invalid logWatches pattern /${w.pattern}/: ${e instanceof Error ? e.message : String(e)}`);
			}
			return { re, pattern: w.pattern, stream: w.stream ?? "both", repeat: w.repeat ?? false, fired: false };
		});
	}

	interface NotifyConfig {
		name?: string;
		alertOnSuccess?: boolean;
		alertOnFailure?: boolean;
		alertOnKill?: boolean;
		watches?: WatchSpec[];
	}

	// Re-arm pollers from each job's persisted notify.json after a (re)connect, so
	// completion / log-watch notifications survive reconnect, ssh_connect switching,
	// and full pi restarts. Jobs already tracked or already notified are skipped;
	// offsets seek to EOF so historical watch lines do not re-fire. A finished,
	// un-notified job fires its missed completion on the first tick.
	async function rehydratePollers(t: SshTarget): Promise<void> {
		if (!t.hasPython) return;
		const root = processRoot(t);
		const script = `
import base64, json, os, sys
root = sys.argv[1]
out = []
if os.path.isdir(root):
    for d in sorted(os.listdir(root)):
        p = os.path.join(root, d)
        nf = os.path.join(p, 'notify.json')
        if not (os.path.isdir(p) and os.path.isfile(nf)):
            continue
        try:
            notify = open(nf).read()
        except OSError:
            continue
        pid = ''
        try:
            pid = open(os.path.join(p, 'pid')).read().strip()
        except OSError:
            pass
        running = False
        if pid:
            try:
                os.kill(int(pid), 0)
                running = True
            except (OSError, ValueError):
                running = False
        def sz(f):
            try:
                return os.path.getsize(os.path.join(p, f))
            except OSError:
                return 0
        out.append({
            'id': d,
            'notify': base64.b64encode(notify.encode()).decode(),
            'running': running,
            'notified': os.path.isfile(os.path.join(p, 'notified')),
            'outSize': sz('stdout.log'),
            'errSize': sz('stderr.log'),
        })
print(json.dumps(out))
`;
		let entries: Array<{ id: string; notify: string; running: boolean; notified: boolean; outSize: number; errSize: number }>;
		try {
			const r = await runRemoteCommand(t, `python3 -c ${shQuote(script)} ${shQuote(root)}`, { timeout: 20, login: false });
			if (r.code !== 0) return;
			entries = JSON.parse(r.stdout.toString() || "[]");
		} catch {
			return;
		}
		for (const e of entries) {
			if (pollers.has(e.id) || e.notified) continue;
			let cfg: NotifyConfig;
			try {
				cfg = JSON.parse(Buffer.from(e.notify, "base64").toString());
			} catch {
				continue;
			}
			startPoller({
				procId: e.id,
				name: cfg.name?.trim() || e.id,
				dir: `${root}/${e.id}`,
				target: t,
				alertOnSuccess: cfg.alertOnSuccess ?? false,
				alertOnFailure: cfg.alertOnFailure ?? true,
				alertOnKill: cfg.alertOnKill ?? false,
				watches: buildWatchStates(cfg.watches),
				offsets: { stdout: e.outSize, stderr: e.errSize },
			});
		}
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

	// ---------------------------------------------------------------------------
	// Tool-call rendering helpers (renderCall titles like `ssh_read [host] path:lines`)
	// ---------------------------------------------------------------------------
	function str(v: unknown): string {
		return typeof v === "string" ? v : "";
	}

	function resultText(r: any): string {
		return (r?.content ?? []).map((c: any) => (c?.type === "text" ? c.text : "")).join("");
	}

	// Remote path shown relative to remoteCwd (best-effort; never throws).
	function remoteDisplayPath(raw: string): string {
		const t = target;
		if (!t) return raw;
		try {
			const rp = toRemotePath(raw, localCwd, t.remoteCwd);
			const base = stripTrailingSlash(t.remoteCwd);
			if (rp === base) return ".";
			if (rp.startsWith(`${base}/`)) return rp.slice(base.length + 1);
			return rp;
		} catch {
			return raw;
		}
	}

	function accentRemotePath(raw: unknown, theme: any): string {
		const p = str(raw);
		return p ? theme.fg("accent", remoteDisplayPath(p)) : theme.fg("toolOutput", "...");
	}

	// Mirrors the SDK read tool's line-range suffix from {offset,limit}.
	function readLineRange(args: { offset?: number; limit?: number }, theme: any): string {
		if (args?.offset === undefined && args?.limit === undefined) return "";
		const startLine = args.offset ?? 1;
		const endLine = args.limit !== undefined ? startLine + args.limit - 1 : "";
		return theme.fg("warning", `:${startLine}${endLine ? `-${endLine}` : ""}`);
	}

	function hostTag(theme: any): string {
		const t = target;
		// The tool-call title renders on a colored Box bg (toolPendingBg/toolSuccessBg/
		// toolErrorBg). `success` clashes with the green success bg and `muted` is too
		// low-contrast; `warning` (amber) is one of the colors the local tools use, so it
		// stays readable on every tool bg and is distinct from the accent path.
		return t ? theme.fg("warning", `[${t.remote}]`) : "";
	}

	// Build the one-line tool-call title, reusing the prior Text component.
	// `[host] <label> <rest>` — host badge first, then the local tool's own short
	// label (read/edit/ls/...) so remote calls read like the local ones. bash passes
	// an empty label (the `$ cmd` is the label, as in the local bash tool).
	function sshTitle(label: string, rest: string, theme: any, context: any): Text {
		const text: Text = context?.lastComponent instanceof Text ? context.lastComponent : new Text("", 0, 0);
		const name = label ? theme.fg("toolTitle", theme.bold(label)) : "";
		text.setText([hostTag(theme), name, rest].filter((s) => s && s.length > 0).join(" "));
		return text;
	}

	// Colored unified-diff result for ssh_edit (parses details.diff produced remotely).
	function renderEditDiffResult(result: any, theme: any, context: any): Container | Text {
		if (context?.isError) {
			const t = context?.lastComponent instanceof Text ? context.lastComponent : new Text("", 0, 0);
			t.setText(theme.fg("error", resultText(result) || "edit failed"));
			return t;
		}
		const diff = typeof result?.details?.diff === "string" ? result.details.diff : "";
		const lines = diff ? diff.split("\n") : [];
		let adds = 0;
		let dels = 0;
		for (const l of lines) {
			if (l.startsWith("+") && !l.startsWith("+++")) adds++;
			else if (l.startsWith("-") && !l.startsWith("---")) dels++;
		}
		const container = new Container();
		const header = adds || dels ? `${theme.fg("toolDiffAdded", `+${adds}`)} ${theme.fg("toolDiffRemoved", `-${dels}`)}` : theme.fg("muted", "(no textual changes)");
		container.addChild(new Text(header, 0, 0));
		const MAX = 40;
		let shown = 0;
		for (const l of lines) {
			if (l.startsWith("+++") || l.startsWith("---") || l.startsWith("@@") || l.startsWith("diff ")) continue;
			if (shown >= MAX) {
				container.addChild(new Text(theme.fg("dim", `\u2026 (${lines.length} diff lines total)`), 0, 0));
				break;
			}
			const colored = l.startsWith("+") ? theme.fg("toolDiffAdded", l) : l.startsWith("-") ? theme.fg("toolDiffRemoved", l) : theme.fg("toolDiffContext", l);
			container.addChild(new Text(colored, 0, 0));
			shown++;
		}
		return container;
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
		description: "Show the active SSH remote used by ssh_* tools.",
		promptSnippet: "Show active SSH connection status",
		parameters: Type.Object({}),
		renderCall(_args: any, theme: any, context: any) {
			return sshTitle("status", "", theme, context);
		},
		async execute() {
			const base = target ? connectedText(target) : "SSH: not connected";
			let profiles: string[] = [];
			try { profiles = profileNames(); } catch { /* corrupt profiles file: ignore for status */ }
			const profileLine = profiles.length ? `\nSaved profiles (reconnect with ssh_connect '@name'): ${profiles.map((n) => `@${n}`).join(", ")}` : "";
			return { content: [{ type: "text" as const, text: base + profileLine }] };
		},
	});

	// --- remote read ---
	pi.registerTool({
		...localRead,
		name: "ssh_read",
		label: "ssh_read",
		description: `Read a file from the active SSH remote. Relative paths are resolved under the remote cwd. Use local read for local files. ${localRead.description}`,
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
		description: `List directory contents on the active SSH remote. Relative paths are resolved under the remote cwd. Use local ls/find for local files. ${localLs.description}`,
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
		description: `Find files on the active SSH remote. Relative paths are resolved under the remote cwd. Use local find for local files. ${localFind.description}`,
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
		description: `Search file contents on the active SSH remote. Relative paths are resolved under the remote cwd. Uses remote rg when available, otherwise grep. Use local grep for local files. ${localGrep.description}`,
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
		description: `Write a file on the active SSH remote. Relative paths are resolved under the remote cwd. Use local write for local files. ${localWrite.description}`,
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
		description: "Write a secret to a remote file WITHOUT the value passing through the tool-call log. The value is read locally from an environment variable (fromEnv) or a local file (fromFile) and streamed to the remote over stdin; only the name/path and destination are recorded. The remote file is created with mode 0600 by default (created under umask 077 so it is never briefly world-readable). Use this instead of ssh_write/ssh_bash for API keys, tokens, and credentials.",
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
		description: `Edit a file on the active SSH remote. Relative paths are resolved under the remote cwd. Use local edit for local files. ${localEdit.description}`,
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

	// --- remote bash ---
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

	function processRoot(t: SshTarget): string {
		return `${t.remoteCwd}/.pi-ssh-processes`;
	}

	// --- shared process queries (used by ssh_process AND the /ssh dashboard) ---
	interface ProcRow {
		id: string;
		name: string;
		status: "running" | "exited";
		code: number | null;
		pid: string;
		/** Epoch ms decoded from the id prefix (base36 Date.now() at start), or null. */
		startedMs: number | null;
	}

	// Single source of truth for the process listing. The structured parse below and
	// the ssh_process `list` text output both derive from this one command.
	function buildProcessListCommand(root: string): string {
		return `root=${shQuote(root)}; if [ ! -d "$root" ]; then echo 'No remote processes.'; exit 0; fi; found=0; for d in "$root"/*; do [ -d "$d" ] || continue; found=1; id=$(basename "$d"); pid=$(cat "$d/pid" 2>/dev/null || true); name=$(cat "$d/name" 2>/dev/null || true); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then status=running; code='-'; else status=exited; code=$(cat "$d/exit_code" 2>/dev/null || true); [ -n "$code" ] || code='?'; fi; printf '%s\t%s\texit=%s\tpid=%s\t%s\n' "$id" "$status" "$code" "$pid" "$name"; done; [ "$found" -eq 0 ] && echo 'No remote processes.' || true`;
	}

	function parseProcRows(text: string): ProcRow[] {
		const rows: ProcRow[] = [];
		for (const line of text.split("\n")) {
			if (!line.trim() || line.trim() === "No remote processes.") continue;
			const parts = line.split("\t");
			if (parts.length < 5) continue;
			const [id, statusRaw, exitField, pidField] = parts;
			const name = parts.slice(4).join("\t");
			const status = statusRaw === "running" ? "running" : "exited";
			const codeStr = exitField.startsWith("exit=") ? exitField.slice(5) : "";
			const code = status === "running" || codeStr === "?" || codeStr === "-" || codeStr === "" ? null : Number.parseInt(codeStr, 10);
			const pid = pidField.startsWith("pid=") ? pidField.slice(4) : pidField;
			const base = id.split("-")[0];
			const ms = Number.parseInt(base, 36);
			rows.push({ id, name, status, code: Number.isNaN(code as number) ? null : code, pid, startedMs: Number.isNaN(ms) ? null : ms });
		}
		return rows;
	}

	// Reproduce the exact ssh_process `list` text from structured rows, byte-identical
	// to the legacy inline command output (so the agent-facing tool is unchanged).
	function formatProcRows(rows: ProcRow[]): string {
		return rows
			.map((r) => {
				const code = r.status === "running" ? "-" : r.code === null ? "?" : String(r.code);
				return `${r.id}\t${r.status}\texit=${code}\tpid=${r.pid}\t${r.name}`;
			})
			.join("\n");
	}

	async function listProcesses(t: SshTarget, signal?: AbortSignal): Promise<ProcRow[]> {
		const r = await runRemoteCommand(t, buildProcessListCommand(processRoot(t)), { signal });
		if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
		return parseProcRows(r.stdout.toString());
	}

	function buildKillCommand(dir: string): string {
		return `d=${shQuote(dir)}; test -d "$d" || { echo 'process not found' >&2; exit 2; }; pid=$(cat "$d/pid" 2>/dev/null || true); test -n "$pid" || { echo 'pid not found' >&2; exit 2; }; kill "$pid" 2>/dev/null || true; sleep 1; kill -9 "$pid" 2>/dev/null || true; printf 'killed %s\n' "$pid"`;
	}

	function buildClearCommand(root: string): string {
		return `root=${shQuote(root)}; if [ ! -d "$root" ]; then echo 'No remote processes.'; exit 0; fi; removed=0; kept=0; for d in "$root"/*; do [ -d "$d" ] || continue; pid=$(cat "$d/pid" 2>/dev/null || true); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then kept=$((kept+1)); continue; fi; rm -rf "$d" && removed=$((removed+1)); done; rmdir "$root" 2>/dev/null || true; printf 'Cleared %s finished process(es); %s still running.\n' "$removed" "$kept"`;
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

	// rsync >= 3.1 supports --info=progress2 (single clean progress line); the
	// rsync Apple ships by default (2.6.9) rejects it with a cryptic usage dump.
	// Probe once and fall back to --progress so push/pull work out of the box on
	// stock macOS. Cached for the session.
	let rsyncProgressFlagCache: string | null = null;
	async function rsyncProgressFlag(): Promise<string> {
		if (rsyncProgressFlagCache) return rsyncProgressFlagCache;
		try {
			const r = await runLocalProcess("rsync", ["--version"]);
			const m = r.stdout.toString().match(/rsync\s+version\s+(\d+)\.(\d+)/i);
			const major = m ? Number.parseInt(m[1], 10) : 0;
			const minor = m ? Number.parseInt(m[2], 10) : 0;
			const modern = major > 3 || (major === 3 && minor >= 1);
			rsyncProgressFlagCache = modern ? "--info=progress2" : "--progress";
		} catch (e) {
			if ((e as NodeJS.ErrnoException)?.code === "ENOENT") {
				throw new Error("rsync not found on this machine. Install it (macOS: `brew install rsync`) to use ssh_push/ssh_pull/ssh_sync.");
			}
			rsyncProgressFlagCache = "--progress"; // unknown version: assume old/safe
		}
		return rsyncProgressFlagCache;
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

	// Shared rsync push/pull core (used by ssh_push, ssh_pull, and ssh_sync).
	async function runRsyncTransfer(
		t: SshTarget,
		source: string,
		dest: string,
		opts: { delete?: boolean; dryRun?: boolean; excludes?: string[]; gitignore?: boolean; quiet?: boolean; verbose?: boolean },
		signal?: AbortSignal,
		onData?: (chunk: Buffer) => void,
	): Promise<{ stdout: string; stderr: string; elapsedMs: number }> {
		// quiet   -> no output (ssh_sync background runs)
		// verbose -> per-file itemized list + progress + full stats (opt-in)
		// default -> stats only, condensed to one line by the caller (summarizeRsync)
		const progress = opts.quiet
			? []
			: opts.verbose
				? [await rsyncProgressFlag(), "--itemize-changes", "--human-readable", "--stats"]
				: ["--human-readable", "--stats"];
		const args = ["-az", ...progress];
		if (opts.gitignore) args.push("--filter=:- .gitignore", "--exclude", ".git/");
		args.push("-e", rsyncSshCommand(t));
		for (const exclude of opts.excludes ?? []) args.push("--exclude", exclude);
		if (opts.delete) args.push("--delete");
		if (opts.dryRun) args.push("--dry-run");
		args.push(source, dest);
		const started = Date.now();
		const r = await runLocalProcess("rsync", args, signal, onData);
		if (r.code !== 0) throw new Error(`rsync failed (${r.code ?? "unknown"}): ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
		return { stdout: r.stdout.toString().trim(), stderr: r.stderr.toString().trim(), elapsedMs: Date.now() - started };
	}

	pi.registerTool({
		name: "ssh_process",
		label: "ssh_process",
		description: "Manage long-running processes on the active SSH remote. Starts commands in the background with logs under remote .pi-ssh-processes/<id>/; supports start/list/output/logs/kill/clear/attach. output accepts followSeconds to live-stream new log lines; attach re-arms completion/log-watch notifications for an existing job. list and output report the captured exit code once a job finishes; clear prunes finished jobs. On start, alertOnSuccess/alertOnFailure/alertOnKill and logWatches push a notification (re-engaging the agent) when the job ends or a log line matches — so you never poll. Notifications survive Mac sleep AND reconnect/pi-restart: each job persists its notify config and pollers are re-armed on connect, sweeping missed completions/log lines.",
		promptSnippet: "Manage long-running remote SSH processes",
		promptGuidelines: [
			"Use ssh_process start for long-running remote jobs such as training, dev servers, and log tails instead of blocking ssh_bash.",
			"Use ssh_process output (optionally followSeconds) to inspect stdout/stderr and ssh_process kill to stop remote jobs.",
			"Background jobs push a notification when they finish or a logWatch matches — rely on it instead of polling list/output. After a reconnect, use ssh_process attach <id> to resume notifications for a job started earlier.",
		],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("start"), Type.Literal("list"), Type.Literal("output"), Type.Literal("logs"), Type.Literal("kill"), Type.Literal("clear"), Type.Literal("attach")]),
			name: Type.Optional(Type.String({ description: "Friendly process name for start" })),
			command: Type.Optional(Type.String({ description: "Command to start on the remote" })),
			id: Type.Optional(Type.String({ description: "Remote process id returned by start/list" })),
			cwd: Type.Optional(Type.String({ description: "Remote working directory, defaults to remote cwd" })),
			env: Type.Optional(Type.Record(Type.String(), Type.String(), { description: "Environment variables exported before command" })),
			commandPrefix: Type.Optional(Type.String({ description: "Shell code run before command" })),
			lines: Type.Optional(Type.Number({ description: "Number of recent log lines for output (default 80)" })),
			followSeconds: Type.Optional(Type.Number({ description: "output: stream new stdout/stderr lines live for this many seconds before returning" })),
			alertOnSuccess: Type.Optional(Type.Boolean({ description: "start: notify when the job exits 0 (default false)" })),
			alertOnFailure: Type.Optional(Type.Boolean({ description: "start: notify when the job exits non-zero (default true)" })),
			alertOnKill: Type.Optional(Type.Boolean({ description: "start: notify when the job is killed by a signal (default false)" })),
			logWatches: Type.Optional(Type.Array(Type.Object({
				pattern: Type.String({ description: "Regex matched per log line" }),
				stream: Type.Optional(Type.Union([Type.Literal("stdout"), Type.Literal("stderr"), Type.Literal("both")], { description: "Which stream to watch (default both)" })),
				repeat: Type.Optional(Type.Boolean({ description: "Fire every match (default false: one-shot)" })),
			}), { description: "start: notify when a log line matches; re-engages the agent" })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const a = str(args?.action) || "?";
			let rest: string;
			if (a === "start") {
				const nm = args?.name ? ` ${theme.fg("accent", `"${args.name}"`)}` : "";
				const cmd = str(args?.command);
				rest = `${theme.fg("accent", "start")}${nm}${cmd ? ` ${theme.fg("muted", `$ ${cmd}`)}` : ""}`;
			} else if (a === "kill" || a === "output" || a === "logs" || a === "attach") {
				rest = `${theme.fg("accent", a)}${args?.id ? ` ${theme.fg("muted", str(args.id))}` : ""}`;
			} else {
				rest = theme.fg("accent", a);
			}
			return sshTitle("process", rest, theme, context);
		},
		async execute(_id, params: {
			action: "start" | "list" | "output" | "logs" | "kill" | "clear" | "attach";
			name?: string;
			command?: string;
			id?: string;
			cwd?: string;
			env?: Record<string, string>;
			commandPrefix?: string;
			lines?: number;
			followSeconds?: number;
			alertOnSuccess?: boolean;
			alertOnFailure?: boolean;
			alertOnKill?: boolean;
			logWatches?: WatchSpec[];
		}, signal, onUpdate) {
			const t = requireTarget();
			const root = processRoot(t);
			if (params.action === "start") {
				if (!params.command?.trim()) throw new Error("ssh_process start requires command");
				const procId = `${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
				const dir = `${root}/${procId}`;
				const name = params.name?.trim() || procId;
				const script = processRunScript(t, { command: params.command, cwd: params.cwd, env: params.env, commandPrefix: params.commandPrefix });
				// Persist the notification config so pollers can be re-armed after a
				// reconnect / pi restart (rehydratePollers reads this).
				const notifyJson = JSON.stringify({
					name,
					alertOnSuccess: params.alertOnSuccess ?? false,
					alertOnFailure: params.alertOnFailure ?? true,
					alertOnKill: params.alertOnKill ?? false,
					watches: params.logWatches ?? [],
				} satisfies NotifyConfig);
				const cmd = [
					`mkdir -p ${shQuote(dir)}`,
					`printf %s ${shQuote(name)} > ${shQuote(`${dir}/name`)}`,
					`printf %s ${shQuote(params.command)} > ${shQuote(`${dir}/command`)}`,
					`printf %s ${shQuote(notifyJson)} > ${shQuote(`${dir}/notify.json`)}`,
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
				const watches: WatchState[] = buildWatchStates(params.logWatches);
				// Record this as the most recent run of `name` so an older run's late
				// completion alert can be flagged as superseded.
				latestStartByName.set(name, procId);
				startPoller({
					procId,
					name,
					dir,
					target: t,
					alertOnSuccess: params.alertOnSuccess ?? false,
					alertOnFailure: params.alertOnFailure ?? true,
					alertOnKill: params.alertOnKill ?? false,
					watches,
					startedAt: Date.now(),
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
				const rows = await listProcesses(t, signal);
				const text = rows.length ? formatProcRows(rows) : "No remote processes.";
				return { content: [{ type: "text" as const, text }] };
			}

			if (params.action === "clear") {
				const r = await runRemoteCommand(t, buildClearCommand(root), { signal });
				if (r.code !== 0) throw new Error(`${sshFailureMessage(r)}: ${r.stderr.toString().trim() || r.stdout.toString().trim()}`);
				return { content: [{ type: "text" as const, text: r.stdout.toString().trim() }] };
			}

			if (!params.id?.trim()) throw new Error(`ssh_process ${params.action} requires id`);
			const dir = `${root}/${params.id}`;

			if (params.action === "attach") {
				if (pollers.has(params.id)) {
					return { content: [{ type: "text" as const, text: `Already watching ${params.id}.` }] };
				}
				const probe = `d=${shQuote(dir)}; if [ ! -d "$d" ]; then echo 'process not found' >&2; exit 2; fi; o=$(wc -c < "$d/stdout.log" 2>/dev/null || echo 0); e=$(wc -c < "$d/stderr.log" 2>/dev/null || echo 0); n=$(base64 < "$d/notify.json" 2>/dev/null | tr -d '\n'); printf '%s\t%s\t%s\n' "$o" "$e" "$n"`;
				const pr = await runRemoteCommand(t, probe, { signal, login: false });
				if (pr.code !== 0) throw new Error(`${sshFailureMessage(pr)}: ${pr.stderr.toString().trim() || pr.stdout.toString().trim()}`);
				const [outStr = "0", errStr = "0", notifyB64 = ""] = pr.stdout.toString().trim().split("\t");
				let saved: NotifyConfig = {};
				if (notifyB64) {
					try { saved = JSON.parse(Buffer.from(notifyB64, "base64").toString()); } catch { /* ignore corrupt config */ }
				}
				const cfg: Required<NotifyConfig> = {
					name: params.name?.trim() || saved.name || params.id,
					alertOnSuccess: params.alertOnSuccess ?? saved.alertOnSuccess ?? true,
					alertOnFailure: params.alertOnFailure ?? saved.alertOnFailure ?? true,
					alertOnKill: params.alertOnKill ?? saved.alertOnKill ?? false,
					watches: params.logWatches ?? saved.watches ?? [],
				};
				// Persist merged prefs and clear the notified marker so a finished job
				// re-fires its completion exactly once for this explicit attach.
				const persist = `d=${shQuote(dir)}; printf %s ${shQuote(JSON.stringify(cfg))} > "$d/notify.json"; rm -f "$d/notified"`;
				await runRemoteCommand(t, persist, { signal, login: false }).catch(() => {});
				startPoller({
					procId: params.id,
					name: cfg.name,
					dir,
					target: t,
					alertOnSuccess: cfg.alertOnSuccess,
					alertOnFailure: cfg.alertOnFailure,
					alertOnKill: cfg.alertOnKill,
					watches: buildWatchStates(cfg.watches),
					offsets: { stdout: Number.parseInt(outStr, 10) || 0, stderr: Number.parseInt(errStr, 10) || 0 },
				});
				return { content: [{ type: "text" as const, text: `Watching ${params.id} (${cfg.name}). Will notify on completion${cfg.watches.length ? " and matching log lines" : ""}.` }] };
			}

			if (params.action === "output") {
				const lines = Math.max(1, Math.floor(params.lines ?? 80));
				const follow = params.followSeconds && params.followSeconds > 0 ? Math.floor(params.followSeconds) : 0;
				if (follow > 0) {
					// Live stream new lines from both logs for `follow` seconds (bounded by the
					// remote `timeout`), pushing incremental output to the tool view.
					const ops = createRemoteBashOps(t, localCwd);
					const MAX = 8 * 1024;
					let acc = "";
					const onData = (d: Buffer) => {
						acc += d.toString();
						if (acc.length > MAX) acc = `…${acc.slice(-MAX)}`;
						onUpdate?.({ content: [{ type: "text", text: acc }], details: undefined });
					};
					await ops
						.exec(`timeout ${follow} tail -n ${lines} -F stdout.log stderr.log 2>/dev/null`, dir, { onData, signal, timeout: follow + 15 })
						.catch(() => {});
				}
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
			const r = await runRemoteCommand(t, buildKillCommand(dir), { signal });
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
			verbose: Type.Optional(Type.Boolean({ description: "Stream the full per-file itemized list. Default false: only a one-line summary (files, bytes, time) to save context." })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const from = theme.fg("accent", str(args?.localPath) || ".");
			const to = theme.fg("accent", args?.remotePath ? remoteDisplayPath(str(args.remotePath)) : "remote cwd");
			const dry = args?.dryRun ? ` ${theme.fg("muted", "(dry-run)")}` : "";
			return sshTitle("push", `${from} ${theme.fg("muted", "\u2192")} ${to}${dry}`, theme, context);
		},
		async execute(_id, params: { localPath?: string; remotePath?: string; delete?: boolean; dryRun?: boolean; excludes?: string[]; verbose?: boolean }, signal, onUpdate) {
			const t = requireTarget();
			const localSource = ensureTrailingSlash(resolve(localCwd, params.localPath ?? "."));
			const remoteDest = ensureTrailingSlash(params.remotePath ? toRemotePath(params.remotePath, localCwd, t.remoteCwd) : t.remoteCwd);
			const verbose = params.verbose ?? false;
			const fallback = `Pushed ${localSource} -> ${t.remote}:${remoteDest}`;
			const { stdout, elapsedMs } = await runRsyncTransfer(
				t,
				localSource,
				`${t.remote}:${remoteDest}`,
				{ delete: params.delete, dryRun: params.dryRun, excludes: params.excludes, gitignore: true, verbose },
				signal,
				verbose ? rsyncStreamer(onUpdate) : undefined,
			);
			const prefix = params.dryRun ? "[dry-run] " : "";
			const body = verbose ? (stdout || fallback) : `${summarizeRsync(stdout, fallback)} in ${formatDuration(elapsedMs)}`;
			return { content: [{ type: "text" as const, text: `${prefix}${body}` }] };
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
			verbose: Type.Optional(Type.Boolean({ description: "Stream the full per-file itemized list. Default false: only a one-line summary (files, bytes, time) to save context." })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const from = theme.fg("accent", args?.remotePath ? remoteDisplayPath(str(args.remotePath)) : "remote cwd");
			const to = theme.fg("accent", str(args?.localPath) || ".");
			const dry = args?.dryRun ? ` ${theme.fg("muted", "(dry-run)")}` : "";
			return sshTitle("pull", `${from} ${theme.fg("muted", "\u2192")} ${to}${dry}`, theme, context);
		},
		async execute(_id, params: { remotePath?: string; localPath?: string; delete?: boolean; dryRun?: boolean; excludes?: string[]; verbose?: boolean }, signal, onUpdate) {
			const t = requireTarget();
			const remoteSource = ensureTrailingSlash(params.remotePath ? toRemotePath(params.remotePath, localCwd, t.remoteCwd) : t.remoteCwd);
			const localDest = ensureTrailingSlash(resolve(localCwd, params.localPath ?? "."));
			const verbose = params.verbose ?? false;
			const fallback = `Pulled ${t.remote}:${remoteSource} -> ${localDest}`;
			const { stdout, elapsedMs } = await runRsyncTransfer(
				t,
				`${t.remote}:${remoteSource}`,
				localDest,
				{ delete: params.delete, dryRun: params.dryRun, excludes: params.excludes, gitignore: false, verbose },
				signal,
				verbose ? rsyncStreamer(onUpdate) : undefined,
			);
			const prefix = params.dryRun ? "[dry-run] " : "";
			const body = verbose ? (stdout || fallback) : `${summarizeRsync(stdout, fallback)} in ${formatDuration(elapsedMs)}`;
			return { content: [{ type: "text" as const, text: `${prefix}${body}` }] };
		},
	});

	// ---------------------------------------------------------------------------
	// Port forwarding (ssh_tunnel) — local port <- remote port over the master
	// ---------------------------------------------------------------------------
	interface TunnelState { localPort: number; remoteHost: string; remotePort: number; spec: string; }
	const tunnels = new Map<number, TunnelState>();

	function tunnelControl(t: SshTarget, action: "forward" | "cancel", spec: string): Promise<RunResult> {
		// `ssh -O forward/cancel -L <spec> host` asks the running ControlMaster to open
		// or close a forward without spawning a new session.
		return runSsh([...t.sshOptions, ...baseSshOptions(t.socket), "-O", action, "-L", spec, "--", t.remote], { timeout: 10 });
	}

	function stopAllTunnels(): void {
		if (!target) {
			tunnels.clear();
			return;
		}
		for (const tn of tunnels.values()) void tunnelControl(target, "cancel", tn.spec).catch(() => {});
		tunnels.clear();
	}

	pi.registerTool({
		name: "ssh_tunnel",
		label: "ssh_tunnel",
		description: "Port-forward a remote port to a local port over the shared SSH connection, so a remote dev server / TensorBoard / Jupyter / web UI is reachable from the local browser at http://localhost:<localPort>. Actions: open | close | list. Tunnels are closed automatically on disconnect.",
		promptSnippet: "Forward a remote port to localhost over SSH",
		promptGuidelines: ["Use ssh_tunnel open to view a remote web UI / dev server / TensorBoard locally; close it with ssh_tunnel close when done."],
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

	// ---------------------------------------------------------------------------
	// Auto-sync (ssh_sync) — debounced rsync of the local workspace on change
	// ---------------------------------------------------------------------------
	interface SyncState {
		watcher: FSWatcher;
		localSource: string;
		remoteDest: string;
		remote: string;
		debounceMs: number;
		delete: boolean;
		excludes?: string[];
		timer: NodeJS.Timeout | null;
		syncing: boolean;
		pending: boolean;
		count: number;
	}
	let syncState: SyncState | null = null;

	function stopSyncWatcher(): void {
		if (!syncState) return;
		if (syncState.timer) clearTimeout(syncState.timer);
		try { syncState.watcher.close(); } catch { /* already closed */ }
		syncState = null;
	}

	async function runSync(s: SyncState): Promise<void> {
		if (!target || target.remote !== s.remote) { stopSyncWatcher(); return; }
		if (s.syncing) { s.pending = true; return; }
		s.syncing = true;
		try {
			await runRsyncTransfer(target, s.localSource, `${s.remote}:${s.remoteDest}`, { delete: s.delete, excludes: s.excludes, gitignore: true, quiet: true });
			s.count += 1;
		} catch (e) {
			emit(`⚠️ ssh_sync failed: ${e instanceof Error ? e.message : String(e)}`, { kind: "sync-error" });
		} finally {
			s.syncing = false;
			if (s.pending) { s.pending = false; scheduleSync(s); }
		}
	}

	function scheduleSync(s: SyncState): void {
		if (s.timer) clearTimeout(s.timer);
		s.timer = setTimeout(() => { s.timer = null; void runSync(s); }, s.debounceMs);
		s.timer.unref?.();
	}

	// Start (or restart) the auto-sync watcher. Shared by the ssh_sync tool and the
	// /ssh dashboard's sync toggle so there is one copy of the rsync+watch setup.
	async function startSync(
		t: SshTarget,
		params: { localPath?: string; remotePath?: string; debounceMs?: number; delete?: boolean; excludes?: string[] },
	): Promise<{ localSource: string; remoteDest: string; debounceMs: number; initialTail: string }> {
		stopSyncWatcher();
		const localSource = ensureTrailingSlash(resolve(localCwd, params.localPath ?? "."));
		const remoteDest = ensureTrailingSlash(params.remotePath ? toRemotePath(params.remotePath, localCwd, t.remoteCwd) : t.remoteCwd);
		// Initial full sync so the remote starts in lockstep.
		const initial = await runRsyncTransfer(t, localSource, `${t.remote}:${remoteDest}`, { delete: params.delete, excludes: params.excludes, gitignore: true }, undefined);
		const debounceMs = Math.max(50, Math.floor(params.debounceMs ?? 400));
		let watcher: FSWatcher;
		try {
			watcher = watch(resolve(localCwd, params.localPath ?? "."), { recursive: true });
		} catch (e) {
			throw new Error(`ssh_sync could not watch ${localSource}: ${e instanceof Error ? e.message : String(e)} (recursive fs.watch requires macOS/Windows; on Linux use ssh_push)`);
		}
		const s: SyncState = { watcher, localSource, remoteDest, remote: t.remote, debounceMs, delete: params.delete ?? false, excludes: params.excludes, timer: null, syncing: false, pending: false, count: 0 };
		watcher.on("change", (_event, file) => {
			// Cheap noise filter: skip VCS/build churn that rsync would exclude anyway.
			const name = typeof file === "string" ? file : file?.toString() ?? "";
			if (/(^|\/)\.git\/|(^|\/)node_modules\/|~$|\.swp$/.test(name)) return;
			scheduleSync(s);
		});
		watcher.on("error", (e) => emit(`⚠️ ssh_sync watcher error: ${e instanceof Error ? e.message : String(e)}`, { kind: "sync-error" }));
		syncState = s;
		return { localSource, remoteDest, debounceMs, initialTail: initial.stdout.split("\n").slice(-3).join("\n") };
	}

	pi.registerTool({
		name: "ssh_sync",
		label: "ssh_sync",
		description: "Auto-sync the local workspace to the active SSH remote on every local file change (debounced rsync, .gitignore-filtered). Removes the manual ssh_push step from the edit-locally/run-remotely loop. Actions: start | stop | status. Only one watcher at a time; stops on disconnect.",
		promptSnippet: "Continuously rsync local edits to the remote on change",
		promptGuidelines: ["Use ssh_sync start to keep the remote in lockstep with local edits instead of calling ssh_push after every change; stop it when done."],
		parameters: Type.Object({
			action: Type.Union([Type.Literal("start"), Type.Literal("stop"), Type.Literal("status")]),
			localPath: Type.Optional(Type.String({ description: "Local path to watch+sync, defaults to current workspace" })),
			remotePath: Type.Optional(Type.String({ description: "Remote destination path, defaults to remote cwd" })),
			debounceMs: Type.Optional(Type.Number({ description: "Quiet period after a change before syncing (default 400)" })),
			delete: Type.Optional(Type.Boolean({ description: "Mirror local deletions to the remote (default false)" })),
			excludes: Type.Optional(Type.Array(Type.String(), { description: "Additional rsync exclude patterns" })),
		}),
		renderCall(args: any, theme: any, context: any) {
			const a = str(args?.action) || "?";
			const where = a === "start" ? ` ${theme.fg("muted", str(args?.localPath) || ".")}` : "";
			return sshTitle("sync", `${theme.fg("accent", a)}${where}`, theme, context);
		},
		async execute(_id, params: { action: "start" | "stop" | "status"; localPath?: string; remotePath?: string; debounceMs?: number; delete?: boolean; excludes?: string[] }) {
			const t = requireTarget();
			if (params.action === "status") {
				if (!syncState) return { content: [{ type: "text" as const, text: "ssh_sync: not running." }] };
				return { content: [{ type: "text" as const, text: `ssh_sync: watching ${syncState.localSource} -> ${syncState.remote}:${syncState.remoteDest} (${syncState.count} syncs so far)` }] };
			}
			if (params.action === "stop") {
				const was = syncState ? `${syncState.count}` : null;
				stopSyncWatcher();
				return { content: [{ type: "text" as const, text: was === null ? "ssh_sync was not running." : `ssh_sync stopped (${was} syncs).` }] };
			}
			// start
			const { localSource, remoteDest, debounceMs, initialTail } = await startSync(t, params);
			return { content: [{ type: "text" as const, text: `ssh_sync started: ${localSource} -> ${t.remote}:${remoteDest} (debounce ${debounceMs}ms).\n${initialTail}` }] };
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
			if (tunnels.size) {
				const tn = [...tunnels.values()].map((x) => `localhost:${x.localPort}\u2192${x.remoteHost}:${x.remotePort}`).join(", ");
				push(`    ${T.fg("dim", "tunnels")}  ${T.fg("muted", tn)}`);
			}
			if (syncState) push(`    ${T.fg("dim", "sync")}  ${T.fg("muted", `watching ${syncState.localSource} (${syncState.count} syncs)`)}`);
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
			stopPoller(r.id);
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
			if (syncState) {
				stopSyncWatcher();
				this.tui.requestRender();
				return;
			}
			try {
				await startSync(t, {});
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
