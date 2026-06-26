// ---------------------------------------------------------------------------
// Remote operation factories (read/write/edit/ls/find/grep/bash) + in-place edit
// ---------------------------------------------------------------------------

import { spawn } from "node:child_process";
import type {
	BashOperations,
	EditOperations,
	FindOperations,
	LsOperations,
	ReadOperations,
	WriteOperations,
} from "@earendil-works/pi-coding-agent";
import type { EditResult, RunResult, SshTarget } from "./types";
import { grepArgs, shQuote, stripTrailingSlash, toRemotePath, withFileLock } from "./utils";
import {
	baseSshOptions,
	closeMaster,
	isRetryableSshFailure,
	remoteShell,
	runRemoteCommand,
	sshConnArgs,
	sshExec,
	sshFailureMessage,
} from "./ssh/transport";

export function createRemoteReadOps(t: SshTarget, localCwd: string): ReadOperations {
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

export function createRemoteWriteOps(t: SshTarget, localCwd: string, lockWrites = true): WriteOperations {
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

export function createRemoteEditOps(t: SshTarget, localCwd: string, lockWrites = true): EditOperations {
	const r = createRemoteReadOps(t, localCwd);
	const w = createRemoteWriteOps(t, localCwd, lockWrites);
	return { readFile: r.readFile, access: r.access, writeFile: w.writeFile };
}

export function createRemoteLsOps(t: SshTarget, localCwd: string): LsOperations {
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

export function createRemoteFindOps(t: SshTarget, localCwd: string): FindOperations {
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

export async function runRemoteGrep(
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

export function createRemoteBashOps(t: SshTarget, localCwd: string, opts?: { tty?: boolean }): BashOperations {
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

export async function remotePatchEdit(
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
