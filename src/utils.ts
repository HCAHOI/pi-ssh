// ---------------------------------------------------------------------------
// Generic helpers (shell quoting, formatting, path mapping, file locks)
// ---------------------------------------------------------------------------

import { isAbsolute, posix, relative, resolve, sep } from "node:path";

export function shQuote(value: string): string {
	return `'${value.replace(/'/g, `'\\''`)}'`;
}

export function stripTrailingSlash(value: string): string {
	return value.length > 1 ? value.replace(/\/+$/, "") : value;
}

export function formatDuration(ms: number): string {
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
export function summarizeRsync(stdout: string, fallback: string): string {
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

export async function withFileLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
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

export function toRemotePath(path: string, localCwd: string, remoteCwd: string): string {
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

export function grepArgs(args: string[]): string {
	return args.map(shQuote).join(" ");
}

export function buildEnvExports(env: Record<string, string> | undefined): string[] {
	if (!env) return [];
	return Object.entries(env).map(([key, value]) => {
		if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
			throw new Error(`Invalid environment variable name for ssh_bash: ${key}`);
		}
		return `export ${key}=${shQuote(value)}`;
	});
}
