// ---------------------------------------------------------------------------
// Exponential-backoff reconnection (opt-in; foreground tools only)
// ---------------------------------------------------------------------------
// On a transient transport failure (isRetryableSshFailure), tear down the dead
// ControlMaster and retry the command — the next ssh invocation auto-respawns the
// master (ControlMaster=auto). With reconnect on, retry up to RECONNECT_MAX_ATTEMPTS
// with exponential backoff and a visible status each attempt; otherwise keep the
// historical retry-once. Only re-runs on transport failures (code 255 + known
// patterns), where the command almost certainly never executed, so re-running is
// safe. Background pollers must fail fast (retry next tick), so they never opt in.

import { AsyncLocalStorage } from "node:async_hooks";

export const RECONNECT_MAX_ATTEMPTS = 10;
export const RECONNECT_BASE_MS = 1000; // delay before the 2nd attempt
export const RECONNECT_FACTOR = 2; // doubles each attempt
export const RECONNECT_CAP_MS = 30_000; // cap on a single backoff delay

export const reconnectCtx = new AsyncLocalStorage<{ reconnect: boolean }>();

export interface ReconnectInfo {
	remote: string;
	attempt: number;
	max: number;
	delayMs: number;
}
export type ReconnectPhase = "retrying" | "recovered" | "gaveup";

let reconnectNotifier: ((phase: ReconnectPhase, info: ReconnectInfo) => void) | null = null;

/** Register the UI-facing reconnect notifier (called from the extension activation). */
export function setReconnectNotifier(fn: ((phase: ReconnectPhase, info: ReconnectInfo) => void) | null): void {
	reconnectNotifier = fn;
}

/** Fire the registered reconnect notifier, if any. */
export function notifyReconnect(phase: ReconnectPhase, info: ReconnectInfo): void {
	reconnectNotifier?.(phase, info);
}

// Enable reconnection for every runRemoteCommand fn triggers. AsyncLocalStorage
// scopes it to fn's async tree, so concurrent background pollers (which run
// outside this context) never inherit it and never block on a long backoff.
export function withReconnect<T>(fn: () => Promise<T>): Promise<T> {
	return reconnectCtx.run({ reconnect: true }, fn);
}

// Backoff (ms) before the given 1-indexed attempt number (>= 2): 1s,2s,4s,8s,16s,30s…
export function backoffDelay(attempt: number): number {
	return Math.min(RECONNECT_CAP_MS, RECONNECT_BASE_MS * RECONNECT_FACTOR ** (attempt - 2));
}

// Resolves true if aborted during the wait, false if it slept the full duration.
export function abortableSleep(ms: number, signal?: AbortSignal): Promise<boolean> {
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
