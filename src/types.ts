// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

export interface SshTarget {
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

// logWatches param shape on ssh_process. Each logWatch is desugared into a
// first-class standalone monitor at start (src/monitor.ts createForProcess).
// `notify`/`template` are the raw tool-arg strings (parsed by parseNotifyPolicy).
// Legacy notify.json from pre-upgrade jobs carries only pattern/stream/repeat.
export interface WatchSpec {
	pattern: string;
	stream?: "stdout" | "stderr" | "both";
	repeat?: boolean;
	notify?: string;
	template?: string;
}

export interface PollerState {
	procId: string;
	name: string;
	dir: string;
	target: SshTarget;
	alertOnSuccess: boolean;
	alertOnFailure: boolean;
	alertOnKill: boolean;
	timer: NodeJS.Timeout | null;
	busy: boolean;
	finished: boolean;
	/** Epoch ms when this poller started tracking the job. Known for fresh starts
	 * (used to report run duration); absent for jobs rehydrated after a restart. */
	startedAt?: number;
}

export interface Activation {
	commandPrefix?: string;
	env?: Record<string, string>;
}

export interface RunResult {
	code: number | null;
	signal: NodeJS.Signals | null;
	stdout: Buffer;
	stderr: Buffer;
	timedOut: boolean;
}

export interface RunOptions {
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

export interface EditResult {
	diff: string;
	patch: string;
	firstChangedLine?: number;
}
