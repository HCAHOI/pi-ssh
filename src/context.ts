// ---------------------------------------------------------------------------
// SshContext: the shared surface passed to every tool/subsystem module.
// Built once in index.ts; lets the modules read the active connection, the
// poller, the notification sink, and the connection actions without sharing a
// giant lexical closure.
// ---------------------------------------------------------------------------

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { SshTarget } from "./types";
import type { PollerManager } from "./poller";
import type { SshRender } from "./render";
import type { TunnelManager } from "./tunnels";
import type { SyncManager } from "./sync";

export interface SshContext {
	pi: ExtensionAPI;
	localCwd: string;
	/** Live getter for the active target (null when disconnected). */
	getTarget(): SshTarget | null;
	/** Active target or throw a connect-first error. */
	requireTarget(): SshTarget;
	poller: PollerManager;
	/** Agent-facing notification sink (completion / watch / sync alerts). */
	emit(content: string, details: Record<string, unknown>): void;
	render: SshRender;
	// connection actions (mutate the active target inside index.ts core)
	connect(arg: string): Promise<SshTarget>;
	switchTarget(arg: string): Promise<SshTarget>;
	disconnect(): Promise<void>;
	refreshStatus(toolCtx: any): void;
	connectedText(t: SshTarget): string;
	statusLabel(t: SshTarget | null): string;
	profileNames(): string[];
	profilesPath(): string;
	saveProfile(name: string): void;
	expandProfile(arg: string): string;
	buildSshBashCommand(t: SshTarget, params: { command: string; cwd?: string; delaySeconds?: number; env?: Record<string, string>; commandPrefix?: string }): string;
	// stateful subsystems (attached after creation)
	tunnels: TunnelManager;
	sync: SyncManager;
}
