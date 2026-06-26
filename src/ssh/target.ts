// ---------------------------------------------------------------------------
// Connection management: resolving an SshTarget from a remote spec
// ---------------------------------------------------------------------------

import { randomBytes } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Activation, SshTarget } from "../types";
import { shQuote } from "../utils";
import { baseSshOptions, probePython, remoteShell, runSsh, sshFailureMessage } from "./transport";

export async function resolveTarget(remote: string, path?: string, sshOptions: string[] = [], activation?: Activation): Promise<SshTarget> {
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
export async function sshExecRaw(socket: string, remote: string, sshOptions: string[], command: string): Promise<Buffer> {
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
