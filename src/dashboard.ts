// ---------------------------------------------------------------------------
// /ssh interactive dashboard (human-facing TUI) + the /ssh slash command.
// Agent tools are unaffected; this is the keyboard-driven overlay only.
// ---------------------------------------------------------------------------

import { Key, matchesKey, type SelectItem, SelectList, truncateToWidth } from "@earendil-works/pi-tui";
import type { SshContext } from "./context";
import { formatDuration, shQuote } from "./utils";
import { runRemoteCommand, sshFailureMessage } from "./ssh/transport";
import { createRemoteBashOps } from "./remote-ops";
import { buildClearCommand, buildKillCommand, listProcesses, type ProcRow, processRoot } from "./process-queries";

export function setupDashboard(ctx: SshContext): void {
	const {
		pi,
		localCwd,
		getTarget,
		requireTarget,
		switchTarget,
		disconnect,
		refreshStatus,
		connectedText,
		statusLabel,
		profileNames,
		profilesPath,
		saveProfile,
		poller,
	} = ctx;

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

	const MONITOR_HELP = `ssh_monitor advanced examples

Simple running-process watch:
  ssh_monitor create source=process:<procId>:stderr pattern='error|failed'

Watch a remote file, logrotate-safe:
  ssh_monitor create source=file:/var/log/app.log pattern='READY|ERROR'

Throttle noisy progress:
  ssh_monitor create source=process:<procId>:stdout pattern='epoch (?<n>\\d+)/(?<total>\\d+)' notify=milestone:0.25,0.5,1.0
  ssh_monitor create source=process:<procId>:stderr pattern='loss=(?<loss>[\\d.]+)' notify=throttle:60s

Probe a metric:
  ssh_monitor create source='probe:nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits' notifyWhen='value==0' intervalMs=60000 consecutive=3

Detect stalls:
  ssh_monitor create source=process:<procId>:stdout pattern='epoch' expectEveryMs=300000

Manage:
  ssh_monitor list | pause/resume/remove id=<mon_id> | attach
`;

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
			const target = getTarget();
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
			const t = getTarget();
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
			const activeTunnels = ctx.tunnels.list();
			if (activeTunnels.length) {
				const tn = activeTunnels.map((x) => `localhost:${x.localPort}\u2192${x.remoteHost}:${x.remotePort}${x.saved ? " [saved]" : ""}`).join(", ");
				push(`    ${T.fg("dim", "tunnels")}  ${T.fg("muted", tn)}`);
			}
			const syncStatus = ctx.sync.getState();
			if (syncStatus) push(`    ${T.fg("dim", "sync")}  ${T.fg("muted", `watching ${syncStatus.localSource} (${syncStatus.count} syncs)`)}`);
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
				if (getTarget()) {
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
			const t = getTarget();
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
			if (getTarget()) items.push({ value: "__off__", label: "disconnect", description: "close the active SSH connection" });
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
			const t = getTarget();
			const r = this.rows[this.sel];
			if (!t || !r) return;
			poller.stopPoller(r.id);
			try {
				await runRemoteCommand(t, buildKillCommand(`${processRoot(t)}/${r.id}`));
			} catch (e) {
				this.err = e instanceof Error ? e.message : String(e);
			}
			await this.poll(true);
		}

		private async clearFinished(): Promise<void> {
			const t = getTarget();
			if (!t) return;
			try {
				await runRemoteCommand(t, buildClearCommand(processRoot(t)));
			} catch (e) {
				this.err = e instanceof Error ? e.message : String(e);
			}
			await this.poll(true);
		}

		private async toggleSync(): Promise<void> {
			const t = getTarget();
			if (!t) return;
			if (ctx.sync.getState()) {
				ctx.sync.stop();
				this.tui.requestRender();
				return;
			}
			try {
				await ctx.sync.startSync(t, {});
			} catch (e) {
				this.err = e instanceof Error ? e.message : String(e);
			}
			this.tui.requestRender();
		}
	}

	async function openSshDashboard(uiCtx: any): Promise<void> {
		if (typeof uiCtx?.ui?.custom !== "function") {
			const t = getTarget();
			uiCtx.ui.notify(t ? statusLabel(t) : "SSH: not connected", "info");
			return;
		}
		await uiCtx.ui.custom(
			(tui: any, theme: any, _kb: any, done: (v: null) => void) => {
				const dash = new SshDashboard(tui, theme, uiCtx, () => {
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
		description: "SSH remote dashboard/connect. Subcommands: status, cd, save, profiles, help monitor, off.",
		handler: async (args, cmdCtx) => {
			const arg = args.trim();

			if (!arg) {
				await openSshDashboard(cmdCtx);
				return;
			}

			if (arg === "status") {
				const t = getTarget();
				cmdCtx.ui.notify(t ? connectedText(t) : "SSH: not connected", "info");
				return;
			}

			if (arg === "off" || arg === "disconnect") {
				await disconnect();
				refreshStatus(cmdCtx);
				cmdCtx.ui.notify("SSH disconnected. Local tools remain local.", "info");
				return;
			}

			if (arg === "help monitor" || arg === "monitor help") {
				cmdCtx.ui.notify(MONITOR_HELP, "info");
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
					refreshStatus(cmdCtx);
					cmdCtx.ui.notify(`SSH cwd -> ${t.remoteCwd}`, "info");
				} catch (e) {
					cmdCtx.ui.notify(`SSH cd failed: ${e instanceof Error ? e.message : String(e)}`, "error");
				}
				return;
			}

			// /ssh profiles: list saved connection handles.
			if (arg === "profiles" || arg === "ls") {
				try {
					const names = profileNames();
					cmdCtx.ui.notify(names.length ? `Saved SSH profiles: ${names.map((n) => `@${n}`).join(", ")}` : `No saved profiles. Connect, then /ssh save <name> (stored in ${profilesPath()}).`, "info");
				} catch (e) {
					cmdCtx.ui.notify(`SSH profiles error: ${e instanceof Error ? e.message : String(e)}`, "error");
				}
				return;
			}

			// /ssh save <name>: persist the active connection string as a profile.
			if (arg.startsWith("save ")) {
				try {
					const name = arg.slice(5).trim();
					if (!name) throw new Error("Usage: /ssh save <name>");
					saveProfile(name);
					cmdCtx.ui.notify(`Saved SSH profile @${name} -> ${profilesPath()}. Reconnect later with /ssh @${name}`, "info");
				} catch (e) {
					cmdCtx.ui.notify(`SSH save failed: ${e instanceof Error ? e.message : String(e)}`, "error");
				}
				return;
			}

			try {
				const next = await switchTarget(arg);
				refreshStatus(cmdCtx);
				cmdCtx.ui.notify(connectedText(next), "info");
			} catch (e) {
				cmdCtx.ui.notify(`SSH connect failed: ${e instanceof Error ? e.message : String(e)}`, "error");
			}
		},
	});
}
