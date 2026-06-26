// ---------------------------------------------------------------------------
// Tool-call rendering helpers (renderCall titles like `ssh_read [host] path:lines`,
// renderResult diff coloring). Bound to the active target via a getter so titles
// reflect the current connection.
// ---------------------------------------------------------------------------

import { Container, Text } from "@earendil-works/pi-tui";
import type { SshTarget } from "./types";
import { stripTrailingSlash, toRemotePath } from "./utils";

export interface SshRender {
	str(v: unknown): string;
	resultText(r: any): string;
	remoteDisplayPath(raw: string): string;
	accentRemotePath(raw: unknown, theme: any): string;
	readLineRange(args: { offset?: number; limit?: number }, theme: any): string;
	hostTag(theme: any): string;
	sshTitle(label: string, rest: string, theme: any, context: any): Text;
	renderEditDiffResult(result: any, theme: any, context: any): Container | Text;
}

export function createRender(getTarget: () => SshTarget | null, localCwd: string): SshRender {
	function str(v: unknown): string {
		return typeof v === "string" ? v : "";
	}

	function resultText(r: any): string {
		return (r?.content ?? []).map((c: any) => (c?.type === "text" ? c.text : "")).join("");
	}

	// Remote path shown relative to remoteCwd (best-effort; never throws).
	function remoteDisplayPath(raw: string): string {
		const t = getTarget();
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
		const t = getTarget();
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

	return { str, resultText, remoteDisplayPath, accentRemotePath, readLineRange, hostTag, sshTitle, renderEditDiffResult };
}
