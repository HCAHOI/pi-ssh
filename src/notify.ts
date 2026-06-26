// ---------------------------------------------------------------------------
// Agent-facing notification sink (re-engages the agent via a follow-up turn)
// ---------------------------------------------------------------------------

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/** Push a message that re-engages the agent (completion / log-watch / sync alerts). */
export function sendProcessMessage(pi: ExtensionAPI, content: string, details: Record<string, unknown>): void {
	pi.sendMessage(
		{ customType: "ssh-process", content, display: true, details },
		{ triggerTurn: true, deliverAs: "followUp" },
	);
}
