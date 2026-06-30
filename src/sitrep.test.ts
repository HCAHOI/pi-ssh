import { test } from "node:test";
import assert from "node:assert/strict";
import { formatMonitorSection, formatProcessSection, formatSyncSection, formatTunnelSection } from "./sitrep";
import type { MonitorRow } from "./monitor";
import type { ProcRow } from "./process-queries";

test("sitrep: process section summarizes running and finished jobs", () => {
	const rows: ProcRow[] = [
		{ id: "m0000000-abcdef", name: "train", status: "running", code: null, pid: "123", startedMs: 1000 },
		{ id: "m0000001-fedcba", name: "lint", status: "exited", code: 0, pid: "456", startedMs: 2000 },
	];
	const out = formatProcessSection(rows, 4000).join("\n");
	assert.match(out, /Processes \(1 running, 1 finished\)/);
	assert.match(out, /▶ train .*running pid=123/);
	assert.match(out, /✓ lint .*exit=0/);
});

test("sitrep: empty sections stay compact", () => {
	assert.deepEqual(formatProcessSection([]), ["Processes: none"]);
	assert.deepEqual(formatMonitorSection([]), ["Monitors: none"]);
	assert.deepEqual(formatTunnelSection([]), ["Tunnels: none"]);
	assert.deepEqual(formatSyncSection(null), ["Sync: not running"]);
});

test("sitrep: monitor/tunnel/sync rows are grep-friendly", () => {
	const monitors: MonitorRow[] = [{
		id: "mon_1",
		kind: "standalone",
		source: "process:abc:stdout",
		pattern: "step (?<n>\\d+)",
		repeat: true,
		notify: "milestone:0.5,1",
		paused: false,
		fired: false,
		matchCount: 7,
		name: "train",
	}];
	assert.match(formatMonitorSection(monitors).join("\n"), /mon_1 train active process:abc:stdout/);
	assert.match(formatMonitorSection(monitors).join("\n"), /matches=7/);
	assert.match(formatTunnelSection([{ localPort: 8077, remoteHost: "127.0.0.1", remotePort: 8077 }]).join("\n"), /localhost:8077 -> 127.0.0.1:8077/);
	assert.match(formatSyncSection({ localSource: "/repo/", remote: "user@host", remoteDest: "/work/", count: 3 }).join("\n"), /3 syncs/);
});
