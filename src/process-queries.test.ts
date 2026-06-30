import { test } from "node:test";
import assert from "node:assert/strict";
import { resolveProcessRef, type ProcRow } from "./process-queries";

const rows: ProcRow[] = [
	{ id: "m0000001-oldrun", name: "train", status: "running", code: null, pid: "111", startedMs: 1 },
	{ id: "m0000003-done", name: "train", status: "exited", code: 0, pid: "333", startedMs: 3 },
	{ id: "m0000002-newrun", name: "train", status: "running", code: null, pid: "222", startedMs: 2 },
	{ id: "m0000004-eval", name: "eval", status: "running", code: null, pid: "444", startedMs: 4 },
];

test("resolveProcessRef returns explicit ids unchanged", () => {
	assert.deepEqual(resolveProcessRef(rows, { id: "abc", name: "train" }), { id: "abc", matchedBy: "id" });
});

test("resolveProcessRef resolves a name to the newest matching run", () => {
	const resolved = resolveProcessRef(rows, { name: "train" });
	assert.equal(resolved.id, "m0000003-done");
	assert.equal(resolved.matchedBy, "name");
});

test("resolveProcessRef can prefer running jobs for kill-by-name", () => {
	const resolved = resolveProcessRef(rows, { name: "train" }, { preferRunning: true });
	assert.equal(resolved.id, "m0000002-newrun");
});

test("resolveProcessRef throws a useful error for unknown names", () => {
	assert.throws(() => resolveProcessRef(rows, { name: "missing" }), /No remote process named "missing"/);
});
