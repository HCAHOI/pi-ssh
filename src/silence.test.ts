// Unit tests for the pure silence/stall predicate (MONITOR_PHASE3_PLAN §7 3d).
// Run: npx -y tsx --test src/silence.test.ts
// silenceDue is pure (now injected), so the timing logic is table-testable.

import { test } from "node:test";
import assert from "node:assert/strict";
import { silenceDue } from "./monitor";

test("no window set → never silent", () => {
	assert.equal(silenceDue({ silenceFired: false, lastMatchAt: 0, startedAt: 0 }, 1_000_000), false);
	assert.equal(silenceDue({ expectEveryMs: 0, silenceFired: false, lastMatchAt: 0, startedAt: 0 }, 1_000_000), false);
});

test("already fired → stays latched until re-armed", () => {
	assert.equal(silenceDue({ expectEveryMs: 1000, silenceFired: true, lastMatchAt: 0, startedAt: 0 }, 1_000_000), false);
});

test("fires once the gap since last match exceeds the window", () => {
	const base = { expectEveryMs: 5000, silenceFired: false, lastMatchAt: 10_000, startedAt: 0 };
	assert.equal(silenceDue(base, 14_000), false); // 4s gap < 5s
	assert.equal(silenceDue(base, 15_000), false); // exactly 5s is not > 5s
	assert.equal(silenceDue(base, 15_001), true); // 5.001s > 5s
});

test("before the first match the window is measured from creation", () => {
	const base = { expectEveryMs: 5000, silenceFired: false, lastMatchAt: null, startedAt: 1000 };
	assert.equal(silenceDue(base, 5500), false); // 4.5s since creation
	assert.equal(silenceDue(base, 6500), true); // 5.5s since creation
});
