// Unit tests for the pure NotifyPolicy gate (MONITOR_PHASE2_PLAN §8).
// Run: npx -y tsx --test src/notify-policy.test.ts
// No SSH / no I/O — the gate is pure, `now` is injected, so policies are
// table-testable.

import { test } from "node:test";
import assert from "node:assert/strict";
import { makeNotifyGate, parseDuration, parseNotifyPolicy, renderTemplate, type MatchEvent } from "./notify-policy";

const ev = (over: Partial<MatchEvent> & { matchCount: number; now: number }): MatchEvent => ({
	line: over.line ?? `line ${over.matchCount}`,
	captures: over.captures ?? {},
	matchCount: over.matchCount,
	now: over.now,
});

// --- parseDuration ---------------------------------------------------------
test("parseDuration: units and bare ms", () => {
	assert.equal(parseDuration("500ms"), 500);
	assert.equal(parseDuration("90s"), 90_000);
	assert.equal(parseDuration("5m"), 300_000);
	assert.equal(parseDuration("1h"), 3_600_000);
	assert.equal(parseDuration("250"), 250);
});

test("parseDuration: rejects junk and non-positive", () => {
	assert.throws(() => parseDuration("soon"));
	assert.throws(() => parseDuration("0s"));
	assert.throws(() => parseDuration("-5m"));
});

// --- parseNotifyPolicy -----------------------------------------------------
test("parseNotifyPolicy: defaults to every-match", () => {
	assert.deepEqual(parseNotifyPolicy(undefined), { mode: "every-match" });
	assert.deepEqual(parseNotifyPolicy(""), { mode: "every-match" });
	assert.deepEqual(parseNotifyPolicy("every-match"), { mode: "every-match" });
});

test("parseNotifyPolicy: each policy form", () => {
	assert.deepEqual(parseNotifyPolicy("every-n:10"), { mode: "every-n", n: 10 });
	assert.deepEqual(parseNotifyPolicy("throttle:60s"), { mode: "throttle", minIntervalMs: 60_000 });
	assert.deepEqual(parseNotifyPolicy("digest:5m"), { mode: "digest", everyMs: 300_000 });
	assert.deepEqual(parseNotifyPolicy("milestone:0.5,0.25,1.0"), { mode: "milestone", fractions: [0.25, 0.5, 1.0] });
});

test("parseNotifyPolicy: validation", () => {
	assert.throws(() => parseNotifyPolicy("every-n:0"));
	assert.throws(() => parseNotifyPolicy("every-n:-3"));
	assert.throws(() => parseNotifyPolicy("milestone:1.5"));
	assert.throws(() => parseNotifyPolicy("milestone:"));
	assert.throws(() => parseNotifyPolicy("bogus:1"));
});

// --- renderTemplate --------------------------------------------------------
test("renderTemplate: substitutes known, leaves unknown literal", () => {
	assert.equal(renderTemplate("progress {n}/{total}", { n: 3, total: 50 }), "progress 3/50");
	assert.equal(renderTemplate("{missing} here", {}), "{missing} here");
});

// --- every-match -----------------------------------------------------------
test("every-match: fires every line, text = line", () => {
	const g = makeNotifyGate({ mode: "every-match" });
	for (let i = 1; i <= 3; i++) {
		const d = g.onMatch(ev({ matchCount: i, now: i * 1000 }));
		assert.equal(d.fire, true);
		assert.equal(d.text, `line ${i}`);
	}
	assert.equal(g.onTick(9999).fire, false);
	assert.equal(g.onClose(9999).fire, false);
});

test("every-match: template overrides body", () => {
	const g = makeNotifyGate({ mode: "every-match" }, { template: "L{matchCount}: {line}" });
	assert.equal(g.onMatch(ev({ matchCount: 7, line: "hi", now: 1 })).text, "L7: hi");
});

// --- every-n ---------------------------------------------------------------
test("every-n: fires only on multiples of n", () => {
	const g = makeNotifyGate({ mode: "every-n", n: 3 });
	const fires = [];
	for (let i = 1; i <= 7; i++) fires.push(g.onMatch(ev({ matchCount: i, now: i })).fire);
	assert.deepEqual(fires, [false, false, true, false, false, true, false]);
});

// --- throttle --------------------------------------------------------------
test("throttle: first fires, within-window suppressed, reports backlog on resume", () => {
	const g = makeNotifyGate({ mode: "throttle", minIntervalMs: 1000 });
	const d1 = g.onMatch(ev({ matchCount: 1, now: 0 }));
	assert.equal(d1.fire, true);
	assert.equal(d1.details?.suppressed, 0);
	assert.equal(g.onMatch(ev({ matchCount: 2, now: 300 })).fire, false);
	assert.equal(g.onMatch(ev({ matchCount: 3, now: 600 })).fire, false);
	const d4 = g.onMatch(ev({ matchCount: 4, line: "again", now: 1100 }));
	assert.equal(d4.fire, true);
	assert.equal(d4.details?.suppressed, 2);
	assert.match(d4.text ?? "", /\+2 more since last/);
});

// --- digest ----------------------------------------------------------------
test("digest: buffers, flushes on tick after window, resets, flushes remainder on close", () => {
	const g = makeNotifyGate({ mode: "digest", everyMs: 10_000 });
	assert.equal(g.onMatch(ev({ matchCount: 1, line: "a", now: 0 })).fire, false);
	assert.equal(g.onMatch(ev({ matchCount: 2, line: "b", now: 2000 })).fire, false);
	assert.equal(g.onTick(5000).fire, false); // window not elapsed
	const flush = g.onTick(10_000);
	assert.equal(flush.fire, true);
	assert.equal(flush.details?.count, 2);
	assert.match(flush.text ?? "", /2 matches/);
	assert.match(flush.text ?? "", /latest: b/);
	// buffer reset: nothing pending
	assert.equal(g.onTick(25_000).fire, false);
	// new matches → flush remainder on close
	g.onMatch(ev({ matchCount: 3, line: "c", now: 21_000 }));
	const close = g.onClose(22_000);
	assert.equal(close.fire, true);
	assert.equal(close.details?.count, 1);
	assert.match(close.text ?? "", /latest: c/);
});

test("digest: ETA/progress when total is captured", () => {
	const g = makeNotifyGate({ mode: "digest", everyMs: 1000 });
	g.onMatch(ev({ matchCount: 10, captures: { n: "10", total: "100" }, now: 0 }));
	g.onMatch(ev({ matchCount: 20, captures: { n: "20", total: "100" }, now: 2000 }));
	const d = g.onTick(2000);
	assert.equal(d.fire, true);
	// 20/100 done in 2s → 50% no, 20% ; remaining 80 at 10/s → ~8s ETA
	assert.match(d.text ?? "", /20%/);
	assert.match(d.text ?? "", /ETA/);
});

// --- milestone -------------------------------------------------------------
test("milestone: fires once per crossed fraction, needs total", () => {
	const g = makeNotifyGate({ mode: "milestone", fractions: [0.25, 0.5, 1.0] });
	// no total → never fires
	assert.equal(g.onMatch(ev({ matchCount: 5, captures: {}, now: 0 })).fire, false);
	const at = (n: number, now: number) => g.onMatch(ev({ matchCount: n, captures: { n: String(n), total: "100" }, now }));
	assert.equal(at(10, 100).fire, false); // 10%
	const d25 = at(30, 300); // crosses 25%
	assert.equal(d25.fire, true);
	assert.equal(d25.details?.milestone, 0.25);
	assert.equal(at(40, 400).fire, false); // 40%, no new milestone
	const d50 = at(60, 600); // crosses 50%
	assert.equal(d50.fire, true);
	assert.equal(d50.details?.milestone, 0.5);
	const d100 = at(100, 1000); // crosses 100%
	assert.equal(d100.fire, true);
	assert.equal(d100.details?.milestone, 1.0);
	// already at 100%, no refire
	assert.equal(at(100, 1100).fire, false);
});

test("milestone: a single jump can cross multiple, reports highest", () => {
	const g = makeNotifyGate({ mode: "milestone", fractions: [0.25, 0.5, 0.75] });
	const d = g.onMatch(ev({ matchCount: 80, captures: { n: "80", total: "100" }, now: 500 }));
	assert.equal(d.fire, true);
	assert.equal(d.details?.milestone, 0.75); // highest crossed
});
