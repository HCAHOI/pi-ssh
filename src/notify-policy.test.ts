// Unit tests for the pure NotifyPolicy gate (MONITOR_PHASE2_PLAN §8).
// Run: npx -y tsx --test src/notify-policy.test.ts
// No SSH / no I/O — the gate is pure, `now` is injected, so policies are
// table-testable.

import { test } from "node:test";
import assert from "node:assert/strict";
import { makeNotifyGate, parseDuration, parseNotifyPolicy, policyLabel, renderTemplate, type MatchEvent } from "./notify-policy";

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

test("parseDuration: decimals", () => {
	assert.equal(parseDuration("1.5s"), 1500);
	assert.equal(parseDuration("0.5m"), 30_000);
});

test("policyLabel round-trips through parseNotifyPolicy", () => {
	for (const s of ["every-match", "every-n:50", "throttle:90s", "digest:5m", "milestone:0.25,0.5,1"]) {
		assert.equal(policyLabel(parseNotifyPolicy(s)), s);
	}
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

// --- every-n remainder -----------------------------------------------------
test("every-n: flushes the partial final batch on close, then is idempotent", () => {
	const g = makeNotifyGate({ mode: "every-n", n: 50 });
	for (let i = 1; i <= 67; i++) {
		const d = g.onMatch(ev({ matchCount: i, now: i }));
		assert.equal(d.fire, i === 50);
	}
	const close = g.onClose(100);
	assert.equal(close.fire, true);
	assert.equal(close.details?.batchedCount, 17);
	assert.match(close.text ?? "", /17 more/);
	assert.match(close.text ?? "", /67 total/);
	assert.equal(g.onClose(101).fire, false); // nothing left to flush
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

test("throttle: suppressed tail surfaces on tick after interval, then drains", () => {
	const g = makeNotifyGate({ mode: "throttle", minIntervalMs: 1000 });
	assert.equal(g.onMatch(ev({ matchCount: 1, now: 0 })).fire, true);
	for (let i = 2; i <= 5; i++) assert.equal(g.onMatch(ev({ matchCount: i, now: 100 * i })).fire, false);
	assert.equal(g.onTick(900).fire, false); // interval not elapsed
	const t = g.onTick(1500);
	assert.equal(t.fire, true);
	assert.equal(t.details?.suppressed, 4);
	assert.equal(g.onTick(3000).fire, false); // drained
	assert.equal(g.onClose(4000).fire, false);
});

test("throttle: backlog flushes on close when the job ends muted", () => {
	const g = makeNotifyGate({ mode: "throttle", minIntervalMs: 10_000 });
	g.onMatch(ev({ matchCount: 1, now: 0 })); // fires
	g.onMatch(ev({ matchCount: 2, now: 100 })); // suppressed
	g.onMatch(ev({ matchCount: 3, now: 200 })); // suppressed
	const c = g.onClose(300);
	assert.equal(c.fire, true);
	assert.equal(c.details?.suppressed, 2);
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

test("digest: window restarts at the next batch's first match (no early flush after a gap)", () => {
	const g = makeNotifyGate({ mode: "digest", everyMs: 10_000 });
	g.onMatch(ev({ matchCount: 1, now: 0 }));
	assert.equal(g.onTick(10_000).fire, true); // flush batch 1, window origin reset
	// long quiet gap, then a single new match; the next tick is only 1s into the
	// NEW batch, so it must NOT flush (would have, if origin stayed at last flush).
	g.onMatch(ev({ matchCount: 2, now: 30_000 }));
	assert.equal(g.onTick(31_000).fire, false);
	assert.equal(g.onTick(40_000).fire, true); // 10s into the new batch → flush
});

test("digest: template {eta} never leaks literally (completion + no-total)", () => {
	// at completion (done==total) ETA is 0s, not the literal {eta}
	const g = makeNotifyGate({ mode: "digest", everyMs: 1000 }, { template: "{n}/{total} ETA {eta}" });
	g.onMatch(ev({ matchCount: 48, captures: { n: "48", total: "48" }, now: 1000 }));
	const d = g.onClose(2000);
	assert.equal(d.fire, true);
	assert.doesNotMatch(d.text ?? "", /\{eta\}/);
	assert.match(d.text ?? "", /48\/48 ETA 0s/);
	// no total capture at all → {eta}/{total} resolve to ? not literal braces
	const g2 = makeNotifyGate({ mode: "digest", everyMs: 1000 }, { template: "{total} ETA {eta}" });
	g2.onMatch(ev({ matchCount: 3, captures: {}, now: 0 }));
	const d2 = g2.onClose(1000);
	assert.doesNotMatch(d2.text ?? "", /\{(eta|total)\}/);
});

test("milestone: template {eta} resolves at 100% (no literal leak)", () => {
	const g = makeNotifyGate({ mode: "milestone", fractions: [1.0] }, { template: "{pct}% ETA {eta}" });
	const d = g.onMatch(ev({ matchCount: 50, captures: { n: "50", total: "50" }, now: 500 }));
	assert.equal(d.fire, true);
	assert.doesNotMatch(d.text ?? "", /\{eta\}/);
	assert.match(d.text ?? "", /100% ETA 0s/);
});

test("renderTemplate still leaves a genuine typo token literal", () => {
	const g = makeNotifyGate({ mode: "every-match" }, { template: "{line} {notavar}" });
	assert.match(g.onMatch(ev({ matchCount: 1, line: "x", now: 0 })).text ?? "", /x \{notavar\}/);
});

test("digest: progress derives from matchCount when no n capture", () => {
	const g = makeNotifyGate({ mode: "digest", everyMs: 1000 });
	g.onMatch(ev({ matchCount: 25, captures: { total: "100" }, now: 0 }));
	g.onMatch(ev({ matchCount: 50, captures: { total: "100" }, now: 2000 }));
	const d = g.onTick(2000);
	assert.equal(d.fire, true);
	assert.match(d.text ?? "", /50%/); // 50/100 via matchCount
});

test("gate vars: explicit fields override same-named captures", () => {
	const g = makeNotifyGate({ mode: "every-match" }, { template: "{line}|{matchCount}" });
	const d = g.onMatch(ev({ matchCount: 9, line: "REAL", captures: { line: "CAP", matchCount: "x" }, now: 1 }));
	assert.equal(d.text, "REAL|9"); // ev.line / ev.matchCount win over captures
});

test("digest: ETA rate is cumulative-over-total-elapsed, correct across windows", () => {
	const g = makeNotifyGate({ mode: "digest", everyMs: 1000 });
	g.onMatch(ev({ matchCount: 10, captures: { n: "10", total: "100" }, now: 1000 })); // firstAt=1000
	assert.match(g.onTick(2000).text ?? "", /ETA 9s/); // 10 done in 1s → 90 left → 9s
	g.onMatch(ev({ matchCount: 20, captures: { n: "20", total: "100" }, now: 3000 }));
	// 2nd window: 20 done over 3s elapsed (from first match) → 80 left at 20/3000 → 12s.
	// (Buggy firstAt-reset would compute ~4s.)
	assert.match(g.onTick(4000).text ?? "", /ETA 12s/);
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
