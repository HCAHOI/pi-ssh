// Unit tests for the pure notifyWhen expression evaluator (MONITOR_PHASE3_PLAN §7 3c).
// Run: npx -y tsx --test src/expr.test.ts
// No SSH / no I/O — parse+eval over an injected env, so it's fully table-testable.

import { test } from "node:test";
import assert from "node:assert/strict";
import { evalExprSource, parseExpr, type EvalEnv } from "./expr";

const ev = (src: string, env: EvalEnv = {}): boolean => evalExprSource(src, env);

// --- literals & truthiness -------------------------------------------------
test("numeric & string literal truthiness", () => {
	assert.equal(ev("1"), true);
	assert.equal(ev("0"), false);
	assert.equal(ev("0.0"), false);
	assert.equal(ev("'x'"), true);
	assert.equal(ev("''"), false);
	assert.equal(ev('"hi"'), true);
});

// --- comparisons -----------------------------------------------------------
test("numeric comparisons", () => {
	assert.equal(ev("value == 0", { value: 0 }), true);
	assert.equal(ev("value == 0", { value: "0" }), true); // string coerces
	assert.equal(ev("value != 0", { value: 3 }), true);
	assert.equal(ev("value < 10", { value: 5 }), true);
	assert.equal(ev("value <= 5", { value: 5 }), true);
	assert.equal(ev("value > 10", { value: 12.5 }), true);
	assert.equal(ev("value >= 100", { value: 99 }), false);
});

test("string equality vs numeric equality", () => {
	assert.equal(ev("s == 'running'", { s: "running" }), true);
	assert.equal(ev("s == 'running'", { s: "stopped" }), false);
	assert.equal(ev("s != 'running'", { s: "stopped" }), true);
	// mixed: numeric-looking strings compare numerically
	assert.equal(ev("a == b", { a: "1.0", b: 1 }), true);
});

test("loss > threshold (capture string)", () => {
	assert.equal(ev("loss > 10", { loss: "12.5" }), true);
	assert.equal(ev("loss > 10", { loss: "3.2" }), false);
});

// --- precedence & associativity --------------------------------------------
test("comparison binds tighter than &&/||", () => {
	assert.equal(ev("value == 0 && exitCode == 0", { value: 0, exitCode: 0 }), true);
	assert.equal(ev("value == 0 && exitCode == 0", { value: 0, exitCode: 1 }), false);
});

test("&& binds tighter than ||", () => {
	// false && x || true  ==  (false && x) || true  == true
	assert.equal(ev("0 && 0 || 1"), true);
	// true || false && false == true || (false && false) == true
	assert.equal(ev("1 || 0 && 0"), true);
});

test("arithmetic precedence and parens", () => {
	assert.equal(ev("1 + 2 * 3 == 7"), true);
	assert.equal(ev("(1 + 2) * 3 == 9"), true);
	assert.equal(ev("10 / 2 - 3 == 2"), true);
});

test("unary ! and -", () => {
	assert.equal(ev("!0"), true);
	assert.equal(ev("!1"), false);
	assert.equal(ev("!(value == 0)", { value: 0 }), false);
	assert.equal(ev("-value < 0", { value: 5 }), true);
	assert.equal(ev("!!1"), true);
});

test("short-circuit && / ||", () => {
	// undefined identifier on the unreached side must not affect the result
	assert.equal(ev("0 && missing"), false);
	assert.equal(ev("1 || missing"), true);
});

// --- undefined / unknown identifiers ---------------------------------------
test("unknown identifier is undefined → falsey, never throws at eval", () => {
	assert.equal(ev("missing"), false);
	assert.equal(ev("missing == 0"), false); // undefined == anything → false
	assert.equal(ev("missing != 0"), true); // != → true
	assert.equal(ev("missing > 5"), false); // relational with NaN → false
	assert.equal(ev("missing && 1"), false);
});

test("prototype-chain identifiers resolve to undefined, not inherited members", () => {
	// __proto__/constructor/toString must not leak Object.prototype members (→ false).
	assert.equal(ev("constructor"), false);
	assert.equal(ev("__proto__"), false);
	assert.equal(ev("toString"), false);
	assert.equal(ev("hasOwnProperty == 0"), false);
	// an own property of the same name still resolves normally
	assert.equal(ev("toString == 1", { toString: 1 }), true);
});

test("notifyWhen over {value, exitCode} probe env", () => {
	const env = { value: 0, exitCode: 0, matchCount: 0, elapsedMs: 1000 };
	assert.equal(ev("value == 0", env), true);
	assert.equal(ev("value == 0 && exitCode == 0", env), true);
	assert.equal(ev("exitCode != 0", env), false);
	assert.equal(ev("value > 90", { ...env, value: 95 }), true);
});

// --- division by zero ------------------------------------------------------
test("division by zero → NaN → comparisons false", () => {
	assert.equal(ev("1 / 0 > 0"), false);
	assert.equal(ev("1 / 0 == 0"), false);
});

// --- parse errors (fail fast at create) ------------------------------------
test("syntax errors throw at parse", () => {
	assert.throws(() => parseExpr(""));
	assert.throws(() => parseExpr("value =="));
	assert.throws(() => parseExpr("== 0"));
	assert.throws(() => parseExpr("(value == 0"));
	assert.throws(() => parseExpr("value == 0)"));
	assert.throws(() => parseExpr("value 0"));
	assert.throws(() => parseExpr("value @ 0"));
	assert.throws(() => parseExpr("'unterminated"));
	assert.throws(() => parseExpr("value && "));
});

test("valid expressions parse without throwing", () => {
	for (const s of ["value==0", "value == 0 && exitCode == 0", "loss > 10 || acc < 0.5", "!(a==b)", "(1+2)*3 >= 9", "gpu_util == 0"]) {
		assert.doesNotThrow(() => parseExpr(s));
	}
});

// --- string escapes in literals --------------------------------------------
test("escaped quote inside string literal", () => {
	assert.equal(ev("s == 'it\\'s'", { s: "it's" }), true);
});

// --- whitespace tolerance --------------------------------------------------
test("whitespace is ignored", () => {
	assert.equal(ev("   value   ==   0   ", { value: 0 }), true);
});
