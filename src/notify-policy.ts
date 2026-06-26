// ---------------------------------------------------------------------------
// NotifyPolicy: the transport-agnostic notification gate (MONITOR_PLAN §5.5).
// Maps raw monitor matches → notifications under a configurable policy, so a
// noisy stream (e.g. [1/50] DONE … [50/50] DONE) collapses into one digest /
// four milestones / a throttled trickle instead of 50 pings.
//
// Pure by construction: no SSH, no I/O, no timers, never calls Date.now(). The
// caller injects `now` and applies the returned decision (the monitor engine
// owns emit + the shared scheduler tick). This keeps every policy unit-testable
// and upstreamable. notifyWhen / expression evaluation is Phase 3, not here.
// ---------------------------------------------------------------------------

import { formatDuration } from "./utils";

export type NotifyPolicy =
	| { mode: "every-match" } // == legacy behavior: one notification per match
	| { mode: "every-n"; n: number } // fire once per n matches
	| { mode: "throttle"; minIntervalMs: number } // min gap between fires
	| { mode: "digest"; everyMs: number } // batch; flush on the scheduler tick
	| { mode: "milestone"; fractions: number[] }; // progress crossings (needs a `total` capture)

export interface MatchEvent {
	line: string;
	captures: Record<string, string>; // named groups from re.exec (may be empty)
	matchCount: number; // cumulative match count (post-increment)
	now: number; // epoch ms, injected
}

export interface GateDecision {
	fire: boolean;
	text?: string; // notification body when fire=true (engine adds the prefix)
	details?: Record<string, unknown>; // extra emit details
}

export interface NotifyGate {
	onMatch(ev: MatchEvent): GateDecision; // per matching line
	onTick(now: number): GateDecision; // each scheduler tick (digest flush)
	onClose(now: number): GateDecision; // monitor stopping (flush remainder)
}

const NO_FIRE: GateDecision = { fire: false };

/** Parse a duration like `500ms` / `90s` / `5m` / `1h` (bare number = ms) to ms. */
export function parseDuration(s: string): number {
	const m = /^(\d+(?:\.\d+)?)(ms|s|m|h)?$/.exec(s.trim());
	if (!m) throw new Error(`Invalid duration "${s}" (use e.g. 500ms, 90s, 5m, 1h)`);
	const value = Number.parseFloat(m[1]);
	const unit = m[2] ?? "ms";
	const mult = unit === "h" ? 3_600_000 : unit === "m" ? 60_000 : unit === "s" ? 1_000 : 1;
	const ms = Math.round(value * mult);
	if (ms <= 0) throw new Error(`Duration must be > 0: "${s}"`);
	return ms;
}

/** Parse the `notify=` tool argument into a NotifyPolicy. Empty/omitted ⇒ every-match. */
export function parseNotifyPolicy(spec: string | undefined): NotifyPolicy {
	const s = (spec ?? "").trim();
	if (s === "" || s === "every-match") return { mode: "every-match" };
	const colon = s.indexOf(":");
	const head = colon === -1 ? s : s.slice(0, colon);
	const arg = colon === -1 ? "" : s.slice(colon + 1).trim();
	switch (head) {
		case "every-n": {
			const n = Number.parseInt(arg, 10);
			if (!Number.isInteger(n) || n < 1) throw new Error(`every-n requires a positive integer, got "${arg}"`);
			return { mode: "every-n", n };
		}
		case "throttle":
			return { mode: "throttle", minIntervalMs: parseDuration(arg) };
		case "digest":
			return { mode: "digest", everyMs: parseDuration(arg) };
		case "milestone": {
			const fractions = arg
				.split(",")
				.map((x) => x.trim())
				.filter(Boolean)
				.map((x) => {
					const f = Number.parseFloat(x);
					if (!Number.isFinite(f) || f <= 0 || f > 1) throw new Error(`milestone fractions must be in (0,1], got "${x}"`);
					return f;
				});
			if (fractions.length === 0) throw new Error("milestone requires at least one fraction, e.g. milestone:0.5,1.0");
			fractions.sort((a, b) => a - b);
			return { mode: "milestone", fractions };
		}
		default:
			throw new Error(`Unknown notify policy "${spec}" (use every-match | every-n:N | throttle:DUR | digest:DUR | milestone:f1,f2,…)`);
	}
}

/** Substitute `{name}` tokens from vars; unknown tokens are left literal. */
export function renderTemplate(tpl: string, vars: Record<string, string | number>): string {
	return tpl.replace(/\{(\w+)\}/g, (whole, key: string) => (key in vars ? String(vars[key]) : whole));
}

// Progress/ETA derived from a `total` capture (and optional `n`), relative to the
// gate's first-match time. Returns nothing useful when `total` is absent/invalid.
function progressVars(
	captures: Record<string, string>,
	matchCount: number,
	startAt: number | null,
	now: number,
): { pct?: number; eta?: string; total?: number; done: number } {
	const total = Number.parseFloat(captures.total ?? "");
	const done = captures.n !== undefined ? Number.parseFloat(captures.n) : matchCount;
	if (!Number.isFinite(total) || total <= 0 || !Number.isFinite(done)) return { done: Number.isFinite(done) ? done : matchCount };
	const pct = Math.round((done / total) * 100);
	let eta: string | undefined;
	if (startAt !== null && done > 0 && done < total && now > startAt) {
		const rate = done / (now - startAt); // matches per ms
		eta = formatDuration((total - done) / rate);
	}
	return { pct, eta, total, done };
}

/**
 * Build a stateful notify gate for one monitor. All policy state (counters,
 * lastFireAt, digest buffer, fired-milestone set) lives in this closure; nothing
 * is persisted (counters reset on reconnect, like the monitor's matchCount).
 */
export function makeNotifyGate(policy: NotifyPolicy, opts?: { template?: string }): NotifyGate {
	const tpl = opts?.template;
	const render = (def: string, vars: Record<string, string | number>): string => (tpl ? renderTemplate(tpl, vars) : def);
	const baseVars = (ev: MatchEvent, extra?: Record<string, string | number>): Record<string, string | number> => ({
		...ev.captures,
		line: ev.line,
		matchCount: ev.matchCount,
		...extra,
	});

	switch (policy.mode) {
		case "every-match":
			return {
				onMatch: (ev) => ({ fire: true, text: render(ev.line, baseVars(ev, { count: 1 })) }),
				onTick: () => NO_FIRE,
				onClose: () => NO_FIRE,
			};

		case "every-n": {
			const n = policy.n;
			return {
				onMatch: (ev) => {
					if (ev.matchCount % n !== 0) return NO_FIRE;
					return { fire: true, text: render(`[${ev.matchCount} matches] ${ev.line}`, baseVars(ev, { count: n })), details: { batchedCount: n } };
				},
				onTick: () => NO_FIRE,
				onClose: () => NO_FIRE,
			};
		}

		case "throttle": {
			const ms = policy.minIntervalMs;
			let lastFireAt = Number.NEGATIVE_INFINITY;
			let suppressed = 0;
			return {
				onMatch: (ev) => {
					if (ev.now - lastFireAt < ms) {
						suppressed++;
						return NO_FIRE;
					}
					const sup = suppressed;
					lastFireAt = ev.now;
					suppressed = 0;
					const def = sup > 0 ? `${ev.line} (+${sup} more since last)` : ev.line;
					return { fire: true, text: render(def, baseVars(ev, { suppressed: sup, count: sup + 1 })), details: { suppressed: sup } };
				},
				onTick: () => NO_FIRE,
				onClose: () => NO_FIRE,
			};
		}

		case "digest": {
			const everyMs = policy.everyMs;
			let count = 0;
			let firstAt: number | null = null; // first match of the current batch's window origin
			let lastFlushAt: number | null = null;
			let lastLine = "";
			let lastCaptures: Record<string, string> = {};
			let lastMatchCount = 0;
			const flush = (now: number): GateDecision => {
				if (count === 0) return NO_FIRE;
				const p = progressVars(lastCaptures, lastMatchCount, firstAt, now);
				const c = count;
				const progressLabel = p.pct !== undefined ? ` · ${p.pct}%` : "";
				const etaLabel = p.eta ? ` · ETA ${p.eta}` : "";
				const def = `${c} match${c === 1 ? "" : "es"}${progressLabel}${etaLabel}\nlatest: ${lastLine}`;
				const vars: Record<string, string | number> = { ...lastCaptures, count: c, matchCount: lastMatchCount, line: lastLine };
				if (p.pct !== undefined) vars.pct = p.pct;
				if (p.total !== undefined) vars.total = p.total;
				if (p.eta) vars.eta = p.eta;
				count = 0;
				lastFlushAt = now;
				return { fire: true, text: render(def, vars), details: { count: c } };
			};
			return {
				onMatch: (ev) => {
					count++;
					lastLine = ev.line;
					lastCaptures = ev.captures;
					lastMatchCount = ev.matchCount;
					if (firstAt === null) firstAt = ev.now;
					if (lastFlushAt === null) lastFlushAt = ev.now; // window starts at the first match
					return NO_FIRE;
				},
				onTick: (now) => (count > 0 && lastFlushAt !== null && now - lastFlushAt >= everyMs ? flush(now) : NO_FIRE),
				onClose: (now) => flush(now),
			};
		}

		case "milestone": {
			const fractions = [...policy.fractions];
			const fired = new Set<number>();
			let startAt: number | null = null;
			return {
				onMatch: (ev) => {
					if (startAt === null) startAt = ev.now;
					const p = progressVars(ev.captures, ev.matchCount, startAt, ev.now);
					if (p.total === undefined) return NO_FIRE; // no total → progress undefined, cannot milestone
					const progress = p.done / p.total;
					let hit: number | null = null;
					for (const f of fractions) {
						if (progress >= f && !fired.has(f)) {
							fired.add(f);
							hit = f; // keep the highest crossed fraction this match
						}
					}
					if (hit === null) return NO_FIRE;
					const etaLabel = p.eta ? ` · ETA ${p.eta}` : "";
					const def = `reached ${Math.round(hit * 100)}% (${p.done}/${p.total})${etaLabel}`;
					const vars: Record<string, string | number> = { ...ev.captures, matchCount: ev.matchCount, line: ev.line, pct: p.pct ?? 0, total: p.total };
					if (p.eta) vars.eta = p.eta;
					return { fire: true, text: render(def, vars), details: { milestone: hit, pct: p.pct } };
				},
				onTick: () => NO_FIRE,
				onClose: () => NO_FIRE,
			};
		}
	}
}
