// ---------------------------------------------------------------------------
// notifyWhen: a tiny, safe expression evaluator (MONITOR_PHASE3_PLAN §2.2, §9).
// A hand-written tokenizer + Pratt parser → AST → evalExpr(env). NO eval / no
// Function — `notifyWhen` is DATA, not code. The variable set is whatever the
// caller puts in `env` (named captures, value, exitCode, matchCount, elapsedMs);
// an unknown identifier resolves to `undefined` (→ false in boolean context),
// never an error or arbitrary access.
//
// Operators: || && (short-circuit), == != < <= > >=, + - * /, unary ! -, parens,
// numeric & string ('…' or "…") literals. Pure and transport-agnostic, so the
// whole thing is table-unit-tested and upstreamable.
// ---------------------------------------------------------------------------

export type Ast =
	| { type: "num"; value: number }
	| { type: "str"; value: string }
	| { type: "ident"; name: string }
	| { type: "unary"; op: "!" | "-"; operand: Ast }
	| { type: "binary"; op: BinOp; left: Ast; right: Ast };

type BinOp = "||" | "&&" | "==" | "!=" | "<" | "<=" | ">" | ">=" | "+" | "-" | "*" | "/";

export type EvalEnv = Record<string, string | number | undefined>;

// --- tokenizer -------------------------------------------------------------

type Tok =
	| { k: "num"; v: number }
	| { k: "str"; v: string }
	| { k: "ident"; v: string }
	| { k: "op"; v: string };

const OPS3: string[] = [];
const OPS2 = ["==", "!=", "<=", ">=", "&&", "||"];
const OPS1 = ["!", "<", ">", "+", "-", "*", "/", "(", ")"];

function tokenize(src: string): Tok[] {
	const toks: Tok[] = [];
	let i = 0;
	const n = src.length;
	while (i < n) {
		const c = src[i];
		if (c === " " || c === "\t" || c === "\n" || c === "\r") {
			i++;
			continue;
		}
		// string literal
		if (c === "'" || c === '"') {
			const quote = c;
			let j = i + 1;
			let s = "";
			while (j < n && src[j] !== quote) {
				if (src[j] === "\\" && j + 1 < n) {
					s += src[j + 1];
					j += 2;
				} else {
					s += src[j];
					j++;
				}
			}
			if (j >= n) throw new Error(`notifyWhen: unterminated string literal in "${src}"`);
			toks.push({ k: "str", v: s });
			i = j + 1;
			continue;
		}
		// number literal (int/float, optional exponent)
		if (c >= "0" && c <= "9") {
			const m = /^\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/.exec(src.slice(i));
			if (!m) throw new Error(`notifyWhen: bad number at "${src.slice(i)}"`);
			toks.push({ k: "num", v: Number.parseFloat(m[0]) });
			i += m[0].length;
			continue;
		}
		// identifier
		if (/[A-Za-z_]/.test(c)) {
			const m = /^[A-Za-z_][A-Za-z0-9_]*/.exec(src.slice(i));
			if (!m) throw new Error(`notifyWhen: bad identifier at "${src.slice(i)}"`);
			toks.push({ k: "ident", v: m[0] });
			i += m[0].length;
			continue;
		}
		// operators (longest match first)
		const two = src.slice(i, i + 2);
		if (OPS2.includes(two)) {
			toks.push({ k: "op", v: two });
			i += 2;
			continue;
		}
		if (OPS1.includes(c)) {
			toks.push({ k: "op", v: c });
			i++;
			continue;
		}
		throw new Error(`notifyWhen: unexpected character "${c}" in "${src}"`);
	}
	return toks;
}

// --- Pratt parser ----------------------------------------------------------

// Binary precedence (higher binds tighter). Unary ! and - bind tighter than all.
const PREC: Record<string, number> = {
	"||": 1,
	"&&": 2,
	"==": 3,
	"!=": 3,
	"<": 4,
	"<=": 4,
	">": 4,
	">=": 4,
	"+": 5,
	"-": 5,
	"*": 6,
	"/": 6,
};

class Parser {
	private pos = 0;
	constructor(
		private toks: Tok[],
		private src: string,
	) {}

	parse(): Ast {
		const ast = this.parseExpr(0);
		if (this.pos < this.toks.length) throw new Error(`notifyWhen: unexpected trailing input "${this.src}"`);
		return ast;
	}

	private peek(): Tok | undefined {
		return this.toks[this.pos];
	}

	private parseExpr(minPrec: number): Ast {
		let left = this.parsePrefix();
		for (;;) {
			const t = this.peek();
			if (!t || t.k !== "op" || !(t.v in PREC)) break;
			const prec = PREC[t.v];
			if (prec < minPrec) break;
			this.pos++;
			// left-associative: parse the rhs at prec+1
			const right = this.parseExpr(prec + 1);
			left = { type: "binary", op: t.v as BinOp, left, right };
		}
		return left;
	}

	private parsePrefix(): Ast {
		const t = this.peek();
		if (!t) throw new Error(`notifyWhen: unexpected end of input in "${this.src}"`);
		if (t.k === "op" && (t.v === "!" || t.v === "-")) {
			this.pos++;
			const operand = this.parsePrefix();
			return { type: "unary", op: t.v, operand };
		}
		if (t.k === "op" && t.v === "(") {
			this.pos++;
			const inner = this.parseExpr(0);
			const close = this.peek();
			if (!close || close.k !== "op" || close.v !== ")") throw new Error(`notifyWhen: missing ")" in "${this.src}"`);
			this.pos++;
			return inner;
		}
		if (t.k === "num") {
			this.pos++;
			return { type: "num", value: t.v };
		}
		if (t.k === "str") {
			this.pos++;
			return { type: "str", value: t.v };
		}
		if (t.k === "ident") {
			this.pos++;
			return { type: "ident", name: t.v };
		}
		throw new Error(`notifyWhen: unexpected token "${t.k === "op" ? t.v : String((t as { v: unknown }).v)}" in "${this.src}"`);
	}
}

/** Parse a notifyWhen expression into an AST. Throws on syntax error (fail fast at create). */
export function parseExpr(src: string): Ast {
	const text = src.trim();
	if (!text) throw new Error("notifyWhen: empty expression");
	const toks = tokenize(text);
	if (toks.length === 0) throw new Error("notifyWhen: empty expression");
	return new Parser(toks, text).parse();
}

// --- evaluation ------------------------------------------------------------

type Val = number | string | boolean | undefined;

// Numeric coercion: numbers pass through; booleans → 1/0; numeric-looking strings
// parse; everything else (incl. undefined, non-numeric strings) → NaN.
function toNum(v: Val): number {
	if (typeof v === "number") return v;
	if (typeof v === "boolean") return v ? 1 : 0;
	if (typeof v === "string") {
		const s = v.trim();
		if (s === "") return Number.NaN;
		return Number(s);
	}
	return Number.NaN;
}

// Truthiness: numeric-aware so "0" is false and "12" is true; non-numeric strings
// are truthy when non-empty; undefined is always false.
function toBool(v: Val): boolean {
	if (typeof v === "boolean") return v;
	if (typeof v === "number") return v !== 0 && !Number.isNaN(v);
	if (typeof v === "string") {
		const num = toNum(v);
		if (!Number.isNaN(num)) return num !== 0;
		return v.length > 0;
	}
	return false;
}

// Equality: numeric when both sides coerce to a number, string otherwise. An
// undefined operand is never equal to anything (so == is false, != is true).
function eq(l: Val, r: Val): boolean {
	if (l === undefined || r === undefined) return false;
	const ln = toNum(l);
	const rn = toNum(r);
	if (!Number.isNaN(ln) && !Number.isNaN(rn)) return ln === rn;
	return String(l) === String(r);
}

function evalAst(ast: Ast, env: EvalEnv): Val {
	switch (ast.type) {
		case "num":
			return ast.value;
		case "str":
			return ast.value;
		case "ident":
			// Own-property only: an identifier like __proto__/constructor/toString must
			// resolve to undefined (→ false), never an inherited Object.prototype member.
			return Object.prototype.hasOwnProperty.call(env, ast.name) ? env[ast.name] : undefined;
		case "unary":
			return ast.op === "!" ? !toBool(evalAst(ast.operand, env)) : -toNum(evalAst(ast.operand, env));
		case "binary": {
			// Short-circuit logical ops.
			if (ast.op === "&&") return toBool(evalAst(ast.left, env)) && toBool(evalAst(ast.right, env));
			if (ast.op === "||") return toBool(evalAst(ast.left, env)) || toBool(evalAst(ast.right, env));
			const l = evalAst(ast.left, env);
			const r = evalAst(ast.right, env);
			switch (ast.op) {
				case "==":
					return eq(l, r);
				case "!=":
					return !eq(l, r);
				case "<":
				case "<=":
				case ">":
				case ">=": {
					const ln = toNum(l);
					const rn = toNum(r);
					if (Number.isNaN(ln) || Number.isNaN(rn)) return false;
					return ast.op === "<" ? ln < rn : ast.op === "<=" ? ln <= rn : ast.op === ">" ? ln > rn : ln >= rn;
				}
				case "+":
				case "-":
				case "*":
				case "/": {
					const ln = toNum(l);
					const rn = toNum(r);
					return ast.op === "+" ? ln + rn : ast.op === "-" ? ln - rn : ast.op === "*" ? ln * rn : rn === 0 ? Number.NaN : ln / rn;
				}
			}
		}
	}
}

/** Evaluate a parsed notifyWhen AST against an env, coercing the result to boolean. */
export function evalExpr(ast: Ast, env: EvalEnv): boolean {
	return toBool(evalAst(ast, env));
}

/** Parse + evaluate convenience (tests / one-shot use). */
export function evalExprSource(src: string, env: EvalEnv): boolean {
	return evalExpr(parseExpr(src), env);
}
