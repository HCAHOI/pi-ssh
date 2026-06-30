// ---------------------------------------------------------------------------
// Noise compaction for package-manager / build output (apt, pip, npm, ...)
// ---------------------------------------------------------------------------
// Tools like `apt-get install` or `pip install` emit hundreds of per-package
// progress/unpacking/setting-up lines that carry almost no signal but burn a lot
// of context. compactNoisyOutput() detects such a command and drops the bulk of
// those lines while ALWAYS keeping errors, warnings, summaries, and a tail, so
// the agent still sees what happened and what failed. Non-matching commands are
// returned unchanged.

interface NoiseProfile {
	/** Command matches this -> treat its output as noisy. */
	cmd: RegExp;
	/** A line matching any of these is dropped (unless it is a keep line). */
	drop: RegExp[];
}

// Lines worth keeping regardless of the per-tool drop rules.
const KEEP = [
	/\b(error|err!|fatal|failed|failure|cannot|not found|no such|denied|traceback|exception|conflict|unmet|broken|warning|warn)\b/i,
	/\bE:\s/, // apt error prefix
	/\b(\d+)\s+(upgraded|newly installed|to remove|not upgraded)\b/i, // apt summary
	/Successfully installed /i, // pip summary
	/Installing collected packages/i,
	/^\s*(added|removed|changed|audited)\s+\d+\s+packages?/i, // npm summary
	/Setting up swapspace|Need to get|After this operation/i,
];

const PROFILES: NoiseProfile[] = [
	{
		cmd: /\bapt(-get)?\b/,
		drop: [
			/^(Get|Hit|Ign|Fetched|Reading|Building|Selecting|Preparing|Unpacking|Setting up|Processing triggers|Adding|Created symlink|update-alternatives:|Scanning|\(Reading database)/,
			/^\s*$/,
			/^\d+%/,
			/^(Running kernel|No services|No containers|No VM guests|No user sessions)/,
		],
	},
	{
		cmd: /\bpip[0-9.]*\b|python[0-9.]*\s+-m\s+pip|\buv\s+pip\b/,
		drop: [
			/^\s*(Collecting|Downloading|Using cached|Requirement already satisfied|Preparing metadata|Building wheel|Created wheel|Stored in directory|Getting requirements|Installing build dependencies|Obtaining|Saved )/,
			/^\s*\|[\s#█▏▎▍▌▋▊▉ ]*\|/, // progress bars
			/\d+\.\d+\s*[kKmM]B/,
			/^\s*$/,
		],
	},
	{
		cmd: /\bnpm\b|\bpnpm\b|\byarn\b/,
		drop: [
			/^npm (warn|http|notice|sill|verb|info)\b/i,
			/^\s*(reify|idealTree|timing|⠋|⠙|⠹|⠸|⠼|⠴|⠦|⠧|⠇|⠏)/,
			/^\s*$/,
		],
	},
	{
		cmd: /\b(docker|podman)\s+(build|pull|compose\s+(up|build|pull))\b/,
		drop: [
			/^\s*(Pulling fs layer|Waiting|Verifying Checksum|Download complete|Downloading|Extracting|Pull complete|Already exists)/,
			/^#\d+\s+(sha256:|extracting|transferring|naming|exporting|DONE|CACHED|\d+\.\d+s)/,
			/^\s*$/,
		],
	},
];

const MAX_TAIL = 25;

export interface CompactStats {
	compacted: boolean;
	original: number;
	kept: number;
}

/** Compact noisy package-manager output. Returns the text unchanged when the
 * command is not a known noisy tool or when there is little to gain. */
export function compactNoisyOutput(command: string, text: string): { text: string; stats: CompactStats } {
	const profile = PROFILES.find((p) => p.cmd.test(command));
	const lines = text.split("\n");
	const noop = { text, stats: { compacted: false, original: lines.length, kept: lines.length } };
	if (!profile) return noop;
	// Small outputs are not worth touching.
	if (lines.length < 60) return noop;

	const tailStart = lines.length - MAX_TAIL;
	const kept: string[] = [];
	let dropped = 0;
	let pendingGap = false;

	lines.forEach((line, idx) => {
		const isTail = idx >= tailStart;
		const keep = isTail || KEEP.some((re) => re.test(line)) || !profile.drop.some((re) => re.test(line));
		if (keep) {
			if (pendingGap) {
				kept.push(`  … (${dropped} line${dropped === 1 ? "" : "s"} of routine output collapsed)`);
				pendingGap = false;
			}
			kept.push(line);
		} else {
			dropped++;
			pendingGap = true;
		}
	});

	if (dropped < 20) return noop; // not enough noise removed to bother
	if (pendingGap) kept.push(`  … (${dropped} line${dropped === 1 ? "" : "s"} of routine output collapsed)`);

	return { text: kept.join("\n"), stats: { compacted: true, original: lines.length, kept: kept.length } };
}
