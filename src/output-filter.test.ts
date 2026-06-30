import { strict as assert } from "node:assert";
import { test } from "node:test";
import { compactNoisyOutput } from "./output-filter";

function aptOutput(n: number): string {
	const lines: string[] = ["Reading package lists...", "Building dependency tree...", "Reading state information..."];
	lines.push("The following NEW packages will be installed:");
	for (let i = 0; i < n; i++) lines.push(`Unpacking libfoo${i}:amd64 (1.0-${i}) ...`);
	for (let i = 0; i < n; i++) lines.push(`Setting up libfoo${i}:amd64 (1.0-${i}) ...`);
	lines.push("Processing triggers for man-db (2.10.2-1) ...");
	return lines.join("\n");
}

test("apt: collapses unpacking/setting-up noise, keeps summary", () => {
	const raw = `${aptOutput(80)}\n0 upgraded, 80 newly installed, 0 to remove and 0 not upgraded.`;
	const { text, stats } = compactNoisyOutput("sudo apt-get install -y a b c", raw);
	assert.equal(stats.compacted, true);
	assert.ok(stats.kept < stats.original);
	assert.match(text, /80 newly installed/);
	assert.match(text, /routine output collapsed/);
	// The bulk of Unpacking lines should be gone.
	assert.ok((text.match(/Unpacking libfoo/g) || []).length < 10);
});

test("apt: keeps error lines", () => {
	const raw = `${aptOutput(70)}\nE: Unable to locate package nope\n0 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.`;
	const { text } = compactNoisyOutput("apt-get install nope", raw);
	assert.match(text, /E: Unable to locate package nope/);
});

test("pip: collapses collecting/downloading, keeps Successfully installed", () => {
	const lines: string[] = [];
	for (let i = 0; i < 80; i++) {
		lines.push(`Collecting pkg${i}`);
		lines.push(`  Downloading pkg${i}-1.0-py3-none-any.whl (123 kB)`);
	}
	lines.push("Installing collected packages: a, b, c");
	lines.push("Successfully installed a-1.0 b-2.0 c-3.0");
	const { text, stats } = compactNoisyOutput("pip install -r req.txt", lines.join("\n"));
	assert.equal(stats.compacted, true);
	assert.match(text, /Successfully installed a-1.0/);
	assert.ok((text.match(/Downloading pkg/g) || []).length < 20);
});

test("non-noisy command is returned unchanged", () => {
	const raw = Array.from({ length: 100 }, (_, i) => `data row ${i}`).join("\n");
	const { text, stats } = compactNoisyOutput("cat big.csv", raw);
	assert.equal(stats.compacted, false);
	assert.equal(text, raw);
});

test("short apt output is left alone", () => {
	const raw = "Reading package lists...\nBuilding dependency tree...\n0 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.";
	const { stats } = compactNoisyOutput("apt-get install x", raw);
	assert.equal(stats.compacted, false);
});

test("tail is always preserved", () => {
	const raw = `${aptOutput(80)}\nFINAL-LINE-MARKER`;
	const { text } = compactNoisyOutput("apt-get install z", raw);
	assert.match(text, /FINAL-LINE-MARKER/);
});
