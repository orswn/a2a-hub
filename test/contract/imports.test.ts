import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const root = join(import.meta.dirname, "..");

describe("contract suite imports", () => {
	it("takes no value from the protocol sources, so only the JSON Schema file defines the wire", () => {
		for (const file of ["support/hub-process.ts", "contract/hub-contract.test.ts"]) {
			const text = readFileSync(join(root, file), "utf8");
			const imports = [...text.matchAll(/^import\s+(type\s+)?[^;]*?from\s+"([^"]+)"/gm)];
			expect(imports.length, file).toBeGreaterThan(0);
			const values = imports
				.filter((match) => !match[1] && /\/protocol\/src\//.test(match[2] ?? ""))
				.map((match) => match[2]);
			expect(values, file).toEqual([]);
		}
	});
});
