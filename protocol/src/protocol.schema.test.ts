import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { renderHubSchema, SCHEMA_FILE } from "../scripts/hub-schema.ts";
import * as S from "./protocol.schema.ts";

/**
 * sha256 of the protocol 2 file that the TypeScript hub published (pimon 1cb229e). The wire of protocol 2
 * must not change, so the file must stay byte-identical. A wire change is a new protocol number and file.
 */
const PUBLISHED_V2_SHA256 = "4c5e21416e15b4c00d55c0856cd60ca7ced8452fa8827758f30e73fb1bcfa4a0";

describe("hub protocol schema", () => {
	it("titles each schema with its export name", () => {
		for (const [name, schema] of Object.entries(S)) expect((schema as { title?: string }).title, name).toBe(name);
	});

	it("matches the committed JSON Schema file", () => {
		expect(readFileSync(SCHEMA_FILE, "utf8"), "run: npm run schema").toBe(renderHubSchema());
	});

	it("keeps the protocol 2 file byte-identical to the published one", () => {
		const file = join(import.meta.dirname, "../schema/hub-protocol-v2.json");
		expect(createHash("sha256").update(readFileSync(file)).digest("hex")).toBe(PUBLISHED_V2_SHA256);
	});

	it("resolves each $ref to a definition", () => {
		const document = JSON.parse(renderHubSchema()) as { $defs: Record<string, unknown> };
		const refs = [...JSON.stringify(document).matchAll(/"\$ref":"#\/\$defs\/([^"]+)"/g)].map((match) => match[1]);
		expect(refs.length).toBeGreaterThan(0);
		for (const name of refs) expect(Object.keys(document.$defs)).toContain(name);
	});
});
