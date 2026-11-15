// Loads a committed JSON Schema file of the link protocol and checks values against it. No import from the
// protocol sources: the contract suites check the hub binary only against the committed file.
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import Schema from "typebox/schema";

export interface Ref {
	$ref: string;
}

export interface RouteEntry {
	method: string;
	path: string;
	request: Ref | null;
	response: Ref | null;
	stream: boolean;
	errors: Record<string, Ref>;
}

export interface ProtocolDocument {
	$id: string;
	$defs: Record<string, unknown>;
	"x-routes": Record<string, RouteEntry>;
	"x-events": Record<string, { payload: Ref; stored: boolean }>;
}

/**
 * Sets `additionalProperties: false` on each object schema that does not set it and has no `patternProperties`.
 * So an undeclared field fails, while string maps such as `labels` and objects marked open stay open.
 */
function closeObjects(node: unknown): unknown {
	if (Array.isArray(node)) return node.map(closeObjects);
	if (!node || typeof node !== "object") return node;
	const out: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(node)) out[key] = closeObjects(value);
	if (out["type"] === "object" && !("additionalProperties" in out) && !("patternProperties" in out)) {
		out["additionalProperties"] = false;
	}
	return out;
}

export function refName(ref: Ref): string {
	const prefix = "#/$defs/";
	if (!ref.$ref.startsWith(prefix)) throw new Error(`unexpected $ref ${ref.$ref}`);
	return ref.$ref.slice(prefix.length);
}

export interface Contract {
	file: string;
	document: ProtocolDocument;
	routes: Record<string, RouteEntry>;
	events: Record<string, { payload: Ref; stored: boolean }>;
	/** Throws when the value does not match the definition. */
	check(name: string, value: unknown, where: string): void;
	/** The `METHOD /path` key of the route that a request path matches. */
	routeOf(method: string, path: string): string;
}

/** `patch` may change the closed definitions before the first check. */
export function loadContract(protocol: number, patch: (defs: Record<string, unknown>) => void = () => {}): Contract {
	const file = resolve(import.meta.dirname, "../../protocol/schema", `hub-protocol-v${protocol}.json`);
	const document = JSON.parse(readFileSync(file, "utf8")) as ProtocolDocument;
	const routes = document["x-routes"];
	const closedDefs = closeObjects(document.$defs) as Record<string, unknown>;
	patch(closedDefs);
	const validators = new Map<string, ReturnType<typeof Schema.Compile>>();

	function check(name: string, value: unknown, where: string): void {
		let validator = validators.get(name);
		if (!validator) {
			if (!(name in closedDefs)) throw new Error(`${where}: no definition ${name}`);
			validator = Schema.Compile({ $defs: closedDefs, $ref: `#/$defs/${name}` } as Schema.XSchema);
			validators.set(name, validator);
		}
		const [valid, errors] = validator.Errors(value);
		if (!valid) {
			throw new Error(
				`${where}: value does not match ${name}\nerrors: ${JSON.stringify(errors, null, 2)}\nvalue: ${JSON.stringify(value)}`,
			);
		}
	}

	function routeOf(method: string, path: string): string {
		const segments = path.split("?")[0]?.split("/").filter(Boolean).map(decodeURIComponent) ?? [];
		for (const [key, route] of Object.entries(routes)) {
			const parts = route.path.split("/").filter(Boolean);
			if (route.method !== method || parts.length !== segments.length) continue;
			if (parts.every((part, index) => part.startsWith(":") || part === segments[index])) return key;
		}
		throw new Error(`${method} ${path} matches no route in ${file}`);
	}

	return { file, document, routes, events: document["x-events"], check, routeOf };
}
