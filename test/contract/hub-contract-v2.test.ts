import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { loadContract, refName } from "../support/contract.ts";
import {
	HOST_TOKEN,
	type HubProcess,
	LEGACY,
	LinkClient,
	type LinkEventRecord,
	startHubProcess,
	tempDir,
	writeTokens,
} from "../support/hub-process.ts";

// A protocol 2 client against this hub: the register body, the agent header, and the bus extension URI of
// protocol 2, checked against the committed protocol 2 schema. Two changes of the hub are open here: an agent
// kind may be any string, because the hub names the kind of a session agent "session", and a 426 body may
// carry the field `supported`.

const contract = loadContract(LEGACY.protocol, (defs) => {
	defs["AgentKind"] = { title: "AgentKind", type: "string" };
	defs["UpgradeErrorBody"] = { ...(defs["UpgradeErrorBody"] as object), additionalProperties: true };
});

describe("hub link contract for protocol 2 clients", () => {
	const tmp = tempDir("hub-contract-v2-");
	let hub: HubProcess;
	const links: LinkClient[] = [];
	let rpcId = 0;
	const alpha = "legacy-alpha";
	const beta = "legacy-beta";
	const kid = "legacy-kid";
	let alphaLink: LinkClient;
	let betaLink: LinkClient;

	beforeAll(async () => {
		hub = await startHubProcess({ dataDir: `${tmp.dir}/data`, tokensFile: writeTokens(tmp.dir) });
	});

	afterAll(async () => {
		try {
			for (const link of links) checkEvents(link);
		} finally {
			for (const link of links) await link.close();
			await hub?.stop();
			tmp.dispose();
		}
	});

	async function call(method: string, path: string, body?: unknown): Promise<{ status: number; body: unknown }> {
		const key = contract.routeOf(method, path);
		const route = contract.routes[key];
		if (!route) throw new Error(`no route ${key}`);
		const response = await fetch(`${hub.url}${path}`, {
			method,
			headers: { Authorization: `Bearer ${HOST_TOKEN}`, "Content-Type": "application/json" },
			body: body === undefined ? null : JSON.stringify(body),
		});
		const text = await response.text();
		const parsed = text ? (JSON.parse(text) as unknown) : null;
		const where = `${key} ${response.status}`;
		if (response.status === 200) {
			if (!route.response) throw new Error(`${where}: the route has no JSON response`);
			contract.check(refName(route.response), parsed, where);
			if (route.request && body !== undefined) contract.check(refName(route.request), body, `${key} request`);
		} else {
			const ref = route.errors[String(response.status)];
			if (!ref) throw new Error(`${where}: status not listed for the route: ${text}`);
			contract.check(refName(ref), parsed, where);
		}
		return { status: response.status, body: parsed };
	}

	async function ok(method: string, path: string, body?: unknown): Promise<Record<string, unknown>> {
		const result = await call(method, path, body);
		expect(result.status, JSON.stringify(result.body)).toBe(200);
		return result.body as Record<string, unknown>;
	}

	function checkEvents(link: LinkClient): void {
		for (const event of link.events) {
			const spec = contract.events[event.type];
			if (!spec) throw new Error(`event ${event.type} for ${link.agent}: type not listed`);
			contract.check(refName(spec.payload), event.data, `event ${event.type} for ${link.agent}`);
		}
	}

	async function stream(agent: string): Promise<LinkClient> {
		const link = new LinkClient(hub.url, agent);
		links.push(link);
		await link.connect();
		return link;
	}

	async function next(link: LinkClient, type: string, match: (data: Record<string, unknown>) => boolean) {
		const event: LinkEventRecord = await link.next(type, match);
		checkEvents(link);
		return event;
	}

	/** A JSON-RPC call as a protocol 2 client sends it. */
	async function rpc(from: string, to: string, method: string, params: unknown) {
		const response = await fetch(`${hub.url}/a2a/${to}`, {
			method: "POST",
			headers: {
				Authorization: `Bearer ${HOST_TOKEN}`,
				[LEGACY.agentHeader]: from,
				"Content-Type": "application/json",
				"A2A-Version": "1.0",
				"A2A-Extensions": LEGACY.busUri,
			},
			body: JSON.stringify({ jsonrpc: "2.0", id: ++rpcId, method, params }),
		});
		const json = (await response.json()) as { result?: Record<string, unknown>; error?: unknown };
		if (response.status !== 200 || !json.result) throw new Error(`${method}: ${JSON.stringify(json)}`);
		return { result: json.result, extensions: response.headers.get("a2a-extensions") };
	}

	function sendParams(text: string, bus: Record<string, unknown>) {
		return {
			message: {
				messageId: crypto.randomUUID(),
				contextId: crypto.randomUUID(),
				role: "ROLE_USER",
				parts: [{ text }],
				metadata: { [LEGACY.busUri]: bus },
			},
			configuration: { returnImmediately: true },
		};
	}

	it("reads the protocol 2 schema", () => {
		expect(contract.document.$id).toBe(`urn:a2a-hub:protocol:${LEGACY.protocol}`);
	});

	it("registers with protocol 2 and gets the harness label of protocol 2", async () => {
		await ok("POST", "/link/register", {
			agent: alpha,
			name: "alpha",
			labels: { team: "core" },
			protocol: LEGACY.protocol,
		});
		await ok("POST", "/link/register", { agent: beta, name: "beta", protocol: LEGACY.protocol });
		alphaLink = await stream(alpha);
		betaLink = await stream(beta);
		expect(await ok("GET", `/agents/${alpha}`)).toMatchObject({
			kind: "session",
			labels: { team: "core", harness: LEGACY.harness },
		});
		await ok("POST", `/link/${alpha}/children`, { id: kid, name: "kid", labels: {} });
		await ok("POST", "/link/register", { agent: kid, parent: alpha, protocol: LEGACY.protocol });
		expect(await ok("GET", `/agents/${kid}`)).toMatchObject({ parent: alpha, labels: { harness: LEGACY.harness } });
		const listed = (await ok("GET", "/agents"))["agents"] as { id: string }[];
		expect(listed.map((agent) => agent.id)).toEqual(expect.arrayContaining([alpha, beta]));
	});

	it("sends a task with the protocol 2 header and URI and answers under that URI", async () => {
		const sent = await rpc(alpha, beta, "SendMessage", sendParams("review", { kind: "task", expectReply: true }));
		expect(sent.extensions).toBe(LEGACY.busUri);
		const task = sent.result["task"] as { id: string };
		const event = await next(betaLink, "message", (data) => data["taskId"] === task.id);
		expect(event.data).toMatchObject({ from: { agent: alpha, name: "alpha", kind: "session" }, expectReply: true });
		expect((await ok("GET", `/link/${beta}/owed`))["tasks"]).toEqual([expect.objectContaining({ taskId: task.id })]);

		await ok("POST", `/link/${beta}/tasks/${task.id}`, { state: "working", text: "on it" });
		await next(alphaLink, "update", (data) => data["taskId"] === task.id);
		await ok("POST", `/link/${beta}/status`, { text: "reading" });
		const read = await rpc(alpha, beta, "GetTask", { id: task.id });
		const status = read.result["status"] as { message?: { metadata?: Record<string, unknown> } };
		expect(status.message?.metadata).toEqual({ [LEGACY.busUri]: { statusText: true } });

		await ok("POST", `/link/${beta}/tasks/${task.id}`, { state: "completed", text: "done" });
		const done = await next(alphaLink, "update", (data) => data["state"] === "completed");
		const inbox = (await ok("GET", `/link/${alpha}/inbox`))["messages"] as { messageId: string; from: unknown }[];
		expect(inbox).toContainEqual(
			expect.objectContaining({ messageId: done.data["messageId"], from: expect.objectContaining({ kind: "session" }) }),
		);
	});

	it("stamps the delivery state under the protocol 2 URI", async () => {
		const sent = await rpc(alpha, kid, "SendMessage", sendParams("for later", { kind: "note", deliveryWait: true }));
		const item = (sent.result["task"] ?? sent.result["message"]) as { metadata?: Record<string, unknown> };
		expect(item.metadata).toEqual({ [LEGACY.busUri]: { delivery: "queued" } });
		contract.check("BusMetadata", item.metadata?.[LEGACY.busUri], "delivery stamp");
	});

	it("acks and goes offline", async () => {
		const last = betaLink.events.filter((event) => event.id > 0).at(-1);
		if (!last) throw new Error("beta got no event");
		expect((await ok("POST", `/link/${beta}/ack`, { upTo: last.id }))["acked"]).toBeGreaterThanOrEqual(1);
		await ok("DELETE", `/link/${beta}`);
	});

	it("tells a protocol 1 client which protocols it accepts", async () => {
		const old = await call("POST", "/link/register", { agent: alpha, protocol: 1 });
		expect(old).toEqual({ status: 426, body: { error: "hub protocol 3 required", expected: 3, supported: [2, 3] } });
	});
});
