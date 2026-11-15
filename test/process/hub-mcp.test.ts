import { createHmac, timingSafeEqual } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { join, resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	HOST_TOKEN,
	type HubProcess,
	LinkClient,
	PEER_TOKEN,
	sleep,
	startHubProcess,
	tempDir,
	writeTokens,
} from "../support/hub.ts";

/** A request that Hermes v0.23.1 accepted on its generic webhook route. */
const VECTOR = JSON.parse(
	readFileSync(resolve(import.meta.dirname, "../../internal/mcp/testdata/hermes-webhook-vector.json"), "utf8"),
) as { secret: string; timestamp: string; body: string; headers: Record<string, string>; replay_window_seconds: number };

const SECRET = "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0";

/** The Hermes check of a generic V2 signature: HMAC-SHA256 of "<timestamp>.<body>" within the replay window. */
function hermesAccepts(secret: string, headers: Record<string, string | undefined>, body: string, nowSeconds: number): boolean {
	const timestamp = headers["x-webhook-timestamp"] ?? "";
	const signature = headers["x-webhook-signature-v2"] ?? "";
	if (!/^\d+$/.test(timestamp) || Math.abs(nowSeconds - Number(timestamp)) > VECTOR.replay_window_seconds) return false;
	const expected = createHmac("sha256", secret).update(`${timestamp}.`).update(body).digest("hex");
	return signature.length === expected.length && timingSafeEqual(Buffer.from(signature), Buffer.from(expected));
}

interface Post {
	path: string;
	headers: IncomingHttpHeaders;
	raw: string;
	body: Record<string, unknown>;
	accepted: boolean;
}

async function startReceiver(): Promise<{ url: string; posts: Post[]; close(): Promise<void> }> {
	const posts: Post[] = [];
	const server: Server = createServer((req, res) => {
		// The hub also fetches the agent card of the external agent here.
		if (req.method !== "POST") {
			res.writeHead(404).end();
			return;
		}
		const chunks: Buffer[] = [];
		req.on("data", (chunk: Buffer) => chunks.push(chunk));
		req.on("end", () => {
			const raw = Buffer.concat(chunks).toString("utf8");
			const headers = Object.fromEntries(Object.entries(req.headers).map(([k, v]) => [k, String(v)]));
			const accepted = hermesAccepts(SECRET, headers, raw, Math.floor(Date.now() / 1000));
			posts.push({ path: req.url ?? "", headers: req.headers, raw, body: JSON.parse(raw) as Record<string, unknown>, accepted });
			res.writeHead(accepted ? 200 : 401).end();
		});
	});
	await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
	const { port } = server.address() as AddressInfo;
	return {
		url: `http://127.0.0.1:${port}`,
		posts,
		close: () =>
			new Promise<void>((done) => {
				server.closeAllConnections();
				server.close(() => done());
			}),
	};
}

async function until<T>(what: string, probe: () => T | undefined, timeoutMs = 5000): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const value = probe();
		if (value !== undefined) return value;
		if (Date.now() > deadline) throw new Error(`timed out: ${what}`);
		await sleep(20);
	}
}

type Json = Record<string, unknown>;

/** A streamable HTTP MCP client that sends what the harness clients send. */
class McpClient {
	session = "";
	frames: Json[] = [];
	#next = 0;
	#controller: AbortController | undefined;
	readonly #url: string;
	readonly #headers: Record<string, string>;

	constructor(hub: string, token: string, headers: Record<string, string>) {
		this.#url = `${hub}/mcp`;
		this.#headers = {
			Authorization: `Bearer ${token}`,
			"Content-Type": "application/json",
			Accept: "application/json, text/event-stream",
			...headers,
		};
	}

	async post(message: Json, extra: Record<string, string> = {}): Promise<{ status: number; headers: Headers; body: Json | null }> {
		const headers: Record<string, string> = { ...this.#headers, ...extra };
		if (this.session) headers["Mcp-Session-Id"] = this.session;
		const response = await fetch(this.#url, { method: "POST", headers, body: JSON.stringify(message) });
		const text = await response.text();
		return { status: response.status, headers: response.headers, body: text ? (JSON.parse(text) as Json) : null };
	}

	rpc(method: string, params?: Json, extra?: Record<string, string>) {
		this.#next++;
		return this.post({ jsonrpc: "2.0", id: this.#next, method, ...(params ? { params } : {}) }, extra);
	}

	async initialize(): Promise<Json> {
		const got = await this.rpc("initialize", {
			protocolVersion: "2025-11-25",
			capabilities: {},
			clientInfo: { name: "test", version: "1" },
		});
		if (got.status !== 200) throw new Error(`initialize: ${got.status} ${JSON.stringify(got.body)}`);
		this.session = got.headers.get("mcp-session-id") ?? "";
		await this.post({ jsonrpc: "2.0", method: "notifications/initialized" });
		return got.body?.["result"] as Json;
	}

	async call(name: string, args: Json, meta?: Json): Promise<{ text: string; isError: boolean }> {
		const got = await this.rpc("tools/call", { name, arguments: args, ...(meta ? { _meta: meta } : {}) });
		const result = got.body?.["result"] as { content: { text: string }[]; isError: boolean } | undefined;
		if (!result) throw new Error(`tools/call ${name}: ${got.status} ${JSON.stringify(got.body)}`);
		return { text: result.content[0]?.text ?? "", isError: result.isError };
	}

	async open(): Promise<number> {
		this.#controller = new AbortController();
		const response = await fetch(this.#url, {
			headers: { ...this.#headers, Accept: "text/event-stream", "Mcp-Session-Id": this.session },
			signal: this.#controller.signal,
		});
		if (response.status !== 200 || !response.body) return response.status;
		const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
		void (async () => {
			let buffer = "";
			try {
				for (;;) {
					const { value, done } = await reader.read();
					if (done) return;
					buffer += value;
					let end = buffer.indexOf("\n\n");
					while (end >= 0) {
						const data = buffer
							.slice(0, end)
							.split("\n")
							.filter((line) => line.startsWith("data: "))
							.map((line) => line.slice(6))
							.join("");
						if (data) this.frames.push(JSON.parse(data) as Json);
						buffer = buffer.slice(end + 2);
						end = buffer.indexOf("\n\n");
					}
				}
			} catch {
				// aborted by close()
			}
		})();
		return 200;
	}

	close(): void {
		this.#controller?.abort();
	}
}

const sendHead = /^Sent to \S+ on thread (\S+) \(messageId (\S+)\)\./;

describe("hub MCP endpoint", () => {
	const tmp = tempDir("hub-mcp-");
	let hub: HubProcess;
	let receiver: Awaited<ReturnType<typeof startReceiver>>;
	let bee: LinkClient;
	const clients: McpClient[] = [];
	const client = (token: string, headers: Record<string, string>) => {
		const made = new McpClient(hub.url, token, headers);
		clients.push(made);
		return made;
	};

	beforeAll(async () => {
		receiver = await startReceiver();
		const externalFile = join(tmp.dir, "external.json");
		writeFileSync(
			externalFile,
			JSON.stringify({
				hermes: {
					name: "hermes",
					url: receiver.url,
					token: "unused",
					webhook: { url: `${receiver.url}/webhooks/a2a-hub`, secret: SECRET },
				},
			}),
			{ mode: 0o600 },
		);
		hub = await startHubProcess({ dataDir: join(tmp.dir, "data"), tokensFile: writeTokens(tmp.dir), externalFile });
		bee = new LinkClient(hub.url, "pi-bee");
		await bee.register({ name: "bee", labels: { harness: "pi" } });
		await bee.connect();
	});

	afterAll(async () => {
		for (const made of clients) made.close();
		await bee.close();
		await hub.stop();
		await receiver.close();
		tmp.dispose();
	});

	it("the receiver check accepts the recorded Hermes vector", () => {
		const headers = Object.fromEntries(Object.entries(VECTOR.headers).map(([k, v]) => [k.toLowerCase(), v]));
		expect(hermesAccepts(VECTOR.secret, headers, VECTOR.body, Number(VECTOR.timestamp))).toBe(true);
		expect(hermesAccepts(VECTOR.secret, headers, `${VECTOR.body} `, Number(VECTOR.timestamp))).toBe(false);
		expect(hermesAccepts(VECTOR.secret, headers, VECTOR.body, Number(VECTOR.timestamp) + 301)).toBe(false);
	});

	it("answers the Claude probe like Claude expects, then pushes a wake as a channel notification", async () => {
		const claude = client(HOST_TOKEN, {
			"X-A2A-Harness": "claude-code",
			"X-A2A-Agent": "claude-mars-777-1",
			"X-A2A-Session": "9c0ffee0-1111-4222-8333-444455556666",
		});
		const probe = await claude.rpc(
			"server/discover",
			{},
			{ "Mcp-Method": "server/discover", "Mcp-Protocol-Version": "2026-07-28" },
		);
		expect(probe.status).toBe(200);
		expect(probe.body).toMatchObject({ error: { code: -32601 } });
		const init = await claude.initialize();
		expect(init).toMatchObject({
			protocolVersion: "2025-11-25",
			capabilities: { tools: {}, experimental: { "claude/channel": {} } },
		});
		expect(String(init["instructions"]).length).toBeGreaterThanOrEqual(400);
		expect(await claude.open()).toBe(200);

		const codex = client(HOST_TOKEN, { "X-A2A-Harness": "codex" });
		await codex.initialize();
		const meta = { threadId: "019a0000-0000-7000-8000-00000000c0de" };
		for (const deadline = Date.now() + 5000; Date.now() < deadline; await sleep(20)) {
			if ((await codex.call("peers", { filter: "claude-mars-777-1" }, meta)).text.startsWith("claude-9c0ffe ")) break;
		}
		const sent = await codex.call("send", { target: "id:claude-mars-777-1", text: "look at the build", kind: "task" }, meta);
		expect(sent.text).toContain("unconfirmed: claude-9c0ffe (id:claude-mars-777-1, pushed to its session, which does not confirm)");
		const notification = await until("channel notification", () => claude.frames.find((f) => f["method"]));
		expect(notification).toMatchObject({
			method: "notifications/claude/channel",
			params: { content: "codex-00c0de: look at the build", meta: { sender_kind: "session", kind: "task" } },
		});
		const stored = await codex.call("send", { target: "id:claude-mars-777-1", text: "later", wake: "never" }, meta);
		expect(stored.text).toContain("queued: claude-9c0ffe (id:claude-mars-777-1, stored in the hub inbox of claude-9c0ffe)");
	});

	it("refuses a request without a known token or with an unknown harness", async () => {
		const anonymous = await fetch(`${hub.url}/mcp`, {
			method: "POST",
			headers: { "Content-Type": "application/json", "X-A2A-Harness": "codex" },
			body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} }),
		});
		expect(anonymous.status).toBe(401);
		const unknown = await client(HOST_TOKEN, { "X-A2A-Harness": "pi" }).rpc("initialize", {});
		expect(unknown.status).toBe(400);
		expect(unknown.headers.get("mcp-session-id")).toBeNull();
	});

	it("binds Codex tool calls to the thread in _meta", async () => {
		const codex = client(HOST_TOKEN, { "X-A2A-Harness": "codex" });
		const init = await codex.initialize();
		expect(init["capabilities"]).toEqual({ tools: {} });
		expect(await codex.open()).toBe(405);
		const missing = await codex.call("peers", {});
		expect(missing.isError).toBe(true);
		const meta = { threadId: "019a0000-0000-7000-8000-0000000000aa", "x-codex-turn-metadata": { session_id: "s" } };
		expect((await codex.call("set_status", { text: "reviewing" }, meta)).text).toBe("Status set: reviewing");
		const peers = await codex.call("peers", { filter: "bee" }, meta);
		expect(peers.text).toMatch(/^bee id:pi-bee kind=session harness=pi /);
		const event = bee.next("message", (data) => data["text"] === "hello bee");
		await codex.call("send", { target: "bee", text: "hello bee" }, meta);
		const message = await event;
		expect(message.data).toMatchObject({ from: { agent: meta.threadId, name: "codex-0000aa", kind: "session" } });
	});

	it("routes a reply to a Hermes send back through the signed webhook with the send origin", async () => {
		const hermes = client(PEER_TOKEN, { "X-A2A-Harness": "hermes" });
		const init = await hermes.initialize();
		expect(init["capabilities"]).toEqual({ tools: {} });
		const event = bee.next("message", (data) => data["text"] === "run the tests");
		const sent = await hermes.call("send", {
			target: "bee",
			text: "run the tests",
			kind: "task",
			origin: { platform: "telegram", chat_id: "-100123", thread_id: null },
		});
		expect(sent.isError).toBe(false);
		const [, threadId, messageId] = sendHead.exec(sent.text) ?? [];
		const taskId = String((await event).data["taskId"]);
		expect((await bee.update(taskId, { state: "completed", text: "all green" })).status).toBe(200);
		const post = await until("webhook post", () => receiver.posts.find((p) => p.body["kind"] === "result"));
		expect(post.accepted).toBe(true);
		expect(post.path).toBe("/webhooks/a2a-hub");
		expect(post.headers["x-request-id"]).toBe(post.body["message_id"]);
		expect(post.body).toEqual({
			event: "a2a-hub.message",
			origin: { platform: "telegram", chat_id: "-100123", thread_id: "" },
			from: { id: "pi-bee", name: "bee", harness: "pi" },
			kind: "result",
			message_id: post.body["message_id"],
			in_reply_to: messageId,
			hub_thread_id: threadId,
			task_id: taskId,
			state: "completed",
			text: "all green",
		});
		const inbox = await hermes.call("inbox", {});
		expect(inbox.text).toContain("all green");
	});

	it("gives a waiting Hermes send the reply and posts nothing for it", async () => {
		const hermes = client(PEER_TOKEN, { "X-A2A-Harness": "hermes" });
		await hermes.initialize();
		const before = receiver.posts.length;
		const answer = (async () => {
			const event = await bee.next("message", (data) => data["text"] === "quick question?");
			await bee.update(String(event.data["taskId"]), { state: "completed", text: "quick answer" });
		})();
		const sent = await hermes.call("send", { target: "bee", text: "quick question?", kind: "question", wait: 10 });
		await answer;
		expect(sent.text).toContain("First reply:");
		expect(sent.text).toContain("quick answer");
		const inbox = await hermes.call("inbox", {});
		expect(inbox.text).not.toContain("quick answer");
		expect(receiver.posts.slice(before).some((p) => p.body["text"] === "quick answer")).toBe(false);
	});
});
