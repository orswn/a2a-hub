// A2A client and webhook helpers for the process suites. Like hub-process.ts, it has no import from the hub sources.
import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { Message, SendMessageRequest, type SendMessageRequest as SendMessageRequestType } from "@a2a-js/sdk";
import {
	type Client,
	ClientFactory,
	ClientFactoryOptions,
	DefaultAgentCardResolver,
	JsonRpcTransportFactory,
} from "@a2a-js/sdk/client";
import { BUS_URI, sleep } from "./hub-process.ts";

export {
	BUS_URI,
	HOST_TOKEN,
	type HubProcess,
	LinkClient,
	type LinkEventRecord,
	OTHER_HOST_TOKEN,
	PEER_TOKEN,
	PROTOCOL,
	sleep,
	startHubProcess,
	tempDir,
	writeTokens,
} from "./hub-process.ts";

function authFetch(headers: Record<string, string>): typeof fetch {
	return (input, init) => {
		const merged = new Headers(init?.headers);
		for (const [key, value] of Object.entries(headers)) merged.set(key, value);
		return fetch(input, { ...init, headers: merged });
	};
}

/** The SDK resolver drops sub paths of a base URL, so the client gets the full card URL. */
export async function a2aClient(hubUrl: string, agent: string, headers: Record<string, string> = {}): Promise<Client> {
	const fetchImpl = authFetch(headers);
	const factory = new ClientFactory(
		ClientFactoryOptions.createFrom(ClientFactoryOptions.default, {
			transports: [new JsonRpcTransportFactory({ fetchImpl })],
			cardResolver: new DefaultAgentCardResolver({ fetchImpl }),
		}),
	);
	return factory.createFromUrl(`${hubUrl}/a2a/${agent}/.well-known/agent-card.json`, "");
}

export function bearer(token: string, agent?: string): Record<string, string> {
	const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
	if (agent) headers["X-Pimon-Agent"] = agent;
	return headers;
}

export function sendRequest(
	text: string,
	options: {
		taskId?: string;
		contextId?: string;
		messageId?: string;
		bus?: Record<string, unknown>;
		returnImmediately?: boolean;
		push?: { url: string; token?: string; id?: string };
	} = {},
): SendMessageRequestType {
	const message: Record<string, unknown> = {
		messageId: options.messageId ?? crypto.randomUUID(),
		role: "ROLE_USER",
		parts: [{ text }],
	};
	if (options.taskId) message["taskId"] = options.taskId;
	if (options.contextId) message["contextId"] = options.contextId;
	if (options.bus) message["metadata"] = { [BUS_URI]: options.bus };
	const configuration: Record<string, unknown> = {};
	if (options.returnImmediately) configuration["returnImmediately"] = true;
	if (options.push) configuration["taskPushNotificationConfig"] = options.push;
	return SendMessageRequest.fromJSON({ message: Message.toJSON(Message.fromJSON(message)), configuration });
}

export function partText(item: { parts?: { content?: { $case: string; value: unknown } }[] } | undefined): string {
	return (item?.parts ?? []).map((part) => (part.content?.$case === "text" ? String(part.content.value) : "")).join("");
}

export interface Webhook {
	url: string;
	calls: { path: string; headers: IncomingHttpHeaders; body: Record<string, unknown> }[];
	waitFor(match: (body: Record<string, unknown>) => boolean, timeoutMs?: number): Promise<void>;
	close(): Promise<void>;
}

export async function startWebhook(): Promise<Webhook> {
	const calls: Webhook["calls"] = [];
	const server: Server = createServer((req, res) => {
		let body = "";
		req.on("data", (chunk: Buffer) => {
			body += chunk.toString();
		});
		req.on("end", () => {
			calls.push({ path: req.url ?? "", headers: req.headers, body: JSON.parse(body) as Record<string, unknown> });
			res.writeHead(200).end();
		});
	});
	await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
	const { port } = server.address() as AddressInfo;
	return {
		url: `http://127.0.0.1:${port}`,
		calls,
		async waitFor(match, timeoutMs = 5000) {
			const deadline = Date.now() + timeoutMs;
			while (!calls.some((call) => match(call.body))) {
				if (Date.now() > deadline) throw new Error(`no matching push; got ${JSON.stringify(calls.map((c) => c.body))}`);
				await sleep(20);
			}
		},
		close: () =>
			new Promise<void>((done) => {
				server.closeAllConnections();
				server.close(() => done());
			}),
	};
}
