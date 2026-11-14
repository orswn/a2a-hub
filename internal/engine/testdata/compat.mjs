// Runs the database compatibility scenario with the TypeScript hub engine, or reads a database back with it.
// Usage: node compat.mjs write <pimon checkout> <data dir>
//        node compat.mjs read <pimon checkout> <data dir>
// compat_test.go runs the same scenario through the Go engine. Both must write the same rows, and each
// engine must read the same JSON from either file.
// The committed compat-node files were made with this script from pimon 1cb229e and Node 24.14.0:
// "write" into compat-node, then "read" of a copy into compat-node/reads.json.
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const [mode, pimon, dataDir] = process.argv.slice(2);
if (!["write", "read"].includes(mode) || !pimon || !dataDir) {
	throw new Error("usage: node compat.mjs write|read <pimon checkout> <data dir>");
}
const load = (file) => import(pathToFileURL(join(pimon, "src/hub", file)).href);
const { openDb } = await load("db.ts");
const { Engine } = await load("engine.ts");
const { BUS_EXTENSION_URI, HUB_PROTOCOL } = await load("protocol.ts");

const START = Date.parse("2026-01-01T00:00:00Z");
const END = START + 60_000;
const MARS = { name: "mars", kind: "host" };
const HERMES = { name: "hermes", kind: "peer" };

let clock = mode === "write" ? START : END;
let counter = 0;
const db = openDb(dataDir);
const engine = new Engine(db, {
	now: () => clock,
	newId: () => `id-${String(++counter).padStart(4, "0")}`,
});
const advance = () => {
	clock += 1000;
};

const asker = { principal: MARS, agent: "asker" };
const helper = { principal: MARS, agent: "helper" };
const fromHermes = { principal: HERMES };
const bus = (meta, other = {}) => ({ [BUS_EXTENSION_URI]: meta, ...other });

if (mode === "write") {
	engine.register(MARS, {
		agent: "asker",
		protocol: HUB_PROTOCOL,
		name: "Asker",
		cwd: "/work",
		role: "lead",
		labels: { team: "core", model: "m1" },
		topics: ["build", "build", "ci"],
	});
	engine.register(MARS, { agent: "helper", protocol: HUB_PROTOCOL, name: "Helper", parent: "asker", ephemeral: true });
	engine.ensureBuiltin("tck", "tck");
	engine.ensureExternal("ext", "External");
	engine.setOnline("asker", true);
	engine.setOnline("helper", true);
	advance();
	const sent = engine.send({
		agentId: "helper",
		caller: asker,
		message: {
			messageId: "m-go",
			role: "ROLE_USER",
			parts: [{ text: "go <&> \u2028 héllo ✓", metadata: { k: 1 } }, { data: { n: 1.5, big: 1e21, list: [] } }],
			extensions: ["urn:x"],
			metadata: bus({ kind: "task", expectReply: true, subject: "build it" }, { other: { keep: true } }),
		},
		pushConfig: {
			url: "https://hooks.example/x",
			token: "tok",
			authentication: { scheme: "Bearer", credentials: "cred" },
		},
	});
	const taskId = sent.task.id;
	advance();
	engine.linkUpdate("helper", taskId, { state: "working" });
	advance();
	engine.setStatus("helper", "  reading\n files  ");
	advance();
	engine.applyChange(taskId, {
		artifacts: [{ artifact: { artifactId: "art-1", name: "out", parts: [{ text: "a" }], extra: "x" }, lastChunk: false }],
	});
	engine.applyChange(taskId, {
		artifacts: [{ artifact: { artifactId: "art-1", parts: [{ text: "b" }] }, append: true }],
	});
	advance();
	engine.linkUpdate("helper", taskId, { state: "input-required", text: "which?" });
	advance();
	engine.send({
		agentId: "helper",
		caller: asker,
		message: { messageId: "m-that", role: "ROLE_USER", parts: [{ text: "that" }], taskId },
	});
	advance();
	engine.linkUpdate("helper", taskId, {
		state: "completed",
		text: "done",
		artifact: { name: "report", text: "body", data: { ok: true } },
	});
	advance();
	const note = engine.send({
		agentId: "helper",
		caller: asker,
		message: {
			messageId: "m-note",
			role: "ROLE_USER",
			parts: [{ text: "fyi" }],
			contextId: "ctx-note",
			metadata: bus({ kind: "note", wake: "never", priority: "urgent", expectReply: true, data: null }),
		},
	});
	advance();
	engine.send({
		agentId: "asker",
		caller: helper,
		message: {
			messageId: "m-reply",
			role: "ROLE_USER",
			parts: [{ text: "ack" }],
			contextId: note.message.contextId,
			metadata: bus({ kind: "answer", replyTo: "m-note" }),
		},
	});
	advance();
	const hermesTask = engine.send({
		agentId: "helper",
		caller: fromHermes,
		message: { messageId: "m-hermes", role: "ROLE_USER", parts: [{ text: "peer job" }], contextId: "ctx-hermes" },
	}).task.id;
	engine.createPushConfig("helper", fromHermes, { taskId: hermesTask, url: "http://hooks.example:8080/p" });
	engine.linkUpdate("helper", hermesTask, { state: "working", text: "on it" });
	advance();
	const [first] = engine.unacked("helper");
	engine.ack("helper", first.seq);
	engine.markRead("helper", ["m-go", "m-note"]);
	advance();
	const extTask = engine.send({
		agentId: "ext",
		caller: asker,
		message: { messageId: "m-ext", role: "ROLE_USER", parts: [{ text: "research" }] },
	}).task.id;
	const remote = engine.startRemote(engine.externalJobs()[0]);
	engine.remoteStarted(remote, "remote-1", "remote-ctx");
	engine.remotePolled(remote);
	advance();
	engine.cancel("ext", asker, extTask);
	advance();
	engine.setTopics("asker", ["deploy"], ["ci"]);
	engine.setState("helper", "busy", "compiling");
	engine.setOnline("helper", false);
	db.exec("VACUUM");
	db.close();
} else {
	const tasks = engine.listTasks("helper", { principal: MARS }, { pageSize: 10, includeArtifacts: true });
	const ext = engine.listTasks("ext", asker, { pageSize: 10, includeArtifacts: true });
	const out = {
		agents: engine.listAgents(true),
		tasks: [...tasks.tasks, ...ext.tasks].map((task) => engine.readTask(task.id)),
		page: engine.listTasks("helper", { principal: MARS }, { pageSize: 1, includeArtifacts: false, historyLength: 1 }),
		inbox: ["asker", "helper"].map((agent) => engine.inbox(agent, false, 50)),
		unacked: ["asker", "helper"].map((agent) => engine.unacked(agent)),
		thread: engine.thread("asker", "ctx-note"),
		message: engine.message("asker", "m-go"),
		pending: engine.pending("asker"),
		owed: engine.owed("helper"),
		pushConfigs: tasks.tasks.flatMap((task) =>
			[{ principal: MARS }, fromHermes].map((caller) => {
				try {
					return engine.listPushConfigs("helper", caller, task.id);
				} catch (error) {
					return error.reason;
				}
			}),
		),
		due: engine.duePushes(END, 50),
		jobs: engine.externalJobs(),
		remotes: engine.activeRemotes(),
		outbound: engine.outboundMessage(ext.tasks[0].id, "m-ext"),
	};
	db.close();
	process.stdout.write(JSON.stringify(out));
}
