// Writes a hub database with the TypeScript hub's own openDb and one or more rows in every table.
// Usage: node node-fixture.mjs <pimon checkout> <data dir>
// The committed node-v2/hub.sqlite was made with this script from pimon 1cb229e and Node 24.14.0.
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const [pimon, dataDir] = process.argv.slice(2);
if (!pimon || !dataDir) throw new Error("usage: node node-fixture.mjs <pimon checkout> <data dir>");
const { openDb } = await import(pathToFileURL(join(pimon, "src/hub/db.ts")).href);

const at = "2026-11-14T00:00:00.000Z";
const db = openDb(dataDir);
const run = (sql, ...args) => db.prepare(sql).run(...args);

run("INSERT INTO agents (id, kind, host, name, labels, topics, online, registered_at) VALUES (?, 'pi', 'mars', 'alpha', '{\"team\":\"core\"}', '[\"build\"]', 1, ?)", "a1", at);
run("INSERT INTO agents (id, kind, host, name, parent, registered_at) VALUES (?, 'pi', 'mars', 'kid', 'a1', ?)", "a2", at);
run("INSERT INTO agents (id, kind, name, registered_at) VALUES (?, 'external', 'hermes', ?)", "hermes", at);

run("INSERT INTO tasks (id, agent_id, sender_agent, sender_principal, context_id, state, status_at, kind, created_at, updated_at) VALUES ('t1', 'a2', 'a1', 'mars', 'c1', 'TASK_STATE_WORKING', ?, 'task', ?, ?)", at, at, at);
run("INSERT INTO tasks (id, agent_id, sender_agent, sender_principal, context_id, state, status_at, kind, created_at, updated_at) VALUES ('t2', 'hermes', 'a1', 'mars', 'c2', 'TASK_STATE_SUBMITTED', ?, 'task', ?, ?)", at, at, at);
run("INSERT INTO task_messages (task_id, seq, message) VALUES ('t1', 0, '{\"messageId\":\"m1\"}')");
run("INSERT INTO artifacts (task_id, artifact_id, seq, artifact) VALUES ('t1', 'art1', 0, '{\"name\":\"notes\"}')");

const message = (id, to) =>
	run("INSERT INTO messages (id, agent_id, sender_agent, sender_principal, sender_name, sender_kind, context_id, task_id, kind, message, created_at) VALUES (?, ?, 'a1', 'mars', 'alpha', 'pi', 'c1', 't1', 'task', '{}', ?)", id, to, at);
message("m1", "a2");
message("m2", "hermes");

const event = (agent, acked) =>
	run("INSERT INTO link_events (agent_id, type, payload, created_at, acked_at) VALUES (?, 'message', '{\"from\":{\"agent\":\"a1\",\"name\":\"alpha\",\"kind\":\"pi\"},\"text\":\"hi\"}', ?, ?)", agent, at, acked);
event("a2", null);
event("a2", null);
event("a1", at);

run("INSERT INTO push_configs (task_id, id, url, principal, created_at) VALUES ('t1', 'p1', 'https://hooks.example/a', 'mars', ?)", at);
run("INSERT INTO push_outbox (task_id, config_id, payload, next_at) VALUES ('t1', 'p1', '{}', 0)");
run("INSERT INTO expect_reply (sender_agent, message_id, recipient_agent, context_id, task_id, created_at) VALUES ('a1', 'm1', 'a2', 'c1', 't1', ?)", at);
run("INSERT INTO external_outbox (agent_id, task_id, context_id, message_id, op, first_at, next_at) VALUES ('hermes', 't2', 'c2', 'm2', 'send', 0, 0)");
run("INSERT INTO remote_tasks (task_id, seq, agent_id, context_id, state, dispatched_at) VALUES ('t2', 0, 'hermes', 'c2', 'dispatched', 0)");

db.close();
