package store

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// rawOpen opens a database file without migrations.
func rawOpen(t *testing.T, file string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+uriPath(file)+"?"+connPragmas)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func open(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func exec1(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func scalar[T any](t *testing.T, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, query string) T {
	t.Helper()
	var value T
	if err := db.QueryRowContext(context.Background(), query).Scan(&value); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return value
}

func tables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	text := scalar[string](t, db, "SELECT group_concat(name, ',') FROM (SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name)")
	return strings.Split(text, ",")
}

var allTables = []string{
	"agents", "artifacts", "expect_reply", "external_outbox", "link_events", "messages", "push_configs",
	"push_outbox", "remote_tasks", "sqlite_sequence", "task_messages", "tasks",
}

func TestOpenCreatesTheSchemaWithThePragmas(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	db := open(t, dir)
	if _, err := os.Stat(filepath.Join(dir, FileName)); err != nil {
		t.Fatal(err)
	}
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 2 || SchemaVersion != 2 {
		t.Errorf("user_version = %d, SchemaVersion = %d, want 2", got, SchemaVersion)
	}
	if got := tables(t, db); !reflect.DeepEqual(got, allTables) {
		t.Errorf("tables = %q", got)
	}
	pragmas := map[string]string{"journal_mode": "wal", "foreign_keys": "1", "busy_timeout": "5000", "synchronous": "1"}
	for name, want := range pragmas {
		if got := scalar[string](t, db, "PRAGMA "+name); got != want {
			t.Errorf("PRAGMA %s = %s, want %s", name, got, want)
		}
	}
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("MaxOpenConnections = %d, want 1", got)
	}
}

func TestOpenInMemory(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	db := open(t, Memory)
	exec1(t, db, "INSERT INTO agents (id, kind, name, registered_at) VALUES ('a1', 'pi', 'bee', 'x')")
	if got := scalar[int](t, db, "SELECT count(*) FROM agents"); got != 1 {
		t.Errorf("agents = %d, want 1", got)
	}
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 2 {
		t.Errorf("user_version = %d", got)
	}
	if got := scalar[string](t, db, "PRAGMA journal_mode"); got != "memory" {
		t.Errorf("journal_mode = %s, want memory", got)
	}
	if got := scalar[int](t, db, "PRAGMA foreign_keys"); got != 1 {
		t.Errorf("foreign_keys = %d", got)
	}
	for _, name := range []string{Memory, "file:" + Memory} {
		if _, err := os.Stat(filepath.Join(cwd, name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a file %q exists", name)
		}
	}
}

func TestUpgradesVersion1AndKeepsRows(t *testing.T) {
	file := filepath.Join(t.TempDir(), FileName)
	raw := rawOpen(t, file)
	exec1(t, raw, Migrations[0].SQL)
	exec1(t, raw, "PRAGMA user_version = 1")
	exec1(t, raw, "INSERT INTO agents (id, kind, host, name, registered_at) VALUES ('a1', 'pi', 'mars', 'bee', '2026-11-14T00:00:00.000Z')")
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	db := open(t, filepath.Dir(file))
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 2 {
		t.Errorf("user_version = %d, want 2", got)
	}
	if got := tables(t, db); !reflect.DeepEqual(got, allTables) {
		t.Errorf("tables = %q", got)
	}
	if got := scalar[string](t, db, "SELECT id || ',' || kind || ',' || name FROM agents"); got != "a1,pi,bee" {
		t.Errorf("agent = %s", got)
	}
	if err := Migrate(context.Background(), db, Migrations); err != nil {
		t.Errorf("second Migrate: %v", err)
	}
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 2 {
		t.Errorf("user_version after a second run = %d", got)
	}
}

func TestRefusesDatabaseFromNewerHub(t *testing.T) {
	dir := t.TempDir()
	raw := rawOpen(t, filepath.Join(dir, FileName))
	exec1(t, raw, "PRAGMA user_version = 3")
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := Open(context.Background(), dir)
	var newer *NewerSchemaError
	if !errors.As(err, &newer) || err.Error() != "database schema 3 is newer than this hub (2)" {
		t.Fatalf("Open = %v, want the newer schema error", err)
	}
	check := rawOpen(t, filepath.Join(dir, FileName))
	if got := scalar[int](t, check, "SELECT count(*) FROM sqlite_master"); got != 0 {
		t.Errorf("the refused database got %d schema objects", got)
	}
}

func TestFailedMigrationRollsBack(t *testing.T) {
	db := open(t, Memory)
	migrations := append(append([]Migration{}, Migrations...), Migration{SQL: "CREATE TABLE extra (id TEXT); INSERT INTO nowhere VALUES (1);"})
	if err := Migrate(context.Background(), db, migrations); err == nil {
		t.Fatal("Migrate passed with a broken migration")
	}
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 2 {
		t.Errorf("user_version = %d, want 2", got)
	}
	if got := scalar[int](t, db, "SELECT count(*) FROM sqlite_master WHERE name = 'extra'"); got != 0 {
		t.Error("the table of the failed migration exists")
	}
}

// agentsWithSessionKind is a rebuild of agents for a CHECK change, as a later schema needs it.
const agentsWithSessionKind = `
	CREATE TABLE agents_new (
		id TEXT PRIMARY KEY,
		kind TEXT NOT NULL CHECK (kind IN ('session', 'external', 'builtin')),
		host TEXT, name TEXT NOT NULL, role TEXT, cwd TEXT, labels TEXT NOT NULL DEFAULT '{}', parent TEXT,
		topics TEXT NOT NULL DEFAULT '[]', ephemeral INTEGER NOT NULL DEFAULT 0, online INTEGER NOT NULL DEFAULT 0,
		state TEXT NOT NULL DEFAULT 'idle' CHECK (state IN ('busy', 'idle')), activity TEXT,
		status_text TEXT NOT NULL DEFAULT '', status_at TEXT, registered_at TEXT NOT NULL, last_seen_at TEXT, offline_at TEXT
	);
	INSERT INTO agents_new SELECT id, CASE kind WHEN 'pi' THEN 'session' ELSE kind END, host, name, role, cwd, labels,
		parent, topics, ephemeral, online, state, activity, status_text, status_at, registered_at, last_seen_at, offline_at
		FROM agents;
	DROP TABLE agents;
	ALTER TABLE agents_new RENAME TO agents;
	CREATE INDEX agents_name ON agents (name);
	CREATE INDEX agents_parent ON agents (parent);
`

func seedLinkEvents(t *testing.T, db *sql.DB) {
	t.Helper()
	exec1(t, db, "INSERT INTO agents (id, kind, name, registered_at) VALUES ('a1', 'pi', 'alpha', 'x'), ('a2', 'pi', 'beta', 'x'), ('h', 'external', 'hermes', 'x')")
	for _, agent := range []string{"a1", "a2", "a2"} {
		exec1(t, db, "INSERT INTO link_events (agent_id, type, payload, created_at) VALUES (?, 'message', '{}', 'x')", agent)
	}
}

func pinned(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestRebuildKeepsLinkEvents(t *testing.T) {
	db := open(t, Memory)
	seedLinkEvents(t, db)
	conn := pinned(t, db)
	err := RebuildTables(context.Background(), conn, func(ctx context.Context, conn *sql.Conn) error {
		if got := scalar[int](t, conn, "PRAGMA foreign_keys"); got != 0 {
			t.Errorf("foreign_keys in the rebuild = %d, want 0", got)
		}
		_, err := conn.ExecContext(ctx, agentsWithSessionKind)
		return err
	})
	if err != nil {
		t.Fatalf("RebuildTables: %v", err)
	}
	if got := scalar[int](t, conn, "SELECT count(*) FROM link_events"); got != 3 {
		t.Errorf("link_events = %d, want 3", got)
	}
	if got := scalar[string](t, conn, "SELECT group_concat(kind, ',') FROM (SELECT kind FROM agents ORDER BY id)"); got != "session,session,external" {
		t.Errorf("kinds = %s", got)
	}
	if got := scalar[int](t, conn, "PRAGMA foreign_keys"); got != 1 {
		t.Errorf("foreign_keys after the rebuild = %d, want 1", got)
	}
	if _, err := conn.ExecContext(context.Background(), "INSERT INTO link_events (agent_id, type, payload, created_at) VALUES ('nobody', 'message', '{}', 'x')"); err == nil {
		t.Error("foreign keys are not enforced after the rebuild")
	}
}

func TestRebuildRollsBackOnForeignKeyViolation(t *testing.T) {
	db := open(t, Memory)
	seedLinkEvents(t, db)
	conn := pinned(t, db)
	err := RebuildTables(context.Background(), conn, func(ctx context.Context, conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, agentsWithSessionKind+"DELETE FROM agents WHERE id = 'a2';")
		return err
	})
	var fkErr *ForeignKeyError
	if !errors.As(err, &fkErr) || len(fkErr.Violations) != 2 {
		t.Fatalf("RebuildTables = %v, want 2 foreign key violations", err)
	}
	if got := scalar[string](t, conn, "SELECT group_concat(kind, ',') FROM (SELECT kind FROM agents ORDER BY id)"); got != "pi,pi,external" {
		t.Errorf("kinds after the rollback = %s", got)
	}
	if got := scalar[int](t, conn, "SELECT count(*) FROM link_events"); got != 3 {
		t.Errorf("link_events = %d, want 3", got)
	}
	if got := scalar[int](t, conn, "PRAGMA foreign_keys"); got != 1 {
		t.Errorf("foreign_keys after the failed rebuild = %d, want 1", got)
	}
}

func TestRebuildWithForeignKeysOnDeletesLinkEvents(t *testing.T) {
	db := open(t, Memory)
	seedLinkEvents(t, db)
	exec1(t, db, "BEGIN IMMEDIATE;"+agentsWithSessionKind+"COMMIT;")
	if got := scalar[int](t, db, "SELECT count(*) FROM link_events"); got != 0 {
		t.Errorf("link_events = %d: the cascade no longer happens, so the rebuild guard needs a new reason", got)
	}
}

func TestMigrationThatRebuildsTables(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	seedLinkEvents(t, db)
	migrations := append(append([]Migration{}, Migrations...), Migration{SQL: agentsWithSessionKind, RebuildsTables: true})
	if err := Migrate(context.Background(), db, migrations); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 3 {
		t.Errorf("user_version = %d, want 3", got)
	}
	if got := scalar[int](t, db, "SELECT count(*) FROM link_events"); got != 3 {
		t.Errorf("link_events = %d, want 3", got)
	}
	if got := scalar[int](t, db, "PRAGMA foreign_keys"); got != 1 {
		t.Errorf("foreign_keys = %d, want 1", got)
	}
}

var nodeFixtureRows = map[string]int{
	"agents": 3, "artifacts": 1, "expect_reply": 1, "external_outbox": 1, "link_events": 3, "messages": 2,
	"push_configs": 1, "push_outbox": 1, "remote_tasks": 1, "sqlite_sequence": 4, "task_messages": 1, "tasks": 2,
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	in, err := os.Open(from)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(to)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

// checkNodeDatabase opens a database that the TypeScript hub wrote and checks its schema, rows, and a write.
func checkNodeDatabase(t *testing.T, dir string) {
	t.Helper()
	db := open(t, dir)
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 2 {
		t.Errorf("user_version = %d, want 2", got)
	}
	if got := tables(t, db); !reflect.DeepEqual(got, allTables) {
		t.Errorf("tables = %q", got)
	}
	for table, want := range nodeFixtureRows {
		if got := scalar[int](t, db, "SELECT count(*) FROM "+table); got != want {
			t.Errorf("%s rows = %d, want %d", table, got, want)
		}
	}
	if got := scalar[string](t, db, "SELECT group_concat(json_extract(payload, '$.from.kind')) FROM link_events"); got != "pi,pi,pi" {
		t.Errorf("link event sender kinds = %s", got)
	}
	result, err := db.ExecContext(context.Background(), "INSERT INTO link_events (agent_id, type, payload, created_at) VALUES ('a1', 'message', '{}', 'x')")
	if err != nil {
		t.Fatal(err)
	}
	if seq, _ := result.LastInsertId(); seq != 4 {
		t.Errorf("new link event seq = %d, want 4", seq)
	}

	fresh := open(t, filepath.Join(t.TempDir(), "fresh"))
	const schemaSQL = "SELECT group_concat(type || ' ' || name || ': ' || coalesce(sql, ''), char(10)) FROM (SELECT * FROM sqlite_master ORDER BY name)"
	if got, want := scalar[string](t, fresh, schemaSQL), scalar[string](t, db, schemaSQL); got != want {
		t.Errorf("schema differs from the TypeScript hub schema\ngo:\n%s\nnode:\n%s", got, want)
	}
}

func TestOpensDatabaseWrittenByTheTypeScriptHub(t *testing.T) {
	dir := t.TempDir()
	copyFile(t, filepath.Join("testdata", "node-v2", FileName), filepath.Join(dir, FileName))
	checkNodeDatabase(t, dir)
}

// PIMON_DIR names a pimon checkout. With it and node on PATH, the test writes a new database with the
// TypeScript hub's current schema code instead of the committed file.
func TestOpensDatabaseFreshlyWrittenByTheTypeScriptHub(t *testing.T) {
	pimon := os.Getenv("PIMON_DIR")
	if pimon == "" {
		t.Skip("PIMON_DIR is not set")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not on PATH")
	}
	dir := t.TempDir()
	cmd := exec.Command(node, "--no-warnings", filepath.Join("testdata", "node-fixture.mjs"), pimon, dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("node-fixture.mjs: %v\n%s", err, out)
	}
	checkNodeDatabase(t, dir)
}
