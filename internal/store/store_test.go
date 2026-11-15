package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 3 || SchemaVersion != 3 {
		t.Errorf("user_version = %d, SchemaVersion = %d, want 3", got, SchemaVersion)
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
	exec1(t, db, "INSERT INTO agents (id, kind, name, registered_at) VALUES ('a1', 'session', 'bee', 'x')")
	if got := scalar[int](t, db, "SELECT count(*) FROM agents"); got != 1 {
		t.Errorf("agents = %d, want 1", got)
	}
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 3 {
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
	exec1(t, raw, "INSERT INTO agents (id, kind, host, name, registered_at) VALUES ('a1', ?, 'mars', 'bee', '2026-11-14T00:00:00.000Z')", legacyKind)
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	db := open(t, filepath.Dir(file))
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 3 {
		t.Errorf("user_version = %d, want 3", got)
	}
	if got := tables(t, db); !reflect.DeepEqual(got, allTables) {
		t.Errorf("tables = %q", got)
	}
	if got := scalar[string](t, db, "SELECT id || ',' || kind || ',' || name || ',' || labels FROM agents"); got != `a1,session,bee,{"harness":"pi"}` {
		t.Errorf("agent = %s", got)
	}
	if err := Migrate(context.Background(), db, Migrations); err != nil {
		t.Errorf("second Migrate: %v", err)
	}
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 3 {
		t.Errorf("user_version after a second run = %d", got)
	}
}

func TestRefusesDatabaseFromNewerHub(t *testing.T) {
	dir := t.TempDir()
	raw := rawOpen(t, filepath.Join(dir, FileName))
	exec1(t, raw, "PRAGMA user_version = 4")
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := Open(context.Background(), dir)
	var newer *NewerSchemaError
	if !errors.As(err, &newer) || err.Error() != "database schema 4 is newer than this hub (3)" {
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
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 3 {
		t.Errorf("user_version = %d, want 3", got)
	}
	if got := scalar[int](t, db, "SELECT count(*) FROM sqlite_master WHERE name = 'extra'"); got != 0 {
		t.Error("the table of the failed migration exists")
	}
}

// openAt makes a database file at an older schema version.
func openAt(t *testing.T, version int) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	raw := rawOpen(t, filepath.Join(dir, FileName))
	if err := Migrate(context.Background(), raw, Migrations[:version]); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	return rawOpen(t, filepath.Join(dir, FileName))
}

// legacyKind is the kind of a session agent in schema 2.
const legacyKind = "pi"

// agentsWithSessionKind rebuilds the agents table of schema 2.
var agentsWithSessionKind = Migrations[2].SQL

func seedLinkEvents(t *testing.T, db *sql.DB, kind string) {
	t.Helper()
	exec1(t, db, "INSERT INTO agents (id, kind, name, registered_at) VALUES ('a1', ?, 'alpha', 'x'), ('a2', ?, 'beta', 'x'), ('h', 'external', 'hermes', 'x')", kind, kind)
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
	db := openAt(t, 2)
	seedLinkEvents(t, db, legacyKind)
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
	db := openAt(t, 2)
	seedLinkEvents(t, db, legacyKind)
	conn := pinned(t, db)
	err := RebuildTables(context.Background(), conn, func(ctx context.Context, conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, agentsWithSessionKind+"DELETE FROM agents WHERE id = 'a2';")
		return err
	})
	var fkErr *ForeignKeyError
	if !errors.As(err, &fkErr) || len(fkErr.Violations) != 2 {
		t.Fatalf("RebuildTables = %v, want 2 foreign key violations", err)
	}
	if got := scalar[string](t, conn, "SELECT group_concat(kind, ',') FROM (SELECT kind FROM agents ORDER BY id)"); got != legacyKind+","+legacyKind+",external" {
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
	db := openAt(t, 2)
	seedLinkEvents(t, db, legacyKind)
	exec1(t, db, "BEGIN IMMEDIATE;"+agentsWithSessionKind+"COMMIT;")
	if got := scalar[int](t, db, "SELECT count(*) FROM link_events"); got != 0 {
		t.Errorf("link_events = %d: the cascade no longer happens, so the rebuild guard needs a new reason", got)
	}
}

func TestMigrationThatDropsAReferencedTableKeepsItsChildRows(t *testing.T) {
	db := openAt(t, 2)
	seedLinkEvents(t, db, legacyKind)
	if err := Migrate(context.Background(), db, Migrations); err != nil {
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

func TestMigrationNamesTheRowThatBreaksAForeignKey(t *testing.T) {
	db := open(t, Memory)
	seedLinkEvents(t, db, "session")
	migrations := append(append([]Migration{}, Migrations...), Migration{SQL: "DELETE FROM agents WHERE id = 'a1';"})
	err := Migrate(context.Background(), db, migrations)
	if err == nil || err.Error() != "migrate schema 3 to 4: foreign key check failed: link_events row 1 has no agents row" {
		t.Fatalf("Migrate = %v", err)
	}
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 3 {
		t.Errorf("user_version = %d, want 3", got)
	}
	if got := scalar[int](t, db, "SELECT count(*) FROM agents"); got != 3 {
		t.Errorf("agents = %d, want 3 after the rollback", got)
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

// dump lists every row of every table, so two dumps show any change.
func dump(t *testing.T, db *sql.DB) string {
	t.Helper()
	var out strings.Builder
	for _, table := range tables(t, db) {
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM "+table+" ORDER BY rowid")
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			values := make([]any, len(cols))
			dest := make([]any, len(cols))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			out.WriteString(table)
			for i, v := range values {
				fmt.Fprintf(&out, " %s=%#v", cols[i], v)
			}
			out.WriteString("\n")
		}
		_ = rows.Close()
	}
	return out.String()
}

func column(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	out := []string{}
	for rows.Next() {
		var value sql.NullString
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		out = append(out, value.String)
	}
	return out
}

func expectColumn(t *testing.T, db *sql.DB, query string, want ...string) {
	t.Helper()
	if got := column(t, db, query); !reflect.DeepEqual(got, want) {
		t.Errorf("%s\n got %q\nwant %q", query, got, want)
	}
}

// reopenIsANoOp opens the migrated database again and checks that no row changed.
func reopenIsANoOp(t *testing.T, dir string, db *sql.DB) {
	t.Helper()
	before := dump(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	again := open(t, dir)
	if got := scalar[int](t, again, "PRAGMA user_version"); got != 3 {
		t.Errorf("user_version after a second open = %d, want 3", got)
	}
	if after := dump(t, again); after != before {
		t.Errorf("a second open changed rows\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

// checkNodeDatabase opens a database that the TypeScript hub wrote, migrates it, and checks its schema, rows,
// and a write.
func checkNodeDatabase(t *testing.T, dir string) {
	t.Helper()
	db := open(t, dir)
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 3 {
		t.Errorf("user_version = %d, want 3", got)
	}
	if got := tables(t, db); !reflect.DeepEqual(got, allTables) {
		t.Errorf("tables = %q", got)
	}
	for table, want := range nodeFixtureRows {
		if got := scalar[int](t, db, "SELECT count(*) FROM "+table); got != want {
			t.Errorf("%s rows = %d, want %d", table, got, want)
		}
	}
	expectColumn(t, db, "SELECT id || ' ' || kind || ' ' || labels FROM agents ORDER BY id",
		`a1 session {"team":"core","harness":"pi"}`, `a2 session {"harness":"pi"}`, "hermes external {}")
	expectColumn(t, db, "SELECT sender_kind FROM messages ORDER BY seq", "session", "session")
	const payload = `{"from":{"agent":"a1","name":"alpha","kind":"session"},"text":"hi"}`
	expectColumn(t, db, "SELECT payload FROM link_events ORDER BY seq", payload, payload, payload)
	expectColumn(t, db, "SELECT agent_id || ' ' || coalesce(acked_at, 'unacked') FROM link_events ORDER BY seq",
		"a2 unacked", "a2 unacked", "a1 2026-11-14T00:00:00.000Z")
	reopenIsANoOp(t, dir, db)
	db = open(t, dir)
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
		t.Errorf("the migrated schema differs from a new schema\nnew:\n%s\nmigrated:\n%s", got, want)
	}
}

// A schema 2 file with every kind of agent, sender, and link event that the TypeScript hub or this hub wrote.
func TestMigrationMapsThePiKindToSessionInEveryTable(t *testing.T) {
	dir := t.TempDir()
	raw := rawOpen(t, filepath.Join(dir, FileName))
	if err := Migrate(context.Background(), raw, Migrations[:2]); err != nil {
		t.Fatal(err)
	}
	at := "2026-11-14T00:00:00.000Z"
	agents := [][]any{
		{"lead", legacyKind, "mars", nil, `{"team":"core"}`, 1, 0},
		{"kid", legacyKind, "mars", "lead", `{}`, 0, 1},
		{"claude", legacyKind, "terra", nil, `{"harness":"claude-code"}`, 1, 0},
		{"blank", legacyKind, "terra", nil, `{"harness":""}`, 0, 0},
		{"hermes", "external", nil, nil, `{}`, 0, 0},
		{"tck", "builtin", nil, nil, `{}`, 1, 0},
	}
	for _, a := range agents {
		exec1(t, raw, `INSERT INTO agents (id, kind, host, name, parent, labels, online, ephemeral, registered_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, a[0], a[1], a[2], a[0], a[3], a[4], a[5], a[6], at)
	}
	exec1(t, raw, `INSERT INTO tasks (id, agent_id, sender_agent, sender_principal, context_id, state, status_at, kind, created_at, updated_at)
		VALUES ('t1', 'kid', 'lead', 'mars', 'c1', 'TASK_STATE_WORKING', ?, 'task', ?, ?)`, at, at, at)
	exec1(t, raw, `INSERT INTO expect_reply (sender_agent, message_id, recipient_agent, context_id, task_id, created_at)
		VALUES ('lead', 'm1', 'kid', 'c1', 't1', ?)`, at)
	senders := [][]any{{"m1", "kid", "lead", legacyKind}, {"m2", "lead", nil, "peer"}, {"m3", "lead", "hermes", "external"},
		{"m4", "lead", "tck", "builtin"}, {"m5", "hermes", "lead", legacyKind}, {"m6", "lead", nil, "host"}}
	for _, m := range senders {
		exec1(t, raw, `INSERT INTO messages (id, agent_id, sender_agent, sender_principal, sender_name, sender_kind, context_id, kind, message, created_at)
			VALUES (?, ?, ?, 'mars', 'x', ?, 'c1', 'task', '{}', ?)`, m[0], m[1], m[2], m[3], at)
	}
	pi := `{"messageId":"m1","contextId":"c1","from":{"agent":"lead","name":"l\u00e9ad ✓","kind":"` + legacyKind +
		`"},"kind":"task","wake":"now","text":"x\u2028y \"kind\":\"` + legacyKind + `\"","data":{"n":1e21,"f":1.50,"list":[]}}`
	events := [][]any{
		{"kid", "message", pi, nil},
		{"kid", "message", pi, at},
		{"lead", "message", `{"messageId":"m2","contextId":"c1","from":{"name":"hermes","kind":"peer"},"kind":"task","wake":"now","text":"t"}`, nil},
		{"lead", "message", `{"messageId":"m3","contextId":"c1","from":{"agent":"hermes","name":"hermes","kind":"external"},"kind":"answer","wake":"now","text":"t"}`, nil},
		{"lead", "update", `{"taskId":"t1","contextId":"c1","state":"working","from":{"agent":"kid","name":"kid"}}`, nil},
		{"lead", "presence", `{"agent":"kid","name":"kid","online":false,"relation":"child"}`, nil},
		{"kid", "control", `{"action":"close","from":{"agent":"lead","name":"lead"}}`, nil},
	}
	for _, e := range events {
		exec1(t, raw, "INSERT INTO link_events (agent_id, type, payload, created_at, acked_at) VALUES (?, ?, ?, ?, ?)", e[0], e[1], e[2], at, e[3])
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	db := open(t, dir)
	if got := scalar[int](t, db, "PRAGMA user_version"); got != 3 {
		t.Errorf("user_version = %d, want 3", got)
	}
	expectColumn(t, db, "SELECT id || ' ' || kind || ' ' || coalesce(host, '-') || ' ' || coalesce(parent, '-') || ' ' || labels || ' ' || online || ephemeral FROM agents ORDER BY rowid",
		`lead session mars - {"team":"core","harness":"pi"} 10`,
		`kid session mars lead {"harness":"pi"} 01`,
		`claude session terra - {"harness":"claude-code"} 10`,
		`blank session terra - {"harness":"pi"} 00`,
		"hermes external - - {} 00",
		"tck builtin - - {} 10")
	expectColumn(t, db, "SELECT id || ' ' || sender_kind FROM messages ORDER BY seq",
		"m1 session", "m2 peer", "m3 external", "m4 builtin", "m5 session", "m6 host")
	session := strings.Replace(pi, `"kind":"`+legacyKind+`"},`, `"kind":"session"},`, 1)
	want := []string{session, session}
	for _, e := range events[2:] {
		want = append(want, e[2].(string))
	}
	expectColumn(t, db, "SELECT payload FROM link_events ORDER BY seq", want...)
	expectColumn(t, db, "SELECT seq || ' ' || agent_id || ' ' || coalesce(acked_at, 'unacked') FROM link_events ORDER BY seq",
		"1 kid unacked", "2 kid "+at, "3 lead unacked", "4 lead unacked", "5 lead unacked", "6 lead unacked", "7 kid unacked")
	expectColumn(t, db, "SELECT id || ' ' || agent_id || ' ' || sender_agent FROM tasks", "t1 kid lead")
	expectColumn(t, db, "SELECT message_id || ' ' || recipient_agent FROM expect_reply", "m1 kid")
	expectColumn(t, db, "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'agents' AND sql IS NOT NULL ORDER BY name",
		"agents_name", "agents_parent")
	if _, err := db.ExecContext(context.Background(), "INSERT INTO agents (id, kind, name, registered_at) VALUES ('old', ?, 'old', 'x')", legacyKind); err == nil {
		t.Error("the agents table takes the old kind")
	}
	reopenIsANoOp(t, dir, db)
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
