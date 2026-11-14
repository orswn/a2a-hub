// Package store opens the hub sqlite database and keeps its schema current.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

// Memory is the data path of a database that lives only in memory.
const Memory = ":memory:"

// FileName is the database file in the data directory.
const FileName = "hub.sqlite"

const (
	connPragmas = "_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	walPragma   = "_pragma=journal_mode(WAL)&"
)

// Migration is one schema change. Its SQL runs through RebuildTables, in one transaction together with the
// user_version update.
type Migration struct {
	SQL string
}

// NewerSchemaError is a database written by a newer hub.
type NewerSchemaError struct {
	Found, Known int
}

func (e *NewerSchemaError) Error() string {
	return fmt.Sprintf("database schema %d is newer than this hub (%d)", e.Found, e.Known)
}

// Open opens hub.sqlite in the data directory, or an in-memory database for Memory, and migrates it.
// The pool has one connection: sqlite has one writer, and an in-memory database exists only on its connection.
func Open(ctx context.Context, path string) (*sql.DB, error) {
	dsn := "file:" + Memory + "?" + connPragmas
	if path != Memory {
		if err := os.MkdirAll(path, 0o777); err != nil { //nolint:gosec // G301: the umask decides, like mkdirSync in the TypeScript hub.
			return nil, err
		}
		dsn = "file:" + uriPath(filepath.Join(path, FileName)) + "?" + walPragma + connPragmas
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	if err := Migrate(ctx, db, Migrations); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// uriPath escapes the characters that end or escape the path part of an sqlite file: URI.
func uriPath(path string) string {
	return strings.NewReplacer("%", "%25", "?", "%3f", "#", "%23").Replace(path)
}

// Migrate applies the migrations after the database user_version. It refuses a database from a newer hub.
func Migrate(ctx context.Context, db *sql.DB, migrations []Migration) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var from int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&from); err != nil {
		return err
	}
	if from > len(migrations) {
		return &NewerSchemaError{Found: from, Known: len(migrations)}
	}
	for version := from; version < len(migrations); version++ {
		migration := migrations[version]
		apply := func(ctx context.Context, conn *sql.Conn) error {
			if _, err := conn.ExecContext(ctx, migration.SQL); err != nil {
				return err
			}
			_, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", version+1))
			return err
		}
		// Every migration runs with foreign keys off, so a DROP TABLE cannot cascade into child rows. A
		// migration that would have cascaded fails at the foreign key check instead.
		if err := RebuildTables(ctx, conn, apply); err != nil {
			return fmt.Errorf("migrate schema %d to %d: %w", version, version+1, err)
		}
	}
	return nil
}

func inTransaction(ctx context.Context, conn *sql.Conn, body func(context.Context, *sql.Conn) error) error {
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	if err := body(ctx, conn); err != nil {
		return rollback(ctx, conn, err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return rollback(ctx, conn, err)
	}
	return nil
}

func rollback(ctx context.Context, conn *sql.Conn, cause error) error {
	if _, err := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK"); err != nil && !strings.Contains(err.Error(), "no transaction is active") {
		return errors.Join(cause, err)
	}
	return cause
}

// ForeignKeyError lists the rows that have no parent row at the end of a rebuild.
type ForeignKeyError struct {
	Violations []string
}

func (e *ForeignKeyError) Error() string {
	return "foreign key check failed: " + strings.Join(e.Violations, ", ")
}

// RebuildTables runs body in one transaction with foreign keys off, for a change that drops or renames a
// referenced table. With foreign keys on, DROP TABLE runs the ON DELETE CASCADE of each child table, so a
// rebuild of agents would delete every link_events row. PRAGMA foreign_keys is a no-op inside a
// transaction, so it is set on the pinned connection before BEGIN. PRAGMA foreign_key_check must find
// no row before COMMIT, else the transaction rolls back.
func RebuildTables(ctx context.Context, conn *sql.Conn, body func(context.Context, *sql.Conn) error) (err error) {
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return err
	}
	defer func() {
		if _, onErr := conn.ExecContext(context.WithoutCancel(ctx), "PRAGMA foreign_keys = ON"); onErr != nil {
			err = errors.Join(err, onErr)
		}
	}()
	return inTransaction(ctx, conn, func(ctx context.Context, conn *sql.Conn) error {
		if err := body(ctx, conn); err != nil {
			return err
		}
		return foreignKeyCheck(ctx, conn)
	})
}

func foreignKeyCheck(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var violations []string
	for rows.Next() {
		var table, parent string
		var rowid sql.NullInt64
		var index int
		if err := rows.Scan(&table, &rowid, &parent, &index); err != nil {
			return err
		}
		row := "a row without rowid"
		if rowid.Valid {
			row = fmt.Sprintf("row %d", rowid.Int64)
		}
		violations = append(violations, fmt.Sprintf("%s %s has no %s row", table, row, parent))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(violations) > 0 {
		return &ForeignKeyError{Violations: violations}
	}
	return nil
}
