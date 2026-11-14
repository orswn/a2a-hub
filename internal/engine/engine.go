// Package engine holds the hub state: agents, tasks, messages, the link mailbox, push configs, and the outboxes.
//
// The engine owns one sqlite connection. One mutex serializes every method that reads or writes it. Listeners
// run after the commit, outside the lock, in commit order. Each event goes to the listeners that were
// registered when its change was committed, as in the TypeScript hub, where listeners run inside the call.
package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/orswn/a2a-hub/internal/pushguard"
)

// Options injects the clock, the id generator, the push guard, and the log.
type Options struct {
	// Now gives the time in milliseconds since the epoch.
	Now func() int64
	// NewID makes an id at the given time.
	NewID     func(now int64) string
	PushGuard *pushguard.Guard
	Log       func(line string)
}

// Engine is the hub state on one sqlite connection.
type Engine struct {
	mu    sync.Mutex
	conn  *sql.Conn
	stmts map[string]*sql.Stmt
	now   func() int64
	newID func(int64) string
	guard *pushguard.Guard
	log   func(string)
	inTx  bool
	// staged holds the notices of the open transaction. The commit turns each into a run with the listeners
	// of that moment.
	staged  []func() func()
	pending []func()

	lmu               sync.Mutex
	taskListeners     map[string]*listenerSet[func(TaskEvent)]
	linkListeners     listenerSet[func(string, LinkEvent)]
	ackListeners      listenerSet[func(string, int64)]
	pushListeners     listenerSet[func()]
	externalListeners listenerSet[func()]

	qmu      sync.Mutex
	queue    []func()
	draining bool
}

// New makes an engine on its own connection of db. Close releases the connection.
func New(ctx context.Context, db *sql.DB, options Options) (*Engine, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		conn:          conn,
		stmts:         map[string]*sql.Stmt{},
		now:           options.Now,
		newID:         options.NewID,
		guard:         options.PushGuard,
		log:           options.Log,
		taskListeners: map[string]*listenerSet[func(TaskEvent)]{},
	}
	if e.now == nil {
		e.now = func() int64 { return time.Now().UnixMilli() }
	}
	if e.newID == nil {
		e.newID = UUIDv7
	}
	if e.guard == nil {
		e.guard, _ = pushguard.New(nil, nil)
	}
	if e.log == nil {
		e.log = func(string) {}
	}
	return e, nil
}

// Close releases the connection.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, stmt := range e.stmts {
		_ = stmt.Close()
	}
	e.stmts = map[string]*sql.Stmt{}
	return e.conn.Close()
}

// Now is the engine clock in milliseconds.
func (e *Engine) Now() int64 { return e.now() }

// NewID makes an id with the engine clock and id generator.
func (e *Engine) NewID() string { return e.newID(e.now()) }

func (e *Engine) iso() string { return ISO(e.now()) }

// ---------- calls, transactions, and listeners ----------

// dbError carries a database failure through a panic to the call boundary.
type dbError struct{ err error }

// do runs one engine call under the lock. A *Error or a database failure inside fn comes back as the error.
// The listeners that the call queued run after the unlock.
func (e *Engine) do(fn func()) (err error) {
	e.mu.Lock()
	e.pending = nil
	e.staged = nil
	var crash any
	func() {
		defer func() {
			r := recover()
			switch v := r.(type) {
			case nil:
			case *Error:
				err = v
			case dbError:
				err = v.err
			default:
				crash = r
			}
		}()
		fn()
	}()
	pending := e.pending
	e.pending = nil
	e.qmu.Lock()
	e.queue = append(e.queue, pending...)
	e.qmu.Unlock()
	e.mu.Unlock()
	e.drain()
	if crash != nil {
		panic(crash)
	}
	return err
}

// catch runs fn and turns a thrown *Error into the result.
func catch(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			hubErr, ok := r.(*Error)
			if !ok {
				panic(r)
			}
			err = hubErr
		}
	}()
	fn()
	return nil
}

func call[T any](e *Engine, fn func() T) (T, error) {
	var out T
	err := e.do(func() { out = fn() })
	return out, err
}

// tx runs fn in one transaction. Nested calls join the outer one. The listeners that fn queued are dropped
// on a rollback.
func (e *Engine) tx(fn func()) {
	if e.inTx {
		fn()
		return
	}
	e.exec("BEGIN IMMEDIATE")
	e.inTx = true
	committed := false
	defer func() {
		e.inTx = false
		staged := e.staged
		e.staged = nil
		if !committed {
			_, _ = e.conn.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		for _, notice := range staged {
			e.pending = append(e.pending, notice())
		}
	}()
	fn()
	e.exec("COMMIT")
	committed = true
}

func txValue[T any](e *Engine, fn func() T) T {
	var out T
	e.tx(func() { out = fn() })
	return out
}

// afterCommit queues a notice. A notice reads its listeners at the commit and returns the run that calls
// them after the unlock. Outside a transaction the statement is already committed.
func (e *Engine) afterCommit(notice func() func()) {
	if e.inTx {
		e.staged = append(e.staged, notice)
		return
	}
	e.pending = append(e.pending, notice())
}

// drain runs the queued listeners in commit order. A listener that calls the engine queues its own
// listeners behind the current ones, and the goroutine that drains runs them too.
func (e *Engine) drain() {
	e.qmu.Lock()
	if e.draining {
		e.qmu.Unlock()
		return
	}
	e.draining = true
	for len(e.queue) > 0 {
		next := e.queue[0]
		e.queue[0] = nil
		e.queue = e.queue[1:]
		e.qmu.Unlock()
		e.safely(next)
		e.qmu.Lock()
	}
	e.queue = nil
	e.draining = false
	e.qmu.Unlock()
}

// safely runs a listener. The change is committed, so a broken listener must not fail the caller or skip
// the other listeners.
func (e *Engine) safely(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			e.log(fmt.Sprintf("listener failed: %v\n%s", r, debug.Stack()))
		}
	}()
	fn()
}

type listenerSet[T any] struct {
	next  int
	items []listenerItem[T]
}

type listenerItem[T any] struct {
	id int
	fn T
}

func (s *listenerSet[T]) add(fn T) int {
	s.next++
	s.items = append(s.items, listenerItem[T]{id: s.next, fn: fn})
	return s.next
}

func (s *listenerSet[T]) remove(id int) {
	for i, item := range s.items {
		if item.id == id {
			s.items = append(s.items[:i:i], s.items[i+1:]...)
			return
		}
	}
}

func (s *listenerSet[T]) snapshot() []T {
	out := make([]T, len(s.items))
	for i, item := range s.items {
		out[i] = item.fn
	}
	return out
}

func (e *Engine) listeners(fn func()) {
	e.lmu.Lock()
	defer e.lmu.Unlock()
	fn()
}

// ListenerCount is the number of open task listeners. Tests use it to prove that waits clean up.
func (e *Engine) ListenerCount(taskID string) int {
	e.lmu.Lock()
	defer e.lmu.Unlock()
	if set := e.taskListeners[taskID]; set != nil {
		return len(set.items)
	}
	return 0
}

// OnTask calls listener after each committed change of the task. The result removes the listener.
//
// Listener rules, for every On method: a listener must not block and must not call a blocking engine wait,
// such as WaitForTask or WaitForAck, because one goroutine runs the listeners of all callers in turn. It may
// run after the call that made the change returned, and after the function that removes it returned.
func (e *Engine) OnTask(taskID string, listener func(TaskEvent)) func() {
	return e.addTaskListener(taskID, listener)
}

func (e *Engine) addTaskListener(taskID string, listener func(TaskEvent)) func() {
	var id int
	e.listeners(func() {
		set := e.taskListeners[taskID]
		if set == nil {
			set = &listenerSet[func(TaskEvent)]{}
			e.taskListeners[taskID] = set
		}
		id = set.add(listener)
	})
	return func() {
		e.listeners(func() {
			if set := e.taskListeners[taskID]; set != nil {
				set.remove(id)
				if len(set.items) == 0 {
					delete(e.taskListeners, taskID)
				}
			}
		})
	}
}

// OnLink calls listener with each committed link event. The rules of OnTask apply.
func (e *Engine) OnLink(listener func(agentID string, event LinkEvent)) func() {
	var id int
	e.listeners(func() { id = e.linkListeners.add(listener) })
	return func() { e.listeners(func() { e.linkListeners.remove(id) }) }
}

// OnAck calls listener after each ack. The rules of OnTask apply.
func (e *Engine) OnAck(listener func(agentID string, upTo int64)) func() {
	var id int
	e.listeners(func() { id = e.ackListeners.add(listener) })
	return func() { e.listeners(func() { e.ackListeners.remove(id) }) }
}

// OnPush calls listener after a commit that queued a push. The rules of OnTask apply.
func (e *Engine) OnPush(listener func()) func() {
	var id int
	e.listeners(func() { id = e.pushListeners.add(listener) })
	return func() { e.listeners(func() { e.pushListeners.remove(id) }) }
}

// OnExternal calls listener after a commit that queued work for an external agent. The rules of OnTask apply.
func (e *Engine) OnExternal(listener func()) func() {
	var id int
	e.listeners(func() { id = e.externalListeners.add(listener) })
	return func() { e.listeners(func() { e.externalListeners.remove(id) }) }
}

func (e *Engine) taskNotice(event TaskEvent) func() {
	var fns []func(TaskEvent)
	e.listeners(func() {
		if set := e.taskListeners[event.TaskID]; set != nil {
			fns = set.snapshot()
		}
	})
	return func() {
		for _, fn := range fns {
			e.safely(func() { fn(event) })
		}
	}
}

func (e *Engine) linkNotice(agentID string, event LinkEvent) func() {
	var fns []func(string, LinkEvent)
	e.listeners(func() { fns = e.linkListeners.snapshot() })
	return func() {
		for _, fn := range fns {
			e.safely(func() { fn(agentID, event) })
		}
	}
}

func (e *Engine) ackNotice(agentID string, upTo int64) func() {
	var fns []func(string, int64)
	e.listeners(func() { fns = e.ackListeners.snapshot() })
	return func() {
		for _, fn := range fns {
			e.safely(func() { fn(agentID, upTo) })
		}
	}
}

func (e *Engine) notice(set *listenerSet[func()]) func() {
	var fns []func()
	e.listeners(func() { fns = set.snapshot() })
	return func() {
		for _, fn := range fns {
			e.safely(fn)
		}
	}
}

// ---------- sql ----------

func (e *Engine) stmt(query string) *sql.Stmt {
	if stmt, ok := e.stmts[query]; ok {
		return stmt
	}
	stmt, err := e.conn.PrepareContext(context.Background(), query)
	if err != nil {
		panic(dbError{fmt.Errorf("prepare %q: %w", query, err)})
	}
	e.stmts[query] = stmt
	return stmt
}

func (e *Engine) exec(query string) {
	if _, err := e.conn.ExecContext(context.Background(), query); err != nil {
		panic(dbError{err})
	}
}

// run runs a write and returns the changed row count and the last insert rowid.
func (e *Engine) run(query string, args ...any) (int64, int64) {
	result, err := e.stmt(query).ExecContext(context.Background(), args...)
	if err != nil {
		panic(dbError{err})
	}
	changes, _ := result.RowsAffected()
	last, _ := result.LastInsertId()
	return changes, last
}

// each calls fn for each row with a scan function.
func (e *Engine) each(query string, args []any, fn func(scan func(dest ...any))) {
	rows, err := e.stmt(query).QueryContext(context.Background(), args...)
	if err != nil {
		panic(dbError{err})
	}
	defer func() { _ = rows.Close() }()
	scan := func(dest ...any) {
		if err := rows.Scan(dest...); err != nil {
			panic(dbError{err})
		}
	}
	for rows.Next() {
		fn(scan)
	}
	if err := rows.Err(); err != nil {
		panic(dbError{err})
	}
}

// one scans the first row into dest. false means no row.
func (e *Engine) one(query string, args []any, dest ...any) bool {
	err := e.stmt(query).QueryRowContext(context.Background(), args...).Scan(dest...)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		panic(dbError{err})
	}
	return true
}

// count reads one integer, such as count(*) or max(seq). NULL gives 0.
func (e *Engine) count(query string, args ...any) int64 {
	var n sql.NullInt64
	e.one(query, args, &n)
	return n.Int64
}

func args(values ...any) []any { return values }
