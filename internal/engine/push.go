package engine

import (
	"github.com/orswn/a2a-hub/internal/pushguard"
)

// ValidatePushURL checks a push config URL at config time: http or https, and no IP literal in a denied range.
func ValidatePushURL(rawURL any, guard *pushguard.Guard) error {
	return catch(func() { validatePushURL(rawURL, guard) })
}

func (e *Engine) validatePushURL(rawURL string) { validatePushURL(rawURL, e.guard) }

func validatePushURL(rawURL any, guard *pushguard.Guard) {
	text, ok := rawURL.(string)
	if !ok || text == "" {
		throw(ReasonInvalid, "push config url is required")
	}
	u, err := pushguard.ParseURL(text)
	if err != nil {
		throw(ReasonInvalid, "push config url is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		throw(ReasonInvalid, "push config url must use http or https")
	}
	if refused := guard.CheckHost(u.Host); refused != "" {
		throw(ReasonInvalid, "%s", refused)
	}
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (e *Engine) savePushConfig(task *taskRow, caller Caller, config PushConfigInput) *PushConfig {
	e.validatePushURL(config.URL)
	id := config.ID
	if id == "" {
		id = e.NewID()
	}
	var owner string
	if e.one("SELECT principal FROM push_configs WHERE task_id = ? AND id = ?", args(task.ID, id), &owner) && owner != caller.Principal.Name {
		throw(ReasonInvalid, "push config id %s is in use on task %s", id, task.ID)
	}
	var scheme, credentials *string
	if config.Authentication != nil {
		scheme = nonEmpty(config.Authentication.Scheme)
		credentials = nonEmpty(config.Authentication.Credentials)
	}
	e.run(`INSERT INTO push_configs (task_id, id, url, token, auth_scheme, auth_credentials, principal, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT (task_id, id) DO UPDATE SET url = excluded.url, token = excluded.token,
			   auth_scheme = excluded.auth_scheme, auth_credentials = excluded.auth_credentials`,
		task.ID, id, config.URL, nonEmpty(config.Token), scheme, credentials, caller.Principal.Name, e.iso())
	return pushConfigWire(e.pushConfigRow("SELECT "+pushConfigCols+" FROM push_configs WHERE task_id = ? AND id = ?", task.ID, id))
}

func (e *Engine) pushConfigRow(query string, params ...any) *pushConfigRow {
	rows := e.pushConfigRows(query, params...)
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

func (e *Engine) pushConfigRows(query string, params ...any) []*pushConfigRow {
	var rows []*pushConfigRow
	e.each(query, params, func(scan func(...any)) {
		var row pushConfigRow
		scan(row.dest()...)
		rows = append(rows, &row)
	})
	return rows
}

func pushConfigWire(row *pushConfigRow) *PushConfig {
	if row == nil {
		throw(ReasonTaskNotFound, "push config not found")
	}
	config := &PushConfig{ID: row.ID, TaskID: row.TaskID, URL: row.URL, Token: deref(row.Token)}
	if row.AuthScheme != nil && *row.AuthScheme != "" {
		config.Authentication = &Authentication{Scheme: *row.AuthScheme, Credentials: deref(row.AuthCredentials)}
	}
	return config
}

// CreatePushConfig saves a push config on a task that the caller may see.
func (e *Engine) CreatePushConfig(agentID string, caller Caller, config PushConfigInput) (*PushConfig, error) {
	return call(e, func() *PushConfig {
		return txValue(e, func() *PushConfig {
			return e.savePushConfig(e.visibleTask(agentID, caller, config.TaskID), caller, config)
		})
	})
}

// GetPushConfig reads a push config. A push config belongs to the principal that made it: others cannot see,
// change, or delete it.
func (e *Engine) GetPushConfig(agentID string, caller Caller, taskID, id string) (*PushConfig, error) {
	return call(e, func() *PushConfig {
		e.visibleTask(agentID, caller, taskID)
		row := e.pushConfigRow("SELECT "+pushConfigCols+" FROM push_configs WHERE task_id = ? AND id = ? AND principal = ?",
			taskID, id, caller.Principal.Name)
		if row == nil {
			throw(ReasonTaskNotFound, "Push notification config %s not found for task %s", id, taskID)
		}
		return pushConfigWire(row)
	})
}

// ListPushConfigs lists the push configs of the caller on a task.
func (e *Engine) ListPushConfigs(agentID string, caller Caller, taskID string) ([]*PushConfig, error) {
	return call(e, func() []*PushConfig {
		e.visibleTask(agentID, caller, taskID)
		out := []*PushConfig{}
		for _, row := range e.pushConfigRows("SELECT "+pushConfigCols+" FROM push_configs WHERE task_id = ? AND principal = ? ORDER BY created_at, id",
			taskID, caller.Principal.Name) {
			out = append(out, pushConfigWire(row))
		}
		return out
	})
}

// DeletePushConfig deletes a push config of the caller. A missing config is no error.
func (e *Engine) DeletePushConfig(agentID string, caller Caller, taskID, id string) error {
	return e.do(func() {
		e.visibleTask(agentID, caller, taskID)
		e.tx(func() {
			var owner string
			if e.one("SELECT principal FROM push_configs WHERE task_id = ? AND id = ?", args(taskID, id), &owner) && owner != caller.Principal.Name {
				throw(ReasonTaskNotFound, "Push notification config %s not found for task %s", id, taskID)
			}
			e.run("DELETE FROM push_configs WHERE task_id = ? AND id = ?", taskID, id)
		})
	})
}

// OutboxItem is one queued push with its webhook.
type OutboxItem struct {
	ID              int64   `json:"id"`
	TaskID          string  `json:"taskId"`
	ConfigID        string  `json:"configId"`
	URL             string  `json:"url"`
	Token           *string `json:"token"`
	AuthScheme      *string `json:"authScheme"`
	AuthCredentials *string `json:"authCredentials"`
	Payload         string  `json:"payload"`
	Attempts        int64   `json:"attempts"`
	NextAt          int64   `json:"nextAt"`
}

// DuePushes gives the oldest due row of each config. Later rows of a config wait, so each webhook gets its
// events in order.
func (e *Engine) DuePushes(now int64, limit int) ([]OutboxItem, error) {
	return call(e, func() []OutboxItem {
		out := []OutboxItem{}
		e.each(`SELECT o.id, o.task_id, o.config_id, o.payload, o.attempts, o.next_at, c.url, c.token, c.auth_scheme, c.auth_credentials
			 FROM push_outbox o JOIN push_configs c ON c.task_id = o.task_id AND c.id = o.config_id
			 WHERE o.id IN (SELECT min(id) FROM push_outbox GROUP BY task_id, config_id) AND o.next_at <= ?
			 ORDER BY o.id LIMIT ?`, args(now, limit), func(scan func(...any)) {
			var item OutboxItem
			scan(&item.ID, &item.TaskID, &item.ConfigID, &item.Payload, &item.Attempts, &item.NextAt, &item.URL,
				&item.Token, &item.AuthScheme, &item.AuthCredentials)
			out = append(out, item)
		})
		return out
	})
}

// NextPushAt is the next_at of the next due push. false means the outbox is empty.
func (e *Engine) NextPushAt() (int64, bool, error) {
	var at *int64
	err := e.do(func() {
		e.one("SELECT min(next_at) AS at FROM push_outbox WHERE id IN (SELECT min(id) FROM push_outbox GROUP BY task_id, config_id)", nil, &at)
	})
	if err != nil || at == nil {
		return 0, false, err
	}
	return *at, true, nil
}

// PushDone removes a sent push.
func (e *Engine) PushDone(id int64) error {
	return e.do(func() { e.run("DELETE FROM push_outbox WHERE id = ?", id) })
}

// PushRetry stores the attempt count and the next try of a push.
func (e *Engine) PushRetry(id, attempts, nextAt int64) error {
	return e.do(func() { e.run("UPDATE push_outbox SET attempts = ?, next_at = ? WHERE id = ?", attempts, nextAt, id) })
}
