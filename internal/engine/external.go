package engine

// ExternalJob is one queued send or cancel for an external agent.
type ExternalJob struct {
	ID        int64   `json:"id"`
	AgentID   string  `json:"agentId"`
	TaskID    string  `json:"taskId"`
	ContextID string  `json:"contextId"`
	MessageID *string `json:"messageId"`
	Op        string  `json:"op"`
	Attempts  int64   `json:"attempts"`
	FirstAt   int64   `json:"firstAt"`
	NextAt    int64   `json:"nextAt"`
}

// RemoteTask is one dispatch of a hub task to an external agent.
type RemoteTask struct {
	TaskID          string  `json:"taskId"`
	Seq             int64   `json:"seq"`
	AgentID         string  `json:"agentId"`
	ContextID       string  `json:"contextId"`
	MessageID       *string `json:"messageId"`
	RemoteTaskID    *string `json:"remoteTaskId"`
	RemoteContextID *string `json:"remoteContextId"`
	// State is dispatched, running, or done.
	State        string `json:"state"`
	DispatchedAt int64  `json:"dispatchedAt"`
}

// OutboundMessage is a hub message in the form an external agent gets it.
type OutboundMessage struct {
	Text       string  `json:"text"`
	ContextID  string  `json:"contextId"`
	SenderName string  `json:"senderName"`
	SenderHost *string `json:"senderHost"`
}

func (e *Engine) queueExternal(task *taskRow, op string, messageID *string) {
	at := e.now()
	e.run(`INSERT INTO external_outbox (agent_id, task_id, context_id, message_id, op, attempts, first_at, next_at)
			 VALUES (?, ?, ?, ?, ?, 0, ?, ?)`, task.AgentID, task.ID, task.ContextID, messageID, op, at, at)
	e.afterCommit(func() { e.notify(&e.externalListeners) })
}

// ExternalJobs lists every queued job, oldest first. The gateway keeps one dispatch per context, so it needs
// the order.
func (e *Engine) ExternalJobs() ([]ExternalJob, error) {
	return call(e, func() []ExternalJob {
		jobs := []ExternalJob{}
		e.each("SELECT id, agent_id, task_id, context_id, message_id, op, attempts, first_at, next_at FROM external_outbox ORDER BY id",
			nil, func(scan func(...any)) {
				var job ExternalJob
				scan(&job.ID, &job.AgentID, &job.TaskID, &job.ContextID, &job.MessageID, &job.Op, &job.Attempts, &job.FirstAt, &job.NextAt)
				jobs = append(jobs, job)
			})
		return jobs
	})
}

// DropExternalJob removes a job.
func (e *Engine) DropExternalJob(id int64) error {
	return e.do(func() { e.run("DELETE FROM external_outbox WHERE id = ?", id) })
}

// StartRemote marks the job as sent before the request goes out, so a restart never sends it twice.
func (e *Engine) StartRemote(job ExternalJob) (*RemoteTask, error) {
	return call(e, func() *RemoteTask {
		return txValue(e, func() *RemoteTask {
			e.run("DELETE FROM external_outbox WHERE id = ?", job.ID)
			seq := e.count("SELECT max(seq) AS n FROM remote_tasks WHERE task_id = ?", job.TaskID) + 1
			at := e.now()
			e.run(`INSERT INTO remote_tasks (task_id, seq, agent_id, context_id, message_id, state, dispatched_at)
				 VALUES (?, ?, ?, ?, ?, 'dispatched', ?)`, job.TaskID, seq, job.AgentID, job.ContextID, job.MessageID, at)
			return &RemoteTask{
				TaskID: job.TaskID, Seq: seq, AgentID: job.AgentID, ContextID: job.ContextID, MessageID: job.MessageID,
				State: "dispatched", DispatchedAt: at,
			}
		})
	})
}

// RequeueRemote puts a job back after the agent did not take the request. It keeps its old id, so it keeps
// its place.
func (e *Engine) RequeueRemote(remote *RemoteTask, job ExternalJob) error {
	return e.do(func() {
		e.tx(func() {
			e.run("DELETE FROM remote_tasks WHERE task_id = ? AND seq = ?", remote.TaskID, remote.Seq)
			e.run(`INSERT INTO external_outbox (id, agent_id, task_id, context_id, message_id, op, attempts, first_at, next_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				job.ID, job.AgentID, job.TaskID, job.ContextID, job.MessageID, job.Op, job.Attempts, job.FirstAt, job.NextAt)
		})
	})
}

// RemoteStarted stores the ids that the external agent gave the task.
func (e *Engine) RemoteStarted(remote *RemoteTask, remoteTaskID string, remoteContextID *string) error {
	return e.do(func() {
		e.run("UPDATE remote_tasks SET remote_task_id = ?, remote_context_id = ?, state = 'running' WHERE task_id = ? AND seq = ?",
			remoteTaskID, remoteContextID, remote.TaskID, remote.Seq)
	})
}

// RemotePolled stores the time of the last poll.
func (e *Engine) RemotePolled(remote *RemoteTask) error {
	return e.do(func() {
		e.run("UPDATE remote_tasks SET last_poll_at = ? WHERE task_id = ? AND seq = ?", e.now(), remote.TaskID, remote.Seq)
	})
}

// RemoteDone marks a dispatch as finished.
func (e *Engine) RemoteDone(remote *RemoteTask) error {
	return e.do(func() {
		e.run("UPDATE remote_tasks SET state = 'done' WHERE task_id = ? AND seq = ?", remote.TaskID, remote.Seq)
	})
}

const remoteCols = "task_id, seq, agent_id, context_id, message_id, remote_task_id, remote_context_id, state, dispatched_at"

func (e *Engine) remotes(query string, params ...any) []*RemoteTask {
	out := []*RemoteTask{}
	e.each(query, params, func(scan func(...any)) {
		var r RemoteTask
		scan(&r.TaskID, &r.Seq, &r.AgentID, &r.ContextID, &r.MessageID, &r.RemoteTaskID, &r.RemoteContextID, &r.State, &r.DispatchedAt)
		out = append(out, &r)
	})
	return out
}

// ActiveRemotes lists the dispatches that have no final answer yet.
func (e *Engine) ActiveRemotes() ([]*RemoteTask, error) {
	return call(e, func() []*RemoteTask {
		return e.remotes("SELECT " + remoteCols + " FROM remote_tasks WHERE state != 'done' ORDER BY dispatched_at, task_id, seq")
	})
}

// LatestRemote gives the newest dispatch of a task. nil means none.
func (e *Engine) LatestRemote(taskID string) (*RemoteTask, error) {
	return call(e, func() *RemoteTask {
		if rows := e.remotes("SELECT "+remoteCols+" FROM remote_tasks WHERE task_id = ? ORDER BY seq DESC LIMIT 1", taskID); len(rows) > 0 {
			return rows[0]
		}
		return nil
	})
}

// RemoteBusy reports an open dispatch in the context.
func (e *Engine) RemoteBusy(agentID, contextID string) (bool, error) {
	return call(e, func() bool {
		return e.count("SELECT count(*) AS n FROM remote_tasks WHERE agent_id = ? AND context_id = ? AND state != 'done'", agentID, contextID) != 0
	})
}

// RemoteKnown reports whether a remote task id belongs to a dispatch of the agent.
func (e *Engine) RemoteKnown(agentID, remoteTaskID string) (bool, error) {
	return call(e, func() bool {
		return e.count("SELECT count(*) AS n FROM remote_tasks WHERE agent_id = ? AND remote_task_id = ?", agentID, remoteTaskID) != 0
	})
}

// OutboundMessage gives the text and sender of one task message. nil means the task or the message is gone.
func (e *Engine) OutboundMessage(taskID, messageID string) (*OutboundMessage, error) {
	return call(e, func() *OutboundMessage {
		var message string
		out := &OutboundMessage{}
		if !e.one(`SELECT m.message, m.context_id, m.sender_name, a.host FROM messages m LEFT JOIN agents a ON a.id = m.sender_agent
			 WHERE m.task_id = ? AND m.id = ? ORDER BY m.seq DESC LIMIT 1`, args(taskID, messageID),
			&message, &out.ContextID, &out.SenderName, &out.SenderHost) {
			return nil
		}
		out.Text = MessageText(mustObject(message))
		return out
	})
}
