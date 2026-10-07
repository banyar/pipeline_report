package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// States that pause a run on purpose while the retry worker backs off; they
// are "retrying", never "stuck".
const retryStates = `'CPE_FETCH_RETRY_SCHEDULED','KAFKA_PUBLISH_RETRY_SCHEDULED'`

// finishedStates are the terminal states reached after the API accepted the
// run - the denominator of the success rate. NORMALIZE_FAILED is legacy data.
const finishedStates = `'COMPLETED','SKIPPED','CONSUME_VALIDATION_FAILED','FAILED_PERMANENT','CPE_NOT_FOUND','RT_UPDATE_FAILED','NORMALIZE_FAILED'`

// classifiedTerminal is every terminal state some bucket counts. A finished
// run outside it is "unclassified" (Q4): a new state was added to rtdatacore
// without updating this report.
const classifiedTerminal = finishedStates + `,'NOT_ELIGIBLE','ALREADY_PROCESSED','API_REJECTED','API_UNREACHABLE'`

// Window is "today" plus the instant every count and age is measured at, so
// the summary and the live list of one refresh agree with each other.
type Window struct {
	Start, End, Now time.Time
	StuckBefore     time.Time
	StuckMinutes    int
	// NOCQueue is the queue name a successful run counts as "kept in NOC"
	// for, when both its before_queue and target_queue are it.
	NOCQueue string
}

func NewWindow(now time.Time, stuckMinutes int) Window {
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	return Window{
		Start:        start,
		End:          start.AddDate(0, 0, 1),
		Now:          now,
		StuckBefore:  now.Add(-time.Duration(stuckMinutes) * time.Minute),
		StuckMinutes: stuckMinutes,
	}
}

type Store struct {
	db *sql.DB
}

// querier is satisfied by *sql.DB and *sql.Tx.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Snapshot runs fn in one read-only REPEATABLE READ transaction, so the
// summary counts and the live rows of one response come from the same
// snapshot (a run finishing between the two queries cannot make them
// disagree), and the report can never write.
func (s *Store) Snapshot(ctx context.Context, fn func(q querier) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin read-only transaction: %w", err)
	}
	defer tx.Rollback()
	return fn(tx)
}

// Summary is Q1 of pipeline-daily-report-v2.md.
type Summary struct {
	Received          int `json:"received"`
	NotEligible       int `json:"not_eligible"`
	InProgress        int `json:"in_progress"`
	Retrying          int `json:"retrying"`
	Stuck             int `json:"stuck"`
	Success           int `json:"success"`
	RemoteResolved    int `json:"remote_resolved"`
	NotRemoteResolved int `json:"not_remote_resolved"`
	KeptInNOC         int `json:"kept_in_noc"`
	BCSOK             int `json:"bcs_ok"`
	BCSFailed         int `json:"bcs_failed"`
	Finished          int `json:"finished"`
	ValidationAPI400  int `json:"validation_api_400"`
	ValidationRTUtil  int `json:"validation_rtutil"`
	APIError409       int `json:"api_error_409"`
	APIErrorOther     int `json:"api_error_other"`
	APIUnreachable    int `json:"api_unreachable"`
	FailedPermanent   int `json:"failed_permanent"`
	CPENotFound       int `json:"cpe_not_found"`
	RTUpdateFailed    int `json:"rt_update_failed"`
	Skipped           int `json:"skipped"`
	Unclassified      int `json:"unclassified"`
}

// rejectedStatus joins each of today's API_REJECTED runs to the http_status
// of its api_rejected transition row. AppendEvent-style rows (to_state left
// at the run's current state) are excluded by the to_state filter.
const rejectedStatus = `
  LEFT JOIN (
    SELECT e.run_id, MAX(CAST(JSON_VALUE(e.detail, '$.http_status') AS UNSIGNED)) AS http_status
    FROM pipeline_runs p
    JOIN pipeline_run_events e
      ON e.run_id = p.run_id AND e.to_state = 'API_REJECTED' AND e.event = 'api_rejected'
    WHERE p.created_at >= ? AND p.created_at < ?
      AND p.current_state = 'API_REJECTED'
    GROUP BY e.run_id
  ) rej ON rej.run_id = pr.run_id`

const summaryQuery = `
SELECT
  COUNT(*),
  COALESCE(SUM(pr.current_state IN ('NOT_ELIGIBLE','ALREADY_PROCESSED')), 0),
  COALESCE(SUM(pr.completed_at IS NULL), 0),
  COALESCE(SUM(pr.completed_at IS NULL AND pr.current_state IN (` + retryStates + `)), 0),
  COALESCE(SUM(pr.completed_at IS NULL
           AND COALESCE(pr.current_state, '') NOT IN (` + retryStates + `)
           AND COALESCE(pr.updated_at, pr.created_at) < ?), 0),
  COALESCE(SUM(pr.current_state = 'COMPLETED'), 0),
  COALESCE(SUM(pr.current_state = 'COMPLETED' AND pr.is_remote_resolved = 1), 0),
  COALESCE(SUM(pr.current_state = 'COMPLETED' AND COALESCE(pr.is_remote_resolved, 0) = 0), 0),
  COALESCE(SUM(pr.current_state = 'COMPLETED' AND COALESCE(pr.is_remote_resolved, 0) = 0
           AND TRIM(pr.before_queue) = ? AND TRIM(pr.target_queue) = ?), 0),
  COALESCE(SUM(pr.current_state = 'COMPLETED' AND pr.is_remote_resolved = 1 AND pr.is_bcs_success = 1), 0),
  COALESCE(SUM(pr.current_state = 'COMPLETED' AND pr.is_remote_resolved = 1 AND pr.is_bcs_success = 0), 0),
  COALESCE(SUM(pr.current_state IN (` + finishedStates + `)), 0),
  COALESCE(SUM(pr.current_state = 'API_REJECTED' AND rej.http_status = 400), 0),
  COALESCE(SUM(pr.current_state = 'CONSUME_VALIDATION_FAILED'), 0),
  COALESCE(SUM(pr.current_state = 'API_REJECTED' AND rej.http_status = 409), 0),
  COALESCE(SUM(pr.current_state = 'API_REJECTED' AND COALESCE(rej.http_status, 0) NOT IN (400, 409)), 0),
  COALESCE(SUM(pr.current_state = 'API_UNREACHABLE'), 0),
  COALESCE(SUM(pr.current_state IN ('FAILED_PERMANENT','NORMALIZE_FAILED')), 0),
  COALESCE(SUM(pr.current_state = 'CPE_NOT_FOUND'), 0),
  COALESCE(SUM(pr.current_state = 'RT_UPDATE_FAILED'), 0),
  COALESCE(SUM(pr.current_state = 'SKIPPED'), 0),
  COALESCE(SUM(pr.completed_at IS NOT NULL
           AND COALESCE(pr.current_state, '') NOT IN (` + classifiedTerminal + `)), 0)
FROM pipeline_runs pr` + rejectedStatus + `
WHERE pr.created_at >= ? AND pr.created_at < ?`

func querySummary(ctx context.Context, q querier, w Window) (Summary, error) {
	var r Summary
	err := q.QueryRowContext(ctx, summaryQuery, w.StuckBefore, w.NOCQueue, w.NOCQueue, w.Start, w.End, w.Start, w.End).Scan(
		&r.Received, &r.NotEligible, &r.InProgress, &r.Retrying, &r.Stuck,
		&r.Success, &r.RemoteResolved, &r.NotRemoteResolved, &r.KeptInNOC, &r.BCSOK, &r.BCSFailed, &r.Finished,
		&r.ValidationAPI400, &r.ValidationRTUtil,
		&r.APIError409, &r.APIErrorOther, &r.APIUnreachable,
		&r.FailedPermanent, &r.CPENotFound, &r.RTUpdateFailed,
		&r.Skipped, &r.Unclassified,
	)
	if err != nil {
		return Summary{}, fmt.Errorf("summary query: %w", err)
	}
	return r, nil
}

// Filter narrows the live list to one of the "In progress now" tabs.
type Filter string

const (
	FilterAll      Filter = "all"
	FilterRetrying Filter = "retrying"
	FilterStuck    Filter = "stuck"
)

func ParseFilter(s string) (Filter, bool) {
	switch f := Filter(strings.ToLower(s)); f {
	case "", FilterAll:
		return FilterAll, true
	case FilterRetrying, FilterStuck:
		return f, true
	}
	return "", false
}

// LiveRun is one row of Q2.
type LiveRun struct {
	RunID        string
	TicketID     sql.NullInt64
	TicketNo     sql.NullString
	CpeID        sql.NullString
	Township     sql.NullString
	State        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	RetryAttempt sql.NullInt64 // attempt_count + 1: the attempt that runs next
	RetryMax     sql.NullInt64
	NextRetryAt  sql.NullTime
}

func queryInProgress(ctx context.Context, q querier, w Window, f Filter, limit int) ([]LiveRun, error) {
	where := `pr.created_at >= ? AND pr.created_at < ? AND pr.completed_at IS NULL`
	args := []any{w.Start, w.End}
	switch f {
	case FilterRetrying:
		where += ` AND pr.current_state IN (` + retryStates + `)`
	case FilterStuck:
		where += ` AND COALESCE(pr.current_state, '') NOT IN (` + retryStates + `) AND COALESCE(pr.updated_at, pr.created_at) < ?`
		args = append(args, w.StuckBefore)
	}
	query := `
SELECT pr.run_id, pr.ticket_id, pr.ticket_no, pr.cpe_id, pr.township, COALESCE(pr.current_state, ''),
       pr.created_at, COALESCE(pr.updated_at, pr.created_at),
       rj.attempt_count + 1, rj.max_attempts, rj.next_retry_at
FROM pipeline_runs pr
LEFT JOIN retry_jobs rj ON rj.id = pr.active_retry_job_id
WHERE ` + where + `
ORDER BY pr.created_at ASC
LIMIT ?`
	rows, err := q.QueryContext(ctx, query, append(args, limit)...)
	if err != nil {
		return nil, fmt.Errorf("in-progress query: %w", err)
	}
	defer rows.Close()

	runs := []LiveRun{}
	for rows.Next() {
		var r LiveRun
		if err := rows.Scan(&r.RunID, &r.TicketID, &r.TicketNo, &r.CpeID, &r.Township, &r.State,
			&r.CreatedAt, &r.UpdatedAt, &r.RetryAttempt, &r.RetryMax, &r.NextRetryAt); err != nil {
			return nil, fmt.Errorf("in-progress scan: %w", err)
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// ExportRun is one of today's runs, whatever its state, for the CSV export.
type ExportRun struct {
	RunID            string
	TicketID         sql.NullInt64
	TicketNo         sql.NullString
	CpeID            sql.NullString
	Township         sql.NullString
	State            string
	BeforeQueue      sql.NullString
	TargetQueue      sql.NullString
	HTTPStatus       sql.NullInt64
	IsRemoteResolved sql.NullBool
	IsBCSSuccess     sql.NullBool
	LastStateReason  sql.NullString
	CreatedAt        time.Time
	UpdatedAt        sql.NullTime
	CompletedAt      sql.NullTime
}

func queryTodayRuns(ctx context.Context, q querier, w Window) ([]ExportRun, error) {
	query := `
SELECT pr.run_id, pr.ticket_id, pr.ticket_no, pr.cpe_id, pr.township, COALESCE(pr.current_state, ''),
       pr.before_queue, pr.target_queue, rej.http_status, pr.is_remote_resolved, pr.is_bcs_success, pr.last_state_reason,
       pr.created_at, pr.updated_at, pr.completed_at
FROM pipeline_runs pr` + rejectedStatus + `
WHERE pr.created_at >= ? AND pr.created_at < ?
ORDER BY pr.created_at ASC`
	rows, err := q.QueryContext(ctx, query, w.Start, w.End, w.Start, w.End)
	if err != nil {
		return nil, fmt.Errorf("export query: %w", err)
	}
	defer rows.Close()

	var runs []ExportRun
	for rows.Next() {
		var r ExportRun
		if err := rows.Scan(&r.RunID, &r.TicketID, &r.TicketNo, &r.CpeID, &r.Township, &r.State,
			&r.BeforeQueue, &r.TargetQueue, &r.HTTPStatus, &r.IsRemoteResolved, &r.IsBCSSuccess, &r.LastStateReason,
			&r.CreatedAt, &r.UpdatedAt, &r.CompletedAt); err != nil {
			return nil, fmt.Errorf("export scan: %w", err)
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}
