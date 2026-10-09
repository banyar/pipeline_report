package main

import "time"

// Wire formats follow section 10 of pipeline-daily-report-v2.md: timestamps
// are server-local wall-clock times without a zone, like the DB stores them.
const wireTime = "2006-01-02T15:04:05"

// stageIndex is the 8-dot stepper position: Submitted, Received, Fetch,
// Workflow, Published, Consumed, BCS, RT. A state missing here has no stepper.
var stageIndex = map[string]int{
	"":                              0,
	"SUBMITTED_TO_API":              0,
	"RECEIVED":                      1,
	"FETCHING_CPE_STATUS":           2,
	"CPE_FETCH_RETRY_SCHEDULED":     2,
	"WORKFLOW_EVALUATED":            3,
	"PUBLISHED":                     4,
	"KAFKA_PUBLISH_RETRY_SCHEDULED": 4,
	"CONSUMED":                      5,
	"BCS_UPDATING":                  6,
	"RT_UPDATING":                   7,
}

// owner is the service that last wrote the state.
func owner(state string) string {
	switch state {
	case "", "SUBMITTED_TO_API":
		return "rt_web_ui"
	case "CPE_FETCH_RETRY_SCHEDULED", "KAFKA_PUBLISH_RETRY_SCHEDULED":
		return "retry_worker"
	case "CONSUMED", "BCS_UPDATING", "RT_UPDATING":
		return "rtutil"
	}
	return "noc_automation"
}

func isRetryState(state string) bool {
	return state == "CPE_FETCH_RETRY_SCHEDULED" || state == "KAFKA_PUBLISH_RETRY_SCHEDULED"
}

// bucket maps a run to the single report bucket it is counted in (section 5).
func bucket(state string, completed bool, httpStatus int64) string {
	if !completed {
		return "in_progress"
	}
	switch state {
	case "COMPLETED":
		return "success"
	case "NOT_ELIGIBLE", "ALREADY_PROCESSED":
		return "not_eligible"
	case "CONSUME_VALIDATION_FAILED":
		return "validation_failed"
	case "API_REJECTED":
		if httpStatus == 400 {
			return "validation_failed"
		}
		return "api_error"
	case "API_UNREACHABLE":
		return "api_error"
	case "FAILED_PERMANENT", "NORMALIZE_FAILED", "CPE_NOT_FOUND", "RT_UPDATE_FAILED":
		return "failed"
	case "SKIPPED":
		return "skipped"
	}
	return "unclassified"
}

// card is the report card a bucket is shown under: validation failed,
// API error and failed are grouped as "manual_check".
func card(bucket string) string {
	switch bucket {
	case "validation_failed", "api_error", "failed":
		return "manual_check"
	}
	return bucket
}

type windowJSON struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type SummaryResponse struct {
	Date         string     `json:"date"`
	Window       windowJSON `json:"window"`
	GeneratedAt  string     `json:"generated_at"`
	StuckMinutes int        `json:"stuck_minutes"`
	Received     int        `json:"received"`
	InProgress   struct {
		Total    int `json:"total"`
		Retrying int `json:"retrying"`
		Stuck    int `json:"stuck"`
	} `json:"in_progress"`
	Success struct {
		Total          int `json:"total"`
		Finished       int `json:"finished"`
		RemoteResolved int `json:"remote_resolved"`
		Transferred    int `json:"transferred"`
		KeptInNOC      int `json:"kept_in_noc"`
		BCSOK          int `json:"bcs_ok"`
		BCSFailed      int `json:"bcs_failed"`
	} `json:"success"`
	NotEligible      int `json:"not_eligible"`
	ValidationFailed struct {
		Total  int `json:"total"`
		API400 int `json:"api_400"`
		RTUtil int `json:"rtutil"`
	} `json:"validation_failed"`
	APIError struct {
		Total       int `json:"total"`
		HTTP409     int `json:"http_409"`
		Other       int `json:"other"`
		Unreachable int `json:"unreachable"`
	} `json:"api_error"`
	Failed struct {
		Total           int `json:"total"`
		FailedPermanent int `json:"failed_permanent"`
		CPENotFound     int `json:"cpe_not_found"`
		RTUpdateFailed  int `json:"rt_update_failed"`
	} `json:"failed"`
	// ManualCheck is validation failed + API error + failed: the runs that
	// ended without automation finishing them and need a person to look.
	ManualCheck  int `json:"manual_check"`
	Skipped      int `json:"skipped"`
	Unclassified int `json:"unclassified"`
}

func newSummaryResponse(w Window, s Summary) SummaryResponse {
	r := SummaryResponse{
		Date:         w.Start.Format("2006-01-02"),
		Window:       windowJSON{From: w.Start.Format(wireTime), To: w.Now.Format(wireTime)},
		GeneratedAt:  w.Now.Format(wireTime),
		StuckMinutes: w.StuckMinutes,
		Received:     s.Received,
		NotEligible:  s.NotEligible,
		Skipped:      s.Skipped,
		Unclassified: s.Unclassified,
	}
	r.InProgress.Total, r.InProgress.Retrying, r.InProgress.Stuck = s.InProgress, s.Retrying, s.Stuck

	r.Success.Total, r.Success.Finished = s.Success, s.Finished
	// Not remote resolved = moved to another queue, or left in the NOC queue
	// (before_queue = target_queue = NOC) for NOC to follow up.
	r.Success.RemoteResolved, r.Success.KeptInNOC = s.RemoteResolved, s.KeptInNOC
	r.Success.Transferred = s.NotRemoteResolved - s.KeptInNOC
	r.Success.BCSOK, r.Success.BCSFailed = s.BCSOK, s.BCSFailed

	r.ValidationFailed.API400, r.ValidationFailed.RTUtil = s.ValidationAPI400, s.ValidationRTUtil
	r.ValidationFailed.Total = s.ValidationAPI400 + s.ValidationRTUtil

	r.APIError.HTTP409, r.APIError.Other, r.APIError.Unreachable = s.APIError409, s.APIErrorOther, s.APIUnreachable
	r.APIError.Total = s.APIError409 + s.APIErrorOther + s.APIUnreachable

	r.Failed.FailedPermanent, r.Failed.CPENotFound, r.Failed.RTUpdateFailed = s.FailedPermanent, s.CPENotFound, s.RTUpdateFailed
	r.Failed.Total = s.FailedPermanent + s.CPENotFound + s.RTUpdateFailed

	r.ManualCheck = r.ValidationFailed.Total + r.APIError.Total + r.Failed.Total
	return r
}

type retryJSON struct {
	NextAttempt int64   `json:"next_attempt"`
	MaxAttempts int64   `json:"max_attempts"`
	NextRetryAt *string `json:"next_retry_at"`
}

type LiveItem struct {
	RunID        string     `json:"run_id"`
	TicketID     *int64     `json:"ticket_id"`
	TicketNo     string     `json:"ticket_no"`
	CpeID        string     `json:"cpe_id"`
	Township     string     `json:"township"`
	CurrentState string     `json:"current_state"`
	StageIndex   *int       `json:"stage_index"`
	Owner        string     `json:"owner"`
	IsRetrying   bool       `json:"is_retrying"`
	IsStuck      bool       `json:"is_stuck"`
	Retry        *retryJSON `json:"retry"`
	AgeSec       int64      `json:"age_sec"`
	IdleSec      int64      `json:"idle_sec"`
	StartedAt    string     `json:"started_at"`
}

type InProgressResponse struct {
	GeneratedAt string     `json:"generated_at"`
	Filter      Filter     `json:"filter"`
	Total       int        `json:"total"`
	Limit       int        `json:"limit"`
	Items       []LiveItem `json:"items"`
}

func newLiveItem(w Window, r LiveRun) LiveItem {
	retrying := isRetryState(r.State)
	it := LiveItem{
		RunID:        r.RunID,
		TicketNo:     r.TicketNo.String,
		CpeID:        r.CpeID.String,
		Township:     r.Township.String,
		CurrentState: r.State,
		Owner:        owner(r.State),
		IsRetrying:   retrying,
		IsStuck:      !retrying && r.UpdatedAt.Before(w.StuckBefore),
		AgeSec:       seconds(w.Now.Sub(r.CreatedAt)),
		IdleSec:      seconds(w.Now.Sub(r.UpdatedAt)),
		StartedAt:    r.CreatedAt.Format(wireTime),
	}
	if r.TicketID.Valid {
		it.TicketID = &r.TicketID.Int64
	}
	if idx, ok := stageIndex[r.State]; ok {
		it.StageIndex = &idx
	}
	if retrying && r.RetryMax.Valid {
		rj := &retryJSON{NextAttempt: r.RetryAttempt.Int64, MaxAttempts: r.RetryMax.Int64}
		if r.NextRetryAt.Valid {
			s := r.NextRetryAt.Time.Format(wireTime)
			rj.NextRetryAt = &s
		}
		it.Retry = rj
	}
	return it
}

// RunItem is one row of a case list: where the run ended up and why.
type RunItem struct {
	RunID            string  `json:"run_id"`
	TicketID         *int64  `json:"ticket_id"`
	TicketNo         string  `json:"ticket_no"`
	CpeID            string  `json:"cpe_id"`
	Township         string  `json:"township"`
	CurrentState     string  `json:"current_state"`
	Bucket           string  `json:"bucket"`
	BeforeQueue      string  `json:"before_queue"`
	TargetQueue      string  `json:"target_queue"`
	HTTPStatus       *int64  `json:"http_status"`
	IsRemoteResolved *bool   `json:"is_remote_resolved"`
	IsBCSSuccess     *bool   `json:"is_bcs_success"`
	BCSStatusMessage string  `json:"bcs_status_message"` // CPEMS/BCS error when is_bcs_success is false
	Reason           string  `json:"reason"`
	StartedAt        string  `json:"started_at"`
	CompletedAt      *string `json:"completed_at"`
	DurationSec      *int64  `json:"duration_sec"`
}

type CaseResponse struct {
	GeneratedAt string    `json:"generated_at"`
	Case        Case      `json:"case"`
	Total       int       `json:"total"`
	Limit       int       `json:"limit"`
	Items       []RunItem `json:"items"`
}

func newRunItem(r ExportRun) RunItem {
	it := RunItem{
		RunID:            r.RunID,
		TicketNo:         r.TicketNo.String,
		CpeID:            r.CpeID.String,
		Township:         r.Township.String,
		CurrentState:     r.State,
		Bucket:           bucket(r.State, r.CompletedAt.Valid, r.HTTPStatus.Int64),
		BeforeQueue:      r.BeforeQueue.String,
		TargetQueue:      r.TargetQueue.String,
		BCSStatusMessage: r.BCSStatusMessage.String,
		Reason:           r.LastStateReason.String,
		StartedAt:        r.CreatedAt.Format(wireTime),
	}
	if r.TicketID.Valid {
		it.TicketID = &r.TicketID.Int64
	}
	if r.HTTPStatus.Valid {
		it.HTTPStatus = &r.HTTPStatus.Int64
	}
	if r.IsRemoteResolved.Valid {
		it.IsRemoteResolved = &r.IsRemoteResolved.Bool
	}
	if r.IsBCSSuccess.Valid {
		it.IsBCSSuccess = &r.IsBCSSuccess.Bool
	}
	if r.CompletedAt.Valid {
		s := r.CompletedAt.Time.Format(wireTime)
		d := seconds(r.CompletedAt.Time.Sub(r.CreatedAt))
		it.CompletedAt, it.DurationSec = &s, &d
	}
	return it
}

// caseTotal is the card number a case list opens from.
func caseTotal(r SummaryResponse, c Case) int {
	switch c {
	case CaseReceived:
		return r.Received
	case CaseSuccess:
		return r.Success.Total
	case CaseRemoteResolved:
		return r.Success.RemoteResolved
	case CaseTransferred:
		return r.Success.Transferred
	case CaseKeptInNOC:
		return r.Success.KeptInNOC
	case CaseBCSOK:
		return r.Success.BCSOK
	case CaseBCSFailed:
		return r.Success.BCSFailed
	case CaseNotEligible:
		return r.NotEligible
	case CaseManualCheck:
		return r.ManualCheck
	}
	return r.InProgress.Total
}

func seconds(d time.Duration) int64 {
	if d < 0 {
		return 0
	}
	return int64(d / time.Second)
}

func filterTotal(s Summary, f Filter) int {
	switch f {
	case FilterRetrying:
		return s.Retrying
	case FilterStuck:
		return s.Stuck
	}
	return s.InProgress
}
