package main

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestNewWindow(t *testing.T) {
	loc := time.FixedZone("MMT", 6*3600+1800)
	now := time.Date(2026, 10, 4, 0, 20, 0, 0, loc)
	w := NewWindow(now, 10)
	if want := time.Date(2026, 10, 4, 0, 0, 0, 0, loc); !w.Start.Equal(want) {
		t.Fatalf("start = %v, want %v", w.Start, want)
	}
	if want := time.Date(2026, 10, 5, 0, 0, 0, 0, loc); !w.End.Equal(want) {
		t.Fatalf("end = %v, want %v", w.End, want)
	}
	if want := now.Add(-10 * time.Minute); !w.StuckBefore.Equal(want) {
		t.Fatalf("stuck before = %v, want %v", w.StuckBefore, want)
	}
}

func TestBucket(t *testing.T) {
	cases := []struct {
		state     string
		completed bool
		http      int64
		want      string
	}{
		{"PUBLISHED", false, 0, "in_progress"},
		{"", false, 0, "in_progress"},
		{"COMPLETED", true, 0, "success"},
		{"NOT_ELIGIBLE", true, 0, "not_eligible"},
		{"API_REJECTED", true, 400, "validation_failed"},
		{"CONSUME_VALIDATION_FAILED", true, 0, "validation_failed"},
		{"API_REJECTED", true, 409, "api_error"},
		{"API_REJECTED", true, 0, "api_error"},
		{"API_UNREACHABLE", true, 0, "api_error"},
		{"NORMALIZE_FAILED", true, 0, "failed"},
		{"RT_UPDATE_FAILED", true, 0, "failed"},
		{"SKIPPED", true, 0, "skipped"},
		{"SOMETHING_NEW", true, 0, "unclassified"},
	}
	for _, c := range cases {
		if got := bucket(c.state, c.completed, c.http); got != c.want {
			t.Errorf("bucket(%q, %v, %d) = %q, want %q", c.state, c.completed, c.http, got, c.want)
		}
	}
}

func TestNewLiveItem(t *testing.T) {
	now := time.Date(2026, 10, 4, 14, 35, 12, 0, time.Local)
	w := NewWindow(now, 10)

	retry := newLiveItem(w, LiveRun{
		RunID: "r1", State: "CPE_FETCH_RETRY_SCHEDULED",
		CreatedAt: now.Add(-375 * time.Second), UpdatedAt: now.Add(-12 * time.Minute),
		RetryAttempt: sql.NullInt64{Int64: 2, Valid: true}, RetryMax: sql.NullInt64{Int64: 3, Valid: true},
		NextRetryAt: sql.NullTime{Time: now.Add(3 * time.Minute), Valid: true},
	})
	if !retry.IsRetrying || retry.IsStuck {
		t.Fatalf("retrying run: retrying=%v stuck=%v, want true/false", retry.IsRetrying, retry.IsStuck)
	}
	if retry.Owner != "retry_worker" || *retry.StageIndex != 2 || retry.AgeSec != 375 {
		t.Fatalf("retrying run: owner=%s stage=%d age=%d", retry.Owner, *retry.StageIndex, retry.AgeSec)
	}
	if retry.Retry == nil || retry.Retry.NextAttempt != 2 || retry.Retry.MaxAttempts != 3 || *retry.Retry.NextRetryAt != "2026-10-04T14:38:12" {
		t.Fatalf("retry = %+v", retry.Retry)
	}

	stuck := newLiveItem(w, LiveRun{RunID: "r2", State: "PUBLISHED",
		CreatedAt: now.Add(-15 * time.Minute), UpdatedAt: now.Add(-15 * time.Minute)})
	if !stuck.IsStuck || stuck.Retry != nil || stuck.Owner != "noc_automation" {
		t.Fatalf("stuck run: %+v", stuck)
	}

	unknown := newLiveItem(w, LiveRun{RunID: "r3", State: "NEW_STATE", CreatedAt: now, UpdatedAt: now})
	if unknown.StageIndex != nil || unknown.IsStuck {
		t.Fatalf("unknown state: %+v", unknown)
	}
}

func TestParseFilter(t *testing.T) {
	for in, want := range map[string]Filter{"": FilterAll, "all": FilterAll, "Retrying": FilterRetrying, "stuck": FilterStuck} {
		if got, ok := ParseFilter(in); !ok || got != want {
			t.Errorf("ParseFilter(%q) = %q, %v", in, got, ok)
		}
	}
	if _, ok := ParseFilter("done"); ok {
		t.Error("ParseFilter(done) accepted")
	}
}

func TestNewSummaryResponseManualCheck(t *testing.T) {
	w := NewWindow(time.Date(2026, 10, 6, 14, 35, 0, 0, time.Local), 10)
	r := newSummaryResponse(w, Summary{
		Received: 312, ValidationAPI400: 6, ValidationRTUtil: 3,
		APIError409: 2, APIErrorOther: 3, APIUnreachable: 3,
		FailedPermanent: 9, CPENotFound: 7, RTUpdateFailed: 5,
	})
	if r.ValidationFailed.Total != 9 || r.APIError.Total != 8 || r.Failed.Total != 21 || r.ManualCheck != 38 {
		t.Fatalf("validation=%d api=%d failed=%d manual=%d, want 9/8/21/38",
			r.ValidationFailed.Total, r.APIError.Total, r.Failed.Total, r.ManualCheck)
	}
	for b, want := range map[string]string{"validation_failed": "manual_check", "api_error": "manual_check",
		"failed": "manual_check", "success": "success", "not_eligible": "not_eligible"} {
		if got := card(b); got != want {
			t.Errorf("card(%q) = %q, want %q", b, got, want)
		}
	}
}

func TestNewSummaryResponseSuccessSplit(t *testing.T) {
	w := NewWindow(time.Date(2026, 10, 6, 14, 35, 0, 0, time.Local), 10)
	r := newSummaryResponse(w, Summary{Success: 192, RemoteResolved: 136, NotRemoteResolved: 56, KeptInNOC: 22})
	if r.Success.RemoteResolved != 136 || r.Success.Transferred != 34 || r.Success.KeptInNOC != 22 {
		t.Fatalf("success split = %+v, want remote 136 / transferred 34 / kept 22", r.Success)
	}
}

func TestParseCase(t *testing.T) {
	for in, want := range map[string]Case{"": CaseInProgress, "in_progress": CaseInProgress,
		"Manual_Check": CaseManualCheck, "transferred": CaseTransferred, "received": CaseReceived} {
		if got, ok := ParseCase(in); !ok || got != want {
			t.Errorf("ParseCase(%q) = %q, %v", in, got, ok)
		}
	}
	for _, in := range []string{"skipped", "unclassified", "done"} {
		if _, ok := ParseCase(in); ok {
			t.Errorf("ParseCase(%q) accepted", in)
		}
	}
}

func TestCaseConditions(t *testing.T) {
	for c, cond := range caseCond {
		if got, want := strings.Count(cond.sql, "?"), map[bool]int{false: 0, true: 2}[cond.inNOC]; got != want {
			t.Errorf("case %s: %d placeholders, inNOC=%v wants %d", c, got, cond.inNOC, want)
		}
	}
	// Manual check is every state the validation, API error and failed buckets count.
	for _, state := range []string{"API_REJECTED", "API_UNREACHABLE", "CONSUME_VALIDATION_FAILED",
		"FAILED_PERMANENT", "NORMALIZE_FAILED", "CPE_NOT_FOUND", "RT_UPDATE_FAILED"} {
		if card(bucket(state, true, 0)) != "manual_check" || !strings.Contains(condManualCheck, "'"+state+"'") {
			t.Errorf("manual check and bucket disagree on %s", state)
		}
	}
}

func TestCaseTotal(t *testing.T) {
	w := NewWindow(time.Date(2026, 10, 6, 14, 35, 0, 0, time.Local), 10)
	r := newSummaryResponse(w, Summary{Received: 312, InProgress: 4, Success: 192, RemoteResolved: 136,
		NotRemoteResolved: 56, KeptInNOC: 22, NotEligible: 40, CPENotFound: 7, APIUnreachable: 3})
	for c, want := range map[Case]int{CaseReceived: 312, CaseInProgress: 4, CaseSuccess: 192,
		CaseTransferred: 34, CaseKeptInNOC: 22, CaseNotEligible: 40, CaseManualCheck: 10} {
		if got := caseTotal(r, c); got != want {
			t.Errorf("caseTotal(%s) = %d, want %d", c, got, want)
		}
	}
}

func TestNewRunItem(t *testing.T) {
	start := time.Date(2026, 10, 6, 9, 0, 0, 0, time.Local)
	it := newRunItem(ExportRun{RunID: "r1", State: "API_REJECTED", CreatedAt: start,
		HTTPStatus:      sql.NullInt64{Int64: 409, Valid: true},
		LastStateReason: sql.NullString{String: "ticket already open", Valid: true},
		CompletedAt:     sql.NullTime{Time: start.Add(95 * time.Second), Valid: true}})
	if it.Bucket != "api_error" || *it.HTTPStatus != 409 || it.Reason != "ticket already open" ||
		*it.DurationSec != 95 || *it.CompletedAt != "2026-10-06T09:01:35" || it.IsBCSSuccess != nil {
		t.Fatalf("run item = %+v", it)
	}
}

func TestCaseWhere(t *testing.T) {
	w := NewWindow(time.Date(2026, 10, 6, 14, 35, 0, 0, time.Local), 10)
	w.NOCQueue = "NOC"
	for _, c := range []Case{CaseInProgress, CaseReceived, CaseKeptInNOC, CaseTransferred, CaseManualCheck} {
		for _, f := range []Filter{FilterAll, FilterRetrying, FilterStuck} {
			cond, args, err := caseWhere(w, c, f)
			if err != nil || strings.Count(cond, "?") != len(args) {
				t.Errorf("caseWhere(%s, %s): %d placeholders, %d args, err %v", c, f, strings.Count(cond, "?"), len(args), err)
			}
		}
	}
	if _, _, err := caseWhere(w, "skipped", FilterAll); err == nil {
		t.Error("caseWhere(skipped) accepted")
	}
}
