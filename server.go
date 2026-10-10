package main

import (
	"context"
	"embed"
	"encoding/csv"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"time"
)

//go:embed web
var webFiles embed.FS

type Server struct {
	store        *Store
	loc          *time.Location
	stuckMinutes int
	nocQueue     string
	now          func() time.Time
}

func (s *Server) Routes() http.Handler {
	static, _ := fs.Sub(webFiles, "web")
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /api/v1/pipeline-runs/today", s.handleToday)
	mux.HandleFunc("GET /api/v1/pipeline-runs/today/summary", s.handleSummary)
	mux.HandleFunc("GET /api/v1/pipeline-runs/today/in-progress", s.handleInProgress)
	mux.HandleFunc("GET /api/v1/pipeline-runs/today/export.csv", s.handleExport)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return withSQLRequest(mux)
}

// window fixes "today" and "now" on the server clock, never the browser's.
func (s *Server) window() Window {
	w := NewWindow(s.now().In(s.loc), s.stuckMinutes)
	w.NOCQueue = s.nocQueue
	return w
}

// handleToday serves the summary and one list measured at one instant, which
// is what the page polls every 15 seconds. The list is the live in-progress
// one ("in_progress"), or with ?case= the runs behind another card ("runs").
func (s *Server) handleToday(w http.ResponseWriter, r *http.Request) {
	f, limit, ok := listParams(w, r)
	if !ok {
		return
	}
	c, ok := ParseCase(r.URL.Query().Get("case"))
	if !ok {
		http.Error(w, "unknown case", http.StatusBadRequest)
		return
	}
	// A card click (?summary=0) reloads only that case's list; the cards
	// catch up on the next poll.
	if c != CaseInProgress && r.URL.Query().Get("summary") == "0" {
		s.handleCaseOnly(w, r, c, limit)
		return
	}
	win := s.window()
	var sum Summary
	var live InProgressResponse
	var runs []ExportRun
	err := s.store.Snapshot(r.Context(), func(q querier) (err error) {
		if sum, err = querySummary(r.Context(), q, win); err != nil {
			return err
		}
		if c != CaseInProgress {
			runs, err = queryCaseRuns(r.Context(), q, win, c, limit)
			return err
		}
		live, err = inProgress(r.Context(), q, win, sum, f, limit)
		return err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	summary := newSummaryResponse(win, sum)
	if c != CaseInProgress {
		items := make([]RunItem, 0, len(runs))
		for _, run := range runs {
			items = append(items, newRunItem(run))
		}
		writeJSON(w, map[string]any{
			"summary": summary,
			"runs": CaseResponse{
				GeneratedAt: win.Now.Format(wireTime),
				Case:        c,
				Total:       caseTotal(summary, c),
				Limit:       limit,
				Items:       items,
			},
		})
		return
	}
	writeJSON(w, map[string]any{
		"summary":     summary,
		"in_progress": live,
	})
}

// handleCaseOnly answers a card click with the case list and its count, no
// summary.
func (s *Server) handleCaseOnly(w http.ResponseWriter, r *http.Request, c Case, limit int) {
	win := s.window()
	var total int
	var runs []ExportRun
	err := s.store.Snapshot(r.Context(), func(q querier) (err error) {
		if total, err = queryCaseCount(r.Context(), q, win, c); err != nil {
			return err
		}
		runs, err = queryCaseRuns(r.Context(), q, win, c, limit)
		return err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	items := make([]RunItem, 0, len(runs))
	for _, run := range runs {
		items = append(items, newRunItem(run))
	}
	writeJSON(w, map[string]any{
		"runs": CaseResponse{
			GeneratedAt: win.Now.Format(wireTime),
			Case:        c,
			Total:       total,
			Limit:       limit,
			Items:       items,
		},
	})
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	win := s.window()
	var sum Summary
	err := s.store.Snapshot(r.Context(), func(q querier) (err error) {
		sum, err = querySummary(r.Context(), q, win)
		return err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, newSummaryResponse(win, sum))
}

func (s *Server) handleInProgress(w http.ResponseWriter, r *http.Request) {
	f, limit, ok := listParams(w, r)
	if !ok {
		return
	}
	win := s.window()
	var live InProgressResponse
	err := s.store.Snapshot(r.Context(), func(q querier) error {
		sum, err := querySummary(r.Context(), q, win)
		if err != nil {
			return err
		}
		live, err = inProgress(r.Context(), q, win, sum, f, limit)
		return err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, live)
}

func inProgress(ctx context.Context, q querier, win Window, sum Summary, f Filter, limit int) (InProgressResponse, error) {
	runs, err := queryInProgress(ctx, q, win, f, limit)
	if err != nil {
		return InProgressResponse{}, err
	}
	items := make([]LiveItem, 0, len(runs))
	for _, run := range runs {
		items = append(items, newLiveItem(win, run))
	}
	return InProgressResponse{
		GeneratedAt: win.Now.Format(wireTime),
		Filter:      f,
		Total:       filterTotal(sum, f),
		Limit:       limit,
		Items:       items,
	}, nil
}

// handleExport writes all of today's runs, or with ?case= (and for
// in_progress, ?filter=) only the runs of the list the page is showing.
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	rawCase := q.Get("case")
	c, ok := ParseCase(rawCase)
	if !ok {
		http.Error(w, "unknown case", http.StatusBadRequest)
		return
	}
	f, ok := ParseFilter(q.Get("filter"))
	if !ok {
		http.Error(w, "filter must be all, retrying or stuck", http.StatusBadRequest)
		return
	}
	win := s.window()
	name := "pipeline-runs-" + win.Start.Format("2006-01-02")
	if rawCase != "" {
		name += "-" + string(c)
		if c == CaseInProgress && f != FilterAll {
			name += "-" + string(f)
		}
	}
	var runs []ExportDetail
	err := s.store.Snapshot(r.Context(), func(q querier) (err error) {
		if rawCase == "" {
			runs, err = queryTodayRuns(r.Context(), q, win)
		} else {
			runs, err = queryCaseExport(r.Context(), q, win, c, f)
		}
		return err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition",
		`attachment; filename="`+name+`.csv"`)

	cw := csv.NewWriter(w)
	header := make([]string, len(exportCSV))
	for i, c := range exportCSV {
		header[i] = c.name
	}
	_ = cw.Write(header)
	row := make([]string, len(exportCSV))
	for _, run := range runs {
		b := bucket(run.State, run.CompletedAt.Valid, run.HTTPStatus.Int64)
		for i, c := range exportCSV {
			row[i] = c.value(run, b)
		}
		_ = cw.Write(row)
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		log.Printf("export csv: %v", err)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.db.PingContext(ctx); err != nil {
		http.Error(w, "database unreachable", http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ok"))
}

func listParams(w http.ResponseWriter, r *http.Request) (Filter, int, bool) {
	q := r.URL.Query()
	f, ok := ParseFilter(q.Get("filter"))
	if !ok {
		http.Error(w, "filter must be all, retrying or stuck", http.StatusBadRequest)
		return "", 0, false
	}
	limit := defaultLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return "", 0, false
		}
		limit = min(n, maxLimit)
	}
	return f, limit, true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json: %v", err)
	}
}

// serverError logs the detail and keeps it (DSN, SQL) out of the response.
func serverError(w http.ResponseWriter, err error) {
	log.Printf("error: %v", err)
	http.Error(w, "report query failed", http.StatusInternalServerError)
}

// exportCSV is the CSV export's columns in order: the ticket, the CPE and
// network, the run's state, the queue / BCS outcome, then the timestamps.
// b is the run's bucket.
var exportCSV = []struct {
	name  string
	value func(r ExportDetail, b string) string
}{
	{"run_id", func(r ExportDetail, _ string) string { return r.RunID }},
	// ticket
	{"ticket_id", func(r ExportDetail, _ string) string { return nullInt(r.TicketID) }},
	{"ticket_no", func(r ExportDetail, _ string) string { return r.TicketNo.String }},
	{"ticket_status", func(r ExportDetail, _ string) string { return r.TicketStatus.String }},
	{"ticket_created_at", func(r ExportDetail, _ string) string { return nullTime(r.TicketCreatedAt) }},
	{"ticket_problem", func(r ExportDetail, _ string) string { return r.TicketProblem.String }},
	{"tags", func(r ExportDetail, _ string) string { return r.Tags.String }},
	{"service_area", func(r ExportDetail, _ string) string { return r.ServiceArea.String }},
	{"township", func(r ExportDetail, _ string) string { return r.Township.String }},
	// CPE and network
	{"cpe_id", func(r ExportDetail, _ string) string { return r.CpeID.String }},
	{"local_service_id", func(r ExportDetail, _ string) string { return r.LocalServiceID.String }},
	{"ref_bcs_process_id", func(r ExportDetail, _ string) string { return r.RefBCSProcessID.String }},
	{"onu_serial", func(r ExportDetail, _ string) string { return r.ONUSerial.String }},
	{"olt_hostname", func(r ExportDetail, _ string) string { return r.OLTHostname.String }},
	{"ca1", func(r ExportDetail, _ string) string { return r.CA1.String }},
	{"uplink", func(r ExportDetail, _ string) string { return r.Uplink.String }},
	// run state
	{"current_state", func(r ExportDetail, _ string) string { return r.State }},
	{"card", func(_ ExportDetail, b string) string { return card(b) }},
	{"bucket", func(_ ExportDetail, b string) string { return b }},
	{"api_http_status", func(r ExportDetail, _ string) string { return nullInt(r.HTTPStatus) }},
	{"last_state_reason", func(r ExportDetail, _ string) string { return r.LastStateReason.String }},
	{"active_retry_job_id", func(r ExportDetail, _ string) string { return nullInt(r.ActiveRetryJobID) }},
	// queue and BCS outcome
	{"before_queue", func(r ExportDetail, _ string) string { return r.BeforeQueue.String }},
	{"target_queue", func(r ExportDetail, _ string) string { return r.TargetQueue.String }},
	{"before_bcs_channel", func(r ExportDetail, _ string) string { return nullInt(r.BeforeBCSChannel) }},
	{"target_bcs_channel", func(r ExportDetail, _ string) string { return nullInt(r.TargetBCSChannel) }},
	{"is_remote_resolved", func(r ExportDetail, _ string) string { return nullBool(r.IsRemoteResolved) }},
	{"is_bcs_success", func(r ExportDetail, _ string) string { return nullBool(r.IsBCSSuccess) }},
	{"bcs_status_message", func(r ExportDetail, _ string) string { return r.BCSStatusMessage.String }},
	{"final_message", func(r ExportDetail, _ string) string { return r.FinalMessage.String }},
	// timestamps
	{"created_at", func(r ExportDetail, _ string) string { return r.CreatedAt.Format(wireTime) }},
	{"updated_at", func(r ExportDetail, _ string) string { return nullTime(r.UpdatedAt) }},
	{"completed_at", func(r ExportDetail, _ string) string { return nullTime(r.CompletedAt) }},
}
