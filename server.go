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
	return mux
}

// window fixes "today" and "now" on the server clock, never the browser's.
func (s *Server) window() Window {
	w := NewWindow(s.now().In(s.loc), s.stuckMinutes)
	w.NOCQueue = s.nocQueue
	return w
}

// handleToday serves the summary and the live list measured at one instant,
// which is what the page polls every 5 seconds.
func (s *Server) handleToday(w http.ResponseWriter, r *http.Request) {
	f, limit, ok := listParams(w, r)
	if !ok {
		return
	}
	win := s.window()
	var sum Summary
	var live InProgressResponse
	err := s.store.Snapshot(r.Context(), func(q querier) (err error) {
		if sum, err = querySummary(r.Context(), q, win); err != nil {
			return err
		}
		live, err = inProgress(r.Context(), q, win, sum, f, limit)
		return err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, map[string]any{
		"summary":     newSummaryResponse(win, sum),
		"in_progress": live,
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

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	win := s.window()
	var runs []ExportRun
	err := s.store.Snapshot(r.Context(), func(q querier) (err error) {
		runs, err = queryTodayRuns(r.Context(), q, win)
		return err
	})
	if err != nil {
		serverError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition",
		`attachment; filename="pipeline-runs-`+win.Start.Format("2006-01-02")+`.csv"`)

	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"run_id", "ticket_id", "ticket_no", "cpe_id", "township", "current_state", "card", "bucket", "before_queue", "target_queue",
		"api_http_status", "is_remote_resolved", "is_bcs_success", "last_state_reason",
		"created_at", "updated_at", "completed_at"})
	for _, run := range runs {
		b := bucket(run.State, run.CompletedAt.Valid, run.HTTPStatus.Int64)
		_ = cw.Write([]string{
			run.RunID, nullInt(run.TicketID), run.TicketNo.String, run.CpeID.String, run.Township.String,
			run.State, card(b), b, run.BeforeQueue.String, run.TargetQueue.String,
			nullInt(run.HTTPStatus), nullBool(run.IsRemoteResolved), nullBool(run.IsBCSSuccess),
			run.LastStateReason.String,
			run.CreatedAt.Format(wireTime), nullTime(run.UpdatedAt), nullTime(run.CompletedAt),
		})
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
