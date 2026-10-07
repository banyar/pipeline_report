// Command pipeline_report serves the Remote Resolve Pipeline daily report
// (noc_automation/logs/mockups/pipeline-daily-report-v2.*) live from the
// pipeline_runs, pipeline_run_events and retry_jobs tables. It only reads
// the database, through its own connection pool.
//
//	go run . --env .env
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	envPath := flag.String("env", ".env", "config file")
	flag.Parse()

	cfg, err := LoadConfig(*envPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	db, err := sql.Open("mysql", cfg.DB.DSN(cfg.Location))
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(cfg.DB.MaxOpen)
	db.SetMaxIdleConns(cfg.DB.MaxOpen)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), defaultDBTimeout)
	err = db.PingContext(ctx)
	cancel()
	if err != nil {
		log.Fatalf("connect to MySQL %s:%s/%s: %v", cfg.DB.Host, cfg.DB.Port, cfg.DB.Name, err)
	}

	srv := &Server{store: &Store{db: db}, loc: cfg.Location, stuckMinutes: cfg.StuckMinutes, nocQueue: cfg.NOCQueue, now: time.Now}
	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
	}()

	log.Printf("pipeline report on http://%s (db %s:%s/%s, tz %s, stuck %s min)",
		displayAddr(cfg.Addr), cfg.DB.Host, cfg.DB.Port, cfg.DB.Name, cfg.Location, strconv.Itoa(cfg.StuckMinutes))
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http: %v", err)
	}
}

func displayAddr(addr string) string {
	if len(addr) > 0 && addr[0] == ':' {
		return "localhost" + addr
	}
	return addr
}
