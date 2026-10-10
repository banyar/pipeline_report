package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// sqlLogger wraps a querier and logs each query with its arguments filled in,
// so the logged SQL can be pasted into a MySQL client as-is.
type sqlLogger struct {
	q querier
}

func (l sqlLogger) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := l.q.QueryContext(ctx, query, args...)
	logSQL(sqlTitle(ctx)+" ==="+sqlRequest(ctx), query, args, time.Since(start), err)
	return rows, err
}

func (l sqlLogger) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	start := time.Now()
	row := l.q.QueryRowContext(ctx, query, args...)
	logSQL(sqlTitle(ctx)+" ==="+sqlRequest(ctx), query, args, time.Since(start), row.Err())
	return row
}

// sqlLogRule closes each logged query so consecutive ones are easy to tell apart.
var sqlLogRule = strings.Repeat("=", 80)

type sqlTitleKey struct{}

// withSQLTitle names the queries run with ctx in the SQL log.
func withSQLTitle(ctx context.Context, title string) context.Context {
	return context.WithValue(ctx, sqlTitleKey{}, title)
}

func sqlTitle(ctx context.Context) string {
	if title, ok := ctx.Value(sqlTitleKey{}).(string); ok {
		return title
	}
	return "query"
}

type sqlRequestKey struct{}

// withSQLRequest tags every query a request runs with that request's path
// and query string, so the SQL log shows whether a click or a poll ran it.
func withSQLRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := strings.TrimPrefix(r.URL.Path, "/api/v1/pipeline-runs")
		if r.URL.RawQuery != "" {
			req += "?" + r.URL.RawQuery
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), sqlRequestKey{}, req)))
	})
}

func sqlRequest(ctx context.Context) string {
	if req, ok := ctx.Value(sqlRequestKey{}).(string); ok {
		return " [" + req + "]"
	}
	return ""
}

func logSQL(title, query string, args []any, took time.Duration, err error) {
	status := "ok"
	if err != nil {
		status = err.Error()
	}
	log.Printf("SQL QUERY === %s (%s, %s):\n%s;\n%s\n", title, took.Round(time.Microsecond), status, interpolateSQL(query, args), sqlLogRule)
}

// interpolateSQL replaces each ? placeholder outside a quoted string with its
// argument as a SQL literal. It is for logging only, never for execution.
func interpolateSQL(query string, args []any) string {
	var b strings.Builder
	var quote rune
	n := 0
	for _, r := range strings.TrimSpace(query) {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"' || r == '`':
			quote = r
		case r == '?' && n < len(args):
			b.WriteString(sqlLiteral(args[n]))
			n++
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func sqlLiteral(v any) string {
	switch v := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case time.Time:
		// The DSN sends times in the report's timezone (loc=), so log them the same way.
		return "'" + v.Format("2006-01-02 15:04:05") + "'"
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprint(v)
	}
}
