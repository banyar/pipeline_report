package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const (
	defaultAddr        = ":8090"
	defaultNOCQueue    = "Network Operation Center (NOC)"
	defaultStuckMin    = 10
	defaultLimit       = 50
	maxLimit           = 200
	defaultDBMaxOpen   = 5
	defaultDBTimeout   = 5 * time.Second
	defaultQueryTimout = 10 * time.Second
)

type Config struct {
	Addr         string
	NOCQueue     string
	Location     *time.Location
	StuckMinutes int
	DB           DBConfig
}

type DBConfig struct {
	Host, Port, User, Password, Name string
	MaxOpen                          int
}

// DSN reads and writes DATETIME values in the report's timezone, the same
// way the pipeline services store them (rtdatacore uses loc=Local).
func (d DBConfig) DSN(loc *time.Location) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=%s&timeout=%s&readTimeout=%s",
		d.User, d.Password, d.Host, d.Port, d.Name, url.QueryEscape(loc.String()),
		defaultDBTimeout, defaultQueryTimout)
}

// LoadConfig reads envPath (a missing file is fine) with real environment
// variables taking precedence. The MYSQL_DB_* keys match noc_automation/.env,
// so that file can be passed as-is.
func LoadConfig(envPath string) (Config, error) {
	file := map[string]string{}
	if envPath != "" {
		values, err := godotenv.Read(envPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return Config{}, fmt.Errorf("read %s: %w", envPath, err)
		}
		if err == nil {
			file = values
		}
	}
	get := func(key, fallback string) string {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
		if v := strings.TrimSpace(file[key]); v != "" {
			return v
		}
		return fallback
	}

	cfg := Config{
		Addr:     get("REPORT_HTTP_ADDR", defaultAddr),
		NOCQueue: get("REPORT_NOC_QUEUE", defaultNOCQueue),
		DB: DBConfig{
			Host:     get("MYSQL_DB_HOST", ""),
			Port:     get("MYSQL_DB_PORT", "3306"),
			User:     get("MYSQL_DB_USERNAME", ""),
			Password: get("MYSQL_DB_PASSWORD", ""),
			Name:     get("MYSQL_DB_DATABASE", ""),
		},
	}

	tz := get("REPORT_TZ", "Local")
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return Config{}, fmt.Errorf("REPORT_TZ %q: %w", tz, err)
	}
	cfg.Location = loc

	if cfg.StuckMinutes, err = positiveInt(get("REPORT_STUCK_MINUTES", ""), defaultStuckMin); err != nil {
		return Config{}, fmt.Errorf("REPORT_STUCK_MINUTES: %w", err)
	}
	if cfg.DB.MaxOpen, err = positiveInt(get("REPORT_DB_MAX_OPEN", ""), defaultDBMaxOpen); err != nil {
		return Config{}, fmt.Errorf("REPORT_DB_MAX_OPEN: %w", err)
	}

	var missing []string
	for _, req := range []struct{ name, value string }{
		{"MYSQL_DB_HOST", cfg.DB.Host},
		{"MYSQL_DB_USERNAME", cfg.DB.User},
		{"MYSQL_DB_DATABASE", cfg.DB.Name},
	} {
		if req.value == "" {
			missing = append(missing, req.name)
		}
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing config: %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}

func positiveInt(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q must be a positive integer", raw)
	}
	return n, nil
}
