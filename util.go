package main

import (
	"database/sql"
	"strconv"
)

func nullInt(v sql.NullInt64) string {
	if !v.Valid {
		return ""
	}
	return strconv.FormatInt(v.Int64, 10)
}

func nullBool(v sql.NullBool) string {
	if !v.Valid {
		return ""
	}
	if v.Bool {
		return "1"
	}
	return "0"
}

func nullTime(v sql.NullTime) string {
	if !v.Valid {
		return ""
	}
	return v.Time.Format(wireTime)
}
