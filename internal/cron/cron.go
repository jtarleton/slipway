// Package cron is a minimal five-field cron matcher — enough for slipway's
// scheduled operations, no more. Minute granularity, standard fields:
//
//	minute hour day-of-month month day-of-week
//
// Each field is "*", "*/step", a number, "a-b", or a comma list of those.
// Day-of-week is 0-6 with Sunday 0 (7 also accepted for Sunday). A time matches
// when every field matches; day-of-month and day-of-week are AND-ed, not OR-ed
// as Vixie cron does — slipway's schedules never need the special case.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type fieldRange struct{ min, max int }

var fields = []fieldRange{
	{0, 59}, // minute
	{0, 23}, // hour
	{1, 31}, // day of month
	{1, 12}, // month
	{0, 6},  // day of week
}

// Valid reports whether spec parses.
func Valid(spec string) bool {
	_, err := parse(spec)
	return err == nil
}

// Match reports whether t (truncated to the minute) satisfies spec.
func Match(spec string, t time.Time) (bool, error) {
	sets, err := parse(spec)
	if err != nil {
		return false, err
	}
	dow := int(t.Weekday()) // Sunday 0
	return sets[0][t.Minute()] && sets[1][t.Hour()] && sets[2][t.Day()] &&
		sets[3][int(t.Month())] && sets[4][dow], nil
}

// Next returns the earliest minute strictly after `after` that matches spec.
// It scans forward a minute at a time and gives up after a year — a spec with
// no match within a year (e.g. Feb 30) is treated as an error.
func Next(spec string, after time.Time) (time.Time, error) {
	sets, err := parse(spec)
	if err != nil {
		return time.Time{}, err
	}
	t := after.Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(1, 0, 0)
	for ; t.Before(limit); t = t.Add(time.Minute) {
		if sets[0][t.Minute()] && sets[1][t.Hour()] && sets[2][t.Day()] &&
			sets[3][int(t.Month())] && sets[4][int(t.Weekday())] {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cron %q has no match within a year of %s", spec, after)
}

// parse turns a spec into five sets of allowed values.
func parse(spec string) ([5]map[int]bool, error) {
	var sets [5]map[int]bool
	parts := strings.Fields(spec)
	if len(parts) != 5 {
		return sets, fmt.Errorf("cron %q: want 5 fields, got %d", spec, len(parts))
	}
	for i, part := range parts {
		set, err := parseField(part, fields[i])
		if err != nil {
			return sets, fmt.Errorf("cron %q field %d: %w", spec, i+1, err)
		}
		sets[i] = set
	}
	return sets, nil
}

func parseField(field string, r fieldRange) (map[int]bool, error) {
	out := map[int]bool{}
	for _, term := range strings.Split(field, ",") {
		lo, hi, step := r.min, r.max, 1

		base := term
		if slash := strings.IndexByte(term, '/'); slash >= 0 {
			s, err := strconv.Atoi(term[slash+1:])
			if err != nil || s < 1 {
				return nil, fmt.Errorf("bad step %q", term)
			}
			step = s
			base = term[:slash]
		}

		switch {
		case base == "*":
			// full range
		case strings.ContainsRune(base, '-'):
			a, b, ok := strings.Cut(base, "-")
			x, err1 := strconv.Atoi(a)
			y, err2 := strconv.Atoi(b)
			if !ok || err1 != nil || err2 != nil {
				return nil, fmt.Errorf("bad range %q", base)
			}
			lo, hi = x, y
		default:
			v, err := strconv.Atoi(base)
			if err != nil {
				return nil, fmt.Errorf("bad value %q", base)
			}
			lo, hi = v, v
		}

		lo, hi = normalize(lo, hi, r)
		if lo < r.min || hi > r.max || lo > hi {
			return nil, fmt.Errorf("%d-%d out of range %d-%d", lo, hi, r.min, r.max)
		}
		for v := lo; v <= hi; v += step {
			out[v] = true
		}
	}
	return out, nil
}

// normalize maps day-of-week 7 to 0 so "7" and "0" both mean Sunday. A range
// with 7 as an endpoint (e.g. "5-7") is unusual; write it as a comma list.
func normalize(lo, hi int, r fieldRange) (int, int) {
	if r.max == 6 {
		if lo == 7 {
			lo = 0
		}
		if hi == 7 {
			hi = 0
		}
	}
	return lo, hi
}
