package store

import (
	"database/sql"
	"errors"
	"testing"
)

func TestSchedulesRoundTrip(t *testing.T) {
	db := open(t)

	id, err := db.UpsertSchedule(Schedule{
		Name: "nightly-dev", Spec: "0 3 * * *", Op: "copy-down", From: "prod", To: "dev",
	})
	if err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	if _, err := db.UpsertSchedule(Schedule{Name: "x", Spec: "* * * * *"}); err == nil {
		t.Error("UpsertSchedule accepted a schedule with no op")
	}

	// Record a run, then editing the schedule clears the history.
	if err := db.RecordScheduleRun("nightly-dev", "2026-09-07T03:00:00Z", "ok"); err != nil {
		t.Fatal(err)
	}
	if s, _ := db.ScheduleByName("nightly-dev"); s.LastRun == "" || s.LastStatus != "ok" {
		t.Fatalf("run not recorded: %+v", s)
	}

	id2, err := db.UpsertSchedule(Schedule{
		Name: "nightly-dev", Spec: "30 2 * * *", Op: "copy-down", From: "prod", To: "dev", SkipFiles: true,
	})
	if err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	if id != id2 {
		t.Errorf("re-upsert created a new row: %d then %d", id, id2)
	}
	s, _ := db.ScheduleByName("nightly-dev")
	if s.Spec != "30 2 * * *" || !s.SkipFiles || s.LastRun != "" || s.LastStatus != "" {
		t.Errorf("edit did not update fields / clear history: %+v", s)
	}

	if err := db.SetScheduleEnabled("nightly-dev", false); err != nil {
		t.Fatal(err)
	}
	if s, _ := db.ScheduleByName("nightly-dev"); s.Enabled {
		t.Error("SetScheduleEnabled(false) had no effect")
	}
	if err := db.SetScheduleEnabled("ghost", true); err == nil {
		t.Error("SetScheduleEnabled on a missing schedule should error")
	}

	if err := db.DeleteSchedule("nightly-dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ScheduleByName("nightly-dev"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("ScheduleByName after delete = %v, want sql.ErrNoRows", err)
	}
	if list, _ := db.Schedules(); len(list) != 0 {
		t.Errorf("Schedules after delete = %+v, want empty", list)
	}
}
