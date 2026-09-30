package process

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

func newTestPlant(t *testing.T, lines ...model.LineID) *Plant {
	t.Helper()
	clock := func() time.Time { return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC) }
	plant, err := NewPlant(t.TempDir(), lines, clock)
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant
}

func TestStartBatchConflictLeavesIntakePoolAndJournalUntouched(t *testing.T) {
	plant := newTestPlant(t, "L1")
	ctx := context.Background()

	batch, err := plant.StartBatch(ctx, "L1", 120, "river-north")
	if err != nil {
		t.Fatalf("first start batch: %v", err)
	}
	beforeAdmission, ok := plant.intake.Current("L1")
	if !ok {
		t.Fatal("expected an intake admission after the first batch")
	}
	beforeEqualizers := plant.equalizers.Snapshot()
	beforeEvents, err := plant.store.Events(ctx, "L1")
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}

	if _, err := plant.StartBatch(ctx, "L1", 640, "industrial-outfall"); err == nil {
		t.Fatal("expected a conflict error for the second start")
	}

	afterAdmission, ok := plant.intake.Current("L1")
	if !ok {
		t.Fatal("intake admission disappeared after the rejected request")
	}
	if afterAdmission != beforeAdmission {
		t.Errorf("rejected request rewrote the intake admission: before %+v, after %+v", beforeAdmission, afterAdmission)
	}
	if afterEqualizers := plant.equalizers.Snapshot(); !slices.Equal(afterEqualizers, beforeEqualizers) {
		t.Errorf("rejected request changed equalizer occupancy: before %+v, after %+v", beforeEqualizers, afterEqualizers)
	}
	afterEvents, err := plant.store.Events(ctx, "L1")
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if len(afterEvents) != len(beforeEvents) {
		t.Errorf("rejected request appended journal events: before %d, after %d", len(beforeEvents), len(afterEvents))
	}
	active, ok := plant.ActiveBatch("L1")
	if !ok || active.ID != batch.ID {
		t.Errorf("active batch changed after the rejected request: before %s, after %+v", batch.ID, active)
	}
}

func TestStartBatchConcurrentCreatesOnlyOneBatch(t *testing.T) {
	plant := newTestPlant(t, "L1")
	ctx := context.Background()

	const attempts = 16
	var wg sync.WaitGroup
	successes := make(chan model.Batch, attempts)
	for i := range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch, err := plant.StartBatch(ctx, "L1", 100+float64(i)*10, "river-north")
			if err == nil {
				successes <- batch
			}
		}()
	}
	wg.Wait()
	close(successes)

	var won []model.Batch
	for batch := range successes {
		won = append(won, batch)
	}
	if len(won) != 1 {
		t.Fatalf("expected exactly one successful start, got %d", len(won))
	}
	active, ok := plant.ActiveBatch("L1")
	if !ok || active.ID != won[0].ID {
		t.Fatalf("active batch %v does not match the winning batch %s", active, won[0].ID)
	}
	events, err := plant.store.Events(ctx, "L1")
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("expected exactly one journal event, got %d", len(events))
	}
}
