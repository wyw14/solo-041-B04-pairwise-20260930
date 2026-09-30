package process

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

func newTestPlant(t *testing.T) *Plant {
	t.Helper()
	base := time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)
	plant, err := NewPlant(t.TempDir(), []model.LineID{"L1"}, func() time.Time { return base })
	if err != nil {
		t.Fatalf("NewPlant: %v", err)
	}
	return plant
}

func equalizerForLine(plant *Plant, lineID model.LineID) (flow float64, assigned bool) {
	for _, basin := range plant.equalizers.Snapshot() {
		if basin.LineID == lineID {
			return basin.AssignedFlow, true
		}
	}
	return 0, false
}

// A rejected duplicate request must leave the existing batch's intake
// admission, equalizer occupancy and journal exactly as they were.
func TestStartBatchConflictLeavesNoSideEffects(t *testing.T) {
	ctx := context.Background()
	plant := newTestPlant(t)
	lineID := model.LineID("L1")

	batch, err := plant.StartBatch(ctx, lineID, 1000, "source-a")
	if err != nil {
		t.Fatalf("first StartBatch: %v", err)
	}

	admission, ok := plant.intake.Current(lineID)
	if !ok {
		t.Fatal("expected an intake admission for the active batch")
	}
	if admission.FlowM3Hour != 1000 || admission.Source != "source-a" {
		t.Fatalf("admission = %+v, want flow 1000 source-a", admission)
	}
	flow, assigned := equalizerForLine(plant, lineID)
	if !assigned || flow != 1000 {
		t.Fatalf("equalizer assigned=%v flow=%v, want assigned flow 1000", assigned, flow)
	}

	if _, err := plant.StartBatch(ctx, lineID, 1400, "source-b"); err == nil {
		t.Fatal("second StartBatch on an active line must fail with a conflict")
	}

	// Intake admission must still describe the first request.
	admission, ok = plant.intake.Current(lineID)
	if !ok {
		t.Fatal("failed request removed the existing intake admission")
	}
	if admission.FlowM3Hour != 1000 || admission.Source != "source-a" {
		t.Fatalf("admission = %+v after conflict, want unchanged flow 1000 source-a", admission)
	}
	if flow, _ := equalizerForLine(plant, lineID); flow != 1000 {
		t.Fatalf("equalizer flow = %v after conflict, want unchanged 1000", flow)
	}

	active, ok := plant.ActiveBatch(lineID)
	if !ok || active.ID != batch.ID {
		t.Fatalf("active batch changed after failed request")
	}
	if len(plant.batches) != 1 || len(plant.lineBatches) != 1 {
		t.Fatalf("batch indexes mutated: batches=%d lineBatches=%d", len(plant.batches), len(plant.lineBatches))
	}

	events, err := plant.store.Events(ctx, lineID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 1 || events[0].Kind != model.EventBatchCreated {
		t.Fatalf("journal has %d events after conflict, want exactly one batch.created", len(events))
	}
}

// Concurrent create requests against the same line must admit exactly one
// batch and never corrupt the intake allocation.
func TestStartBatchConcurrentOnlyOneSucceeds(t *testing.T) {
	ctx := context.Background()
	plant := newTestPlant(t)
	lineID := model.LineID("L1")

	const n = 64
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	var winnerFlow float64
	var winnerSource string
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		flow := 1000 + float64(i)
		source := "source-" + string(rune('a'+i))
		go func() {
			defer wg.Done()
			<-start
			_, err := plant.StartBatch(ctx, lineID, flow, source)
			if err == nil {
				mu.Lock()
				successes++
				winnerFlow = flow
				winnerSource = source
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if successes != 1 {
		t.Fatalf("concurrent StartBatch succeeded %d times, want exactly 1", successes)
	}
	if len(plant.batches) != 1 || len(plant.lineBatches) != 1 {
		t.Fatalf("batch indexes corrupted: batches=%d lineBatches=%d", len(plant.batches), len(plant.lineBatches))
	}
	admission, ok := plant.intake.Current(lineID)
	if !ok {
		t.Fatal("missing intake admission after concurrent creates")
	}
	if admission.FlowM3Hour != winnerFlow || admission.Source != winnerSource {
		t.Fatalf("admission = %+v, want winning request flow %v source %q", admission, winnerFlow, winnerSource)
	}
	if flow, _ := equalizerForLine(plant, lineID); flow != winnerFlow {
		t.Fatalf("equalizer flow = %v, want winning flow %v", flow, winnerFlow)
	}
	events, err := plant.store.Events(ctx, lineID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("journal has %d events after concurrent creates, want exactly 1", len(events))
	}
}

// A request that fails intake validation before the batch is reserved must not
// create a batch or move the line state.
func TestStartBatchInvalidFlowIsRejectedCleanly(t *testing.T) {
	ctx := context.Background()
	plant := newTestPlant(t)
	lineID := model.LineID("L1")

	if _, err := plant.StartBatch(ctx, lineID, 5000, "source-a"); err == nil {
		t.Fatal("StartBatch above configured capacity must fail")
	}
	if _, ok := plant.ActiveBatch(lineID); ok {
		t.Fatal("invalid flow request created an active batch")
	}
	if len(plant.batches) != 0 || len(plant.lineBatches) != 0 {
		t.Fatalf("indexes mutated on validation failure: batches=%d lineBatches=%d", len(plant.batches), len(plant.lineBatches))
	}
	if _, ok := plant.intake.Current(lineID); ok {
		t.Fatal("invalid flow request recorded an intake admission")
	}
	if flow, assigned := equalizerForLine(plant, lineID); assigned && flow != 0 {
		t.Fatalf("invalid flow request occupied an equalizer: flow=%v", flow)
	}
	events, err := plant.store.Events(ctx, lineID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("journal has %d events after invalid request, want none", len(events))
	}
}
