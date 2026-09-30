package intake

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

type Admission struct {
	LineID      model.LineID `json:"line_id"`
	FlowM3Hour  float64      `json:"flow_m3_hour"`
	Source      string       `json:"source"`
	AcceptedAt  time.Time    `json:"accepted_at"`
	EqualizerID string       `json:"equalizer_id"`
}

type Controller struct {
	mu         sync.RWMutex
	capacity   map[model.LineID]float64
	admissions map[model.LineID]Admission
	equalizers *EqualizationPool
}

func NewController(equalizers *EqualizationPool) (*Controller, error) {
	if equalizers == nil {
		return nil, errors.New("equalization pool is required")
	}
	return &Controller{
		capacity: make(map[model.LineID]float64), admissions: make(map[model.LineID]Admission),
		equalizers: equalizers,
	}, nil
}

func (c *Controller) Configure(lineID model.LineID, maximumFlow float64) error {
	if lineID == "" || maximumFlow <= 0 {
		return errors.New("line and positive intake capacity are required")
	}
	c.mu.Lock()
	c.capacity[lineID] = maximumFlow
	c.mu.Unlock()
	return nil
}

func (c *Controller) Admit(ctx context.Context, lineID model.LineID, flow float64, source string, now time.Time) (Admission, error) {
	if err := ctx.Err(); err != nil {
		return Admission{}, err
	}
	c.mu.RLock()
	capacity, configured := c.capacity[lineID]
	c.mu.RUnlock()
	if !configured {
		return Admission{}, errors.New("process line intake is not configured")
	}
	if flow <= 0 || flow > capacity {
		return Admission{}, errors.New("intake flow exceeds configured range")
	}
	basin, err := c.equalizers.Assign(lineID, flow, now)
	if err != nil {
		return Admission{}, err
	}
	admission := Admission{LineID: lineID, FlowM3Hour: flow, Source: source, AcceptedAt: now.UTC(), EqualizerID: basin.ID}
	c.mu.Lock()
	c.admissions[lineID] = admission
	c.mu.Unlock()
	return admission, nil
}

func (c *Controller) Current(lineID model.LineID) (Admission, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.admissions[lineID]
	return value, ok
}

// Release withdraws a line's admission and frees its equalizer. It is used when
// a batch start that already reserved intake capacity fails to commit, so that
// a failed request leaves no admission and no pool occupancy behind.
func (c *Controller) Release(lineID model.LineID) {
	c.mu.Lock()
	delete(c.admissions, lineID)
	c.mu.Unlock()
	c.equalizers.Release(lineID)
}
