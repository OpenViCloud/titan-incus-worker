package command

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// Pool runs one loop per slot, each under leases of its own, so a long command holds one slot and the others go on
// taking commands. Which commands may run side by side is the control plane's lease rule, not the pool's
// (titan-ops features/provider/design.md#tenant-lane).
type Pool struct {
	loops []*Loop
}

func NewPool(cp *controlplane.Client, routes map[string]Route, pollInterval time.Duration, slots int, logger *slog.Logger) *Pool {
	pool := &Pool{}
	for range slots {
		pool.loops = append(pool.loops, NewLoop(cp, routes, pollInterval, logger))
	}
	return pool
}

// Run runs every slot until ctx ends, and returns once all have stopped.
func (p *Pool) Run(ctx context.Context) {
	var running sync.WaitGroup
	for _, loop := range p.loops {
		running.Go(func() { loop.Run(ctx) })
	}
	running.Wait()
}
