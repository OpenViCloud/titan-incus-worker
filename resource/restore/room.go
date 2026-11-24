package restore

import (
	"context"
	"errors"
	"fmt"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// An import needs room for what it brings with half as much again, and a tenth of the pool beyond that, so it never
// takes the pool a tenant's next write needs (measured 2026-11-23: an import grew the pool by 1.03x the root disk;
// titan-ops features/backup/design.md#restore-drill).
const (
	marginDivisor  = 2
	reserveDivisor = 10
)

// ErrNoRoom is in CheckPoolRoom's refusal of a pool it read and found too full. A pool it could not read is another
// error: that one may pass on a second try.
var ErrNoRoom = errors.New("not enough space")

// CheckPoolRoom refuses an import the pool lacks room for: free >= 1.5 x importBytes + a tenth of the pool. The
// refusal says what was measured. importBytes is the size of the disks the import makes, 0 when none is known: the
// tenth of the pool is then all that is checked.
func CheckPoolRoom(ctx context.Context, p provider.Provider, pool string, importBytes int64) error {
	if importBytes < 0 {
		return fmt.Errorf("an import of %d bytes into pool %s is not a size", importBytes, pool)
	}
	resources, err := p.GetStoragePoolResources(ctx, pool)
	if err != nil {
		return err
	}
	space := resources.Space
	free := uint64(0)
	if space.Total > space.Used {
		free = space.Total - space.Used
	}
	imported := uint64(importBytes)
	need := imported + imported/marginDivisor + space.Total/reserveDivisor
	if free < need {
		return fmt.Errorf("pool %s has %s free, importing %s needs %s (1.5 x the disks + a tenth of the pool): %w",
			pool, inGiB(free), inGiB(imported), inGiB(need), ErrNoRoom)
	}
	return nil
}

func inGiB(bytes uint64) string {
	return fmt.Sprintf("%.1f GiB", float64(bytes)/float64(1<<30))
}
