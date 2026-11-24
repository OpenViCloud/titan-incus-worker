package restore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const gib = uint64(1) << 30

func poolWith(total uint64, used uint64) *testsupport.Provider {
	p := testsupport.NewProvider()
	p.PoolSpace = api.ResourcesStoragePoolSpace{Total: total, Used: used}
	return p
}

// The rule of titan-ops features/backup/design.md#restore-drill: free >= 1.5 x the bytes imported + a tenth of the pool.
func TestCheckPoolRoom_FreeIsExactlyTheRequirement_Passes_AndOneByteLess_IsRefused(t *testing.T) {
	required := 9*gib + 4*gib // 1.5 x 6 GiB + 10% of 40 GiB
	for name, tc := range map[string]struct {
		free uint64
		ok   bool
	}{"exactly": {required, true}, "one byte less": {required - 1, false}} {
		p := poolWith(40*gib, 40*gib-tc.free)

		err := CheckPoolRoom(context.Background(), p, "default", int64(6*gib))

		if tc.ok != (err == nil) {
			t.Errorf("%s: err = %v, want success=%v", name, err, tc.ok)
		}
	}
}

// The refusal says what was measured, and reads the same whoever imports: a drill, or a restore of one resource.
func TestCheckPoolRoom_TheRefusal_SaysThePoolTheFreeSpaceTheImportAndTheNeed_AndNamesNoCaller(t *testing.T) {
	p := poolWith(40*gib, 34*gib)

	err := CheckPoolRoom(context.Background(), p, "pool1", int64(6*gib))

	if err == nil {
		t.Fatal("6 GiB free passed for an import that needs 13")
	}
	want := "pool pool1 has 6.0 GiB free, importing 6.0 GiB needs 13.0 GiB (1.5 x the disks + a tenth of the pool): not enough space"
	if err.Error() != want {
		t.Errorf("refusal\n got %q\nwant %q", err, want)
	}
	if strings.Contains(err.Error(), "drill") || strings.Contains(err.Error(), "restore") {
		t.Errorf("the refusal names a caller: %q", err)
	}
}

// With no size known, the tenth of the pool is still kept free.
func TestCheckPoolRoom_AnImportOfNoKnownSize_StillNeedsTheTenthOfThePool(t *testing.T) {
	for name, tc := range map[string]struct {
		used uint64
		ok   bool
	}{"a tenth free": {36 * gib, true}, "one byte less than a tenth free": {36*gib + 1, false}} {
		p := poolWith(40*gib, tc.used)

		err := CheckPoolRoom(context.Background(), p, "default", 0)

		if tc.ok != (err == nil) {
			t.Errorf("%s: err = %v, want success=%v", name, err, tc.ok)
		}
	}
}

// LVM thin pools report more used than there is when overcommitted: that is no free space, not a huge amount of it.
func TestCheckPoolRoom_APoolUsedBeyondItsTotal_HasNoFreeSpace(t *testing.T) {
	p := poolWith(40*gib, 41*gib)

	err := CheckPoolRoom(context.Background(), p, "default", int64(gib))

	if err == nil || !strings.Contains(err.Error(), "has 0.0 GiB free") {
		t.Fatalf("err = %v, want a refusal for 0.0 GiB free", err)
	}
}

func TestCheckPoolRoom_ASizeBelowZero_IsRefused_BeforeThePoolIsRead(t *testing.T) {
	p := poolWith(40*gib, 0)

	err := CheckPoolRoom(context.Background(), p, "default", -1)

	if err == nil || p.Calls != 0 {
		t.Fatalf("err = %v after %d call(s), want a refusal and no call", err, p.Calls)
	}
}

func TestCheckPoolRoom_APoolThatCannotBeRead_IsThatError(t *testing.T) {
	unreadable := errors.New("incus is busy")
	p := poolWith(40*gib, 0)
	p.PoolResourcesErr = unreadable

	err := CheckPoolRoom(context.Background(), p, "default", int64(gib))

	if !errors.Is(err, unreadable) {
		t.Fatalf("err = %v, want the pool's error", err)
	}
}

// A caller gives up at once on a pool that was read and is too full, and tries again on one that could not be read:
// the two must not look alike.
func TestCheckPoolRoom_OnlyAPoolReadAndFoundTooFull_IsErrNoRoom(t *testing.T) {
	tooFull := poolWith(40*gib, 34*gib)
	unreadable := poolWith(40*gib, 0)
	unreadable.PoolResourcesErr = errors.New("incus is busy")

	refusal := CheckPoolRoom(context.Background(), tooFull, "default", int64(6*gib))
	failedRead := CheckPoolRoom(context.Background(), unreadable, "default", int64(6*gib))
	notASize := CheckPoolRoom(context.Background(), tooFull, "default", -1)

	if !errors.Is(refusal, ErrNoRoom) {
		t.Errorf("a pool too full: %v, want ErrNoRoom", refusal)
	}
	if failedRead == nil || errors.Is(failedRead, ErrNoRoom) {
		t.Errorf("a pool that could not be read: %v, want an error that is not ErrNoRoom", failedRead)
	}
	if notASize == nil || errors.Is(notASize, ErrNoRoom) {
		t.Errorf("a size below zero: %v, want an error that is not ErrNoRoom", notASize)
	}
}
