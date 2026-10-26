package provider

import (
	"strings"
	"testing"
)

const (
	someId      = "11111111-2222-3333-4444-555555555555"
	someProject = "t-0123456789abcdef0123456789abcdef"
	// Oracle: lab-storage 05c, VG "default": an LV name of 117 characters was created, 118 refused.
	longestLvName = 117
)

func TestObjectNames_AreThePrefixAndTwentySixBase32Characters(t *testing.T) {
	instance, err := InstanceName(someId)
	if err != nil {
		t.Fatal(err)
	}
	volume, err := VolumeName(someId)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := SnapshotName(someId)
	if err != nil {
		t.Fatal(err)
	}
	// Oracle: python3 -c "import base64,uuid; print(base64.b32encode(uuid.UUID('11111111-2222-3333-4444-555555555555').bytes).decode().lower().rstrip('='))"
	const encoded = "ceircejceiztgrcekvkvkvkvku"
	if instance != "vm"+encoded || volume != "vol"+encoded || snapshot != "s"+encoded {
		t.Fatalf("names %q %q %q, want the prefixes before %q", instance, volume, snapshot, encoded)
	}
}

func TestBackupCopyNames_AreTheirPrefixAndTheSameBase32_ApartFromTheVmAndVolume(t *testing.T) {
	instanceCopy, err := BackupInstanceCopyName(someId)
	if err != nil {
		t.Fatal(err)
	}
	volumeCopy, err := BackupVolumeCopyName(someId)
	if err != nil {
		t.Fatal(err)
	}
	// Oracle: the python3 line of the test above.
	const encoded = "ceircejceiztgrcekvkvkvkvku"
	if instanceCopy != "bk"+encoded || volumeCopy != "bkv"+encoded {
		t.Fatalf("copies: %s %s", instanceCopy, volumeCopy)
	}
	if _, err := BackupInstanceCopyName("not-an-id"); err == nil {
		t.Fatal("accepted a name that is not a Titan id")
	}
}

func TestObjectNames_TheSameIdWithoutDashesOrInCapitals_IsTheSameName(t *testing.T) {
	plain, _ := VolumeName(someId)
	undashed, _ := VolumeName(strings.ReplaceAll(someId, "-", ""))
	capitals, _ := VolumeName(strings.ToUpper(someId))
	if undashed != plain || capitals != plain {
		t.Fatalf("%q %q %q differ", plain, undashed, capitals)
	}
}

func TestObjectNames_NotAUuid_IsAnError(t *testing.T) {
	for _, id := range []string{"", "vm-1", "11111111-2222-3333-4444-55555555555", "zz111111-2222-3333-4444-555555555555"} {
		_, err := InstanceName(id)
		if err == nil {
			t.Errorf("%q: want an error", id)
		}
	}
}

// The longest LV Titan's names make: an instance snapshot's root disk. The name format is the one lvcreate printed in
// lab-storage 05b ("virtual-machines_<project>_<instance>-<snapshot>.block" and "custom_<project>_<volume>-<snapshot>.block",
// every "-" inside the three names doubled).
func TestLvNames_OfTitansSnapshots_FitTheLvmLimit(t *testing.T) {
	instance, _ := InstanceName(someId)
	volume, _ := VolumeName(someId)
	snapshot, _ := SnapshotName(someId)
	vmSnapshotLv := "virtual-machines_" + lvEscape(someProject) + "_" + lvEscape(instance) + "-" + lvEscape(snapshot) + ".block"
	volumeSnapshotLv := "custom_" + lvEscape(someProject) + "_" + lvEscape(volume) + "-" + lvEscape(snapshot) + ".block"
	for _, lv := range []string{vmSnapshotLv, volumeSnapshotLv} {
		if len(lv) > longestLvName {
			t.Errorf("%s is %d characters; LVM refuses past %d", lv, len(lv), longestLvName)
		}
	}
}

func lvEscape(name string) string {
	return strings.ReplaceAll(name, "-", "--")
}

func TestSnapshotId_RoundTrips_AndRefusesAnythingElse(t *testing.T) {
	parent, snapshot, err := SplitSnapshotId(SnapshotId("volabc", "sdef"))
	if err != nil || parent != "volabc" || snapshot != "sdef" {
		t.Fatalf("got %q %q %v", parent, snapshot, err)
	}
	for _, id := range []string{"", "volabc", "/sdef", "volabc/"} {
		_, _, err := SplitSnapshotId(id)
		if err == nil {
			t.Errorf("%q: want an error", id)
		}
	}
}
