package provider

import (
	"os"
	"strings"
	"testing"
)

func TestParseMetrics_ReadsEscapedLabels_SkipsComments_AndExponentNumbers(t *testing.T) {
	text := strings.Join([]string{
		"# HELP incus_disk_read_bytes_total The total number of bytes read.",
		"# TYPE incus_disk_read_bytes_total counter",
		`incus_disk_read_bytes_total{device="a \"quoted\", b\\c}",name="vm1",note="line\nbreak"} 1.6e+08`,
		"",
		"incus_uptime_seconds 84842.957873688",
		`incus_cpu_seconds_total{cpu="0",mode="user"} 170.17 1731247200000`,
		"# EOF",
	}, "\n")

	samples, err := ParseMetrics(text)

	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 3 {
		t.Fatalf("want 3 samples, got %d: %+v", len(samples), samples)
	}
	disk := samples[0]
	if disk.Name != "incus_disk_read_bytes_total" || disk.Value != 160000000 {
		t.Fatalf("disk sample %+v", disk)
	}
	if disk.Labels["device"] != `a "quoted", b\c}` || disk.Labels["name"] != "vm1" || disk.Labels["note"] != "line\nbreak" {
		t.Fatalf("labels %q", disk.Labels)
	}
	uptime := samples[1]
	if uptime.Name != "incus_uptime_seconds" || uptime.Value != 84842.957873688 || len(uptime.Labels) != 0 {
		t.Fatalf("uptime sample %+v", uptime)
	}
	cpu := samples[2]
	if cpu.Value != 170.17 || cpu.Labels["mode"] != "user" {
		t.Fatalf("a timestamp after the value must not change it: %+v", cpu)
	}
}

func TestParseMetrics_MalformedLine_IsAnErrorNamingTheLine(t *testing.T) {
	for _, line := range []string{
		`incus_x{name="vm1" 1`,
		`incus_x{name="vm1} 1`,
		`incus_x{name=vm1} 1`,
		`incus_x{name="vm1"}`,
		`incus_x{name="vm1"} many`,
	} {
		_, err := ParseMetrics("# ok\n" + line + "\n")

		if err == nil || !strings.Contains(err.Error(), "line 2") {
			t.Errorf("%s: want an error naming line 2, got %v", line, err)
		}
	}
}

// Oracle: the lab recording of an Incus 7.0.2 server's /1.0/metrics; grep -cv '^#' gives the sample count.
func TestParseMetrics_TheLabRecording(t *testing.T) {
	text, err := os.ReadFile("testdata/lab-metrics-2026-11-10.txt")
	if err != nil {
		t.Fatal(err)
	}

	samples, err := ParseMetrics(string(text))

	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 217 {
		t.Fatalf("want 217 samples, got %d", len(samples))
	}
}
