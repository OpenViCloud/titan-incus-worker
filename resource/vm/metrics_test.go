package vm

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	labMetrics   = "../../incus/provider/testdata/lab-metrics-2026-11-10.txt"
	agentVm      = "vm7atdlelbvjafpojgfreep4u23i"
	noAgentVm    = "vmlabmetricsnoagent222222222"
	metricsRoute = "POST /internal/clusters/c1/vm-metrics"
)

var sampledAt = time.Date(2026, 11, 10, 14, 0, 0, 0, time.UTC)

// sentVm decodes one VM of the batch with every field, so a null and a missing key read apart.
type sentVm map[string]any

func readLabMetrics(t *testing.T) string {
	t.Helper()
	text, err := os.ReadFile(labMetrics)
	if err != nil {
		t.Fatal(err)
	}
	return string(text)
}

func metricsAt(at time.Time) *Metrics {
	metrics := NewMetrics()
	metrics.now = func() time.Time { return at }
	return metrics
}

// sendMetrics runs one metrics cycle of cluster c1 and returns what reached the control plane, keyed by externalId.
func sendMetrics(t *testing.T, text string) (map[string]sentVm, *testsupport.ControlPlane) {
	t.Helper()
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Metrics = text

	err := metricsAt(sampledAt).Reconcile(context.Background(), cp.Client(), p, "c1", testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	posts := cp.RequestsTo(metricsRoute)
	if len(posts) == 0 {
		return map[string]sentVm{}, cp
	}
	if len(posts) != 1 || len(cp.Requests()) != 1 {
		t.Fatalf("want one POST and nothing else, got %+v", cp.Requests())
	}
	var batch struct {
		SampledAt string   `json:"sampledAt"`
		Vms       []sentVm `json:"vms"`
	}
	if err := json.Unmarshal([]byte(posts[0].Body), &batch); err != nil {
		t.Fatal(err)
	}
	if batch.SampledAt != "2026-11-10T14:00:00Z" {
		t.Fatalf("sampledAt %q", batch.SampledAt)
	}
	byExternalId := map[string]sentVm{}
	for _, vm := range batch.Vms {
		byExternalId[vm["externalId"].(string)] = vm
	}
	return byExternalId, cp
}

func assertVm(t *testing.T, vm sentVm, want map[string]any) {
	t.Helper()
	if len(vm) != len(want) {
		t.Errorf("%v: want exactly the fields %v", vm, want)
	}
	for field, wanted := range want {
		got, present := vm[field]
		if !present {
			t.Errorf("%s: missing (a null must be sent as null)", field)
			continue
		}
		wantedNumber, isNumber := wanted.(float64)
		if isNumber {
			gotNumber, gotIsNumber := got.(float64)
			if !gotIsNumber || math.Abs(gotNumber-wantedNumber) > 1e-9 {
				t.Errorf("%s: got %v, want %v", field, got, wanted)
			}
			continue
		}
		if got != wanted {
			t.Errorf("%s: got %v, want %v", field, got, wanted)
		}
	}
}

// Oracle: the lab recording. cpu 0 of the agent VM: nice 1.4 + softirq 56.1 + steal 18.24 + system 241.16 + user 170.17
// + irq 0 (idle 84283.85 and iowait 9.17 left out). Disk: sda only (sda1, sda15, loop0-7 left out). Root filesystem:
// mountpoint "/" (/dev/sda1).
func TestMetrics_AgentVm_FromTheLabRecording(t *testing.T) {
	sent, _ := sendMetrics(t, readLabMetrics(t))

	assertVm(t, sent[agentVm], map[string]any{
		"externalId":           agentVm,
		"source":               "agent",
		"cpuSeconds":           487.07,
		"memoryTotalBytes":     2059284480.0,
		"memoryAvailableBytes": 1780793344.0,
		"rootFsSizeBytes":      10222313472.0,
		"rootFsAvailBytes":     8431116288.0,
		"diskReadBytes":        443663360.0,
		"diskWrittenBytes":     286373376.0,
	})
}

// Oracle: the lab recording. Without the agent only "system" carries time; the disk is dev-incus_root (the two
// /machine/virt.flash firmware devices left out); no filesystem series, so no root filesystem; MemAvailable is there.
func TestMetrics_VmWithoutTheAgent_FromTheLabRecording(t *testing.T) {
	sent, _ := sendMetrics(t, readLabMetrics(t))

	assertVm(t, sent[noAgentVm], map[string]any{
		"externalId":           noAgentVm,
		"source":               "qemu",
		"cpuSeconds":           53.72 + 48.01,
		"memoryTotalBytes":     1073741824.0,
		"memoryAvailableBytes": 488549824.0,
		"rootFsSizeBytes":      nil,
		"rootFsAvailBytes":     nil,
		"diskReadBytes":        160879104.0,
		"diskWrittenBytes":     277465600.0,
	})
	if len(sent) != 2 {
		t.Fatalf("want the two lab VMs, got %d", len(sent))
	}
}

func TestMetrics_OnlyTitanVirtualMachines_AreSent(t *testing.T) {
	text := strings.Join([]string{
		`incus_cpu_seconds_total{cpu="0",mode="user",name="vm7atdlelbvjafpojgfreep4u23i",project="t-1",type="container"} 5`,
		`incus_cpu_seconds_total{cpu="0",mode="user",name="lab-vm",project="lab",type="virtual-machine"} 5`,
		`incus_cpu_seconds_total{cpu="0",mode="user",name="vmaaaaaaaaaaaaaaaaaaaaaaaaaa",project="t-1",type="virtual-machine"} 5`,
	}, "\n")

	sent, _ := sendMetrics(t, text)

	if len(sent) != 1 || sent["vmaaaaaaaaaaaaaaaaaaaaaaaaaa"] == nil {
		t.Fatalf("want only the Titan VM, got %v", sent)
	}
}

func TestMetrics_WholeDisksOnly_AndAvailableMemoryOfZeroIsNull(t *testing.T) {
	const vm = `name="vmaaaaaaaaaaaaaaaaaaaaaaaaaa",type="virtual-machine"`
	lines := []string{
		`incus_cpu_seconds_total{cpu="0",mode="user",` + vm + `} 1`,
		`incus_memory_MemTotal_bytes{` + vm + `} 1000`,
		`incus_memory_MemAvailable_bytes{` + vm + `} 0`,
	}
	read := map[string]string{
		"sda": "1", "sdb": "2", "vda": "4", "xvda": "8", "nvme0n1": "16", "nvme10n2": "32", "dev-incus_data": "64",
		"sda1": "1000", "vda2": "1000", "xvda3": "1000", "nvme0n1p1": "1000", "nvme0": "1000", "loop0": "1000",
		"/machine/virt.flash0": "1000", "dm-0": "1000", "sr0": "1000",
	}
	for device, bytes := range read {
		lines = append(lines, `incus_disk_read_bytes_total{device="`+device+`",`+vm+`} `+bytes)
	}

	sent, _ := sendMetrics(t, strings.Join(lines, "\n"))

	got := sent["vmaaaaaaaaaaaaaaaaaaaaaaaaaa"]
	if got["diskReadBytes"] != 127.0 || got["diskWrittenBytes"] != 0.0 {
		t.Fatalf("disk read %v written %v, want 127 and 0", got["diskReadBytes"], got["diskWrittenBytes"])
	}
	if got["memoryTotalBytes"] != 1000.0 || got["memoryAvailableBytes"] != nil || got["source"] != "qemu" {
		t.Fatalf("got %v", got)
	}
}

func TestMetrics_VmWithoutCpuSeconds_IsNotSent(t *testing.T) {
	text := `incus_memory_MemTotal_bytes{name="vmaaaaaaaaaaaaaaaaaaaaaaaaaa",type="virtual-machine"} 1000`

	sent, cp := sendMetrics(t, text)

	if len(sent) != 0 || len(cp.Requests()) != 0 {
		t.Fatalf("want no call, got %+v", cp.Requests())
	}
}

func TestMetrics_NoVm_CallsNothing(t *testing.T) {
	_, cp := sendMetrics(t, "# EOF\nincus_uptime_seconds 1\n")

	if len(cp.Requests()) != 0 {
		t.Fatalf("want no call, got %+v", cp.Requests())
	}
}

func TestMetrics_ReadFails_ReturnsTheError_AndCallsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.GetMetricsErr = errors.New("metrics refused")

	err := metricsAt(sampledAt).Reconcile(context.Background(), cp.Client(), p, "c1", testsupport.DiscardLogger())

	if err == nil || !strings.Contains(err.Error(), "metrics refused") {
		t.Fatalf("want the read error, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("want no call, got %+v", cp.Requests())
	}
}

func TestMetrics_RefusedBatch_ReturnsTheError(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond("POST", "/internal/clusters/c1/vm-metrics", 400, `{"code":"BAD"}`)
	p := testsupport.NewProvider()
	p.Metrics = readLabMetrics(t)

	err := metricsAt(sampledAt).Reconcile(context.Background(), cp.Client(), p, "c1", testsupport.DiscardLogger())

	if err == nil {
		t.Fatal("a refused batch must fail the cycle")
	}
}

// One line per batch with the count: the payload stays out of the log.
func TestMetrics_LogsTheCount_NotThePayload(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Metrics = readLabMetrics(t)
	logger, logs := testsupport.NewLogCapture(t)

	err := metricsAt(sampledAt).Reconcile(context.Background(), cp.Client(), p, "c1", logger)

	if err != nil {
		t.Fatal(err)
	}
	line := logs.One("vm metrics sent")
	if line["vms"] != 2.0 {
		t.Fatalf("log line %v, want vms=2", line)
	}
	if len(line) != 4 {
		t.Fatalf("log line %v carries more than time, level, msg and vms", line)
	}
}

// The batch is stamped in UTC at the second, the shape the contract shows, whatever the host's time zone.
func TestMetrics_SampledAt_IsUtcAtTheSecond(t *testing.T) {
	hanoi := time.FixedZone("ICT", 7*60*60)
	readAt := time.Date(2026, 11, 10, 21, 0, 0, 999_999_999, hanoi)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Metrics = readLabMetrics(t)

	err := metricsAt(readAt).Reconcile(context.Background(), cp.Client(), p, "c1", testsupport.DiscardLogger())

	posts := cp.RequestsTo(metricsRoute)
	if err != nil || len(posts) != 1 || !strings.HasPrefix(posts[0].Body, `{"sampledAt":"2026-11-10T14:00:00Z",`) {
		t.Fatalf("err %v, posts %+v", err, posts)
	}
}

func TestNewMetrics_ReadsTheClock(t *testing.T) {
	before := time.Now()

	at := NewMetrics().now()

	if at.Before(before) || time.Since(at) > time.Minute {
		t.Fatalf("now %v", at)
	}
}
