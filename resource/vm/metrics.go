package vm

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"regexp"
	"sort"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// The source of a VM's numbers: the agent inside the guest, or QEMU from outside it.
const (
	sourceAgent = "agent"
	sourceQemu  = "qemu"
)

// wholeDisk names a guest's whole disks: a partition, a loop device or the firmware (/machine/*) would count the same
// bytes twice or count bytes that are not the tenant's (titan-ops features/vm/design.md#vm-metrics).
var wholeDisk = regexp.MustCompile(`^((sd|vd|xvd)[a-z]+|nvme[0-9]+n[0-9]+|dev-incus_.+)$`)

// metricsBatch is the body of POST /internal/clusters/{clusterId}/vm-metrics: one per cluster and cycle.
type metricsBatch struct {
	SampledAt time.Time   `json:"sampledAt"`
	Vms       []vmMetrics `json:"vms"`
}

// vmMetrics carries the counters as Incus totals them, never rates: the control plane divides by the time between two
// samples. A nil field is sent as null, Incus having no number for it.
type vmMetrics struct {
	ExternalId           string  `json:"externalId"`
	Source               string  `json:"source"`
	CpuSeconds           float64 `json:"cpuSeconds"`
	MemoryTotalBytes     *int64  `json:"memoryTotalBytes"`
	MemoryAvailableBytes *int64  `json:"memoryAvailableBytes"`
	RootFsSizeBytes      *int64  `json:"rootFsSizeBytes"`
	RootFsAvailBytes     *int64  `json:"rootFsAvailBytes"`
	DiskReadBytes        int64   `json:"diskReadBytes"`
	DiskWrittenBytes     int64   `json:"diskWrittenBytes"`
}

// vmSeries gathers one VM's series while the text is read.
type vmSeries struct {
	hasCpu        bool
	hasFilesystem bool
	metrics       vmMetrics
	diskRead      float64
	diskWritten   float64
}

// Metrics reads each cluster's VM usage and sends it to the control plane. It changes no resource's state.
type Metrics struct {
	now func() time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{now: time.Now}
}

// Reconcile reads /1.0/metrics of the cluster once and posts one batch. A failed read sends nothing: the next cycle
// reads again.
func (m *Metrics) Reconcile(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, logger *slog.Logger) error {
	sampledAt := m.now().UTC().Truncate(time.Second)
	text, err := p.GetMetrics(ctx)
	if err != nil {
		return fmt.Errorf("read metrics of cluster %s: %w", clusterId, err)
	}
	samples, err := provider.ParseMetrics(text)
	if err != nil {
		return fmt.Errorf("read metrics of cluster %s: %w", clusterId, err)
	}
	vms := aggregateMetrics(samples)
	if len(vms) == 0 {
		return nil
	}

	path := "/internal/clusters/" + url.PathEscape(clusterId) + "/vm-metrics"
	if err := cp.Post(ctx, path, metricsBatch{SampledAt: sampledAt, Vms: vms}); err != nil {
		return fmt.Errorf("send metrics of cluster %s: %w", clusterId, err)
	}
	logger.Info("vm metrics sent", "vms", len(vms))
	return nil
}

// aggregateMetrics sums each Titan VM's series into one row, in externalId order. A VM without CPU time is left out.
func aggregateMetrics(samples []provider.Sample) []vmMetrics {
	byName := map[string]*vmSeries{}
	for _, sample := range samples {
		name := sample.Labels["name"]
		isTitanVm := sample.Labels["type"] == "virtual-machine" && provider.IsInstanceName(name)
		isNumber := !math.IsNaN(sample.Value) && !math.IsInf(sample.Value, 0)
		if !isTitanVm || !isNumber {
			continue
		}
		series, found := byName[name]
		if !found {
			series = &vmSeries{metrics: vmMetrics{ExternalId: name}}
			byName[name] = series
		}
		addSample(series, sample)
	}

	vms := []vmMetrics{}
	for _, series := range byName {
		if !series.hasCpu {
			continue
		}
		vm := series.metrics
		vm.Source = sourceQemu
		if series.hasFilesystem {
			vm.Source = sourceAgent
		}
		vm.DiskReadBytes = int64(math.Round(series.diskRead))
		vm.DiskWrittenBytes = int64(math.Round(series.diskWritten))
		vms = append(vms, vm)
	}
	sort.Slice(vms, func(i, j int) bool { return vms[i].ExternalId < vms[j].ExternalId })
	return vms
}

func addSample(series *vmSeries, sample provider.Sample) {
	isRootFs := sample.Labels["mountpoint"] == "/"
	switch sample.Name {
	case "incus_cpu_seconds_total":
		series.hasCpu = true
		mode := sample.Labels["mode"]
		isBusy := mode != "idle" && mode != "iowait"
		if isBusy {
			series.metrics.CpuSeconds += sample.Value
		}
	case "incus_memory_MemTotal_bytes":
		series.metrics.MemoryTotalBytes = bytesOf(sample.Value)
	case "incus_memory_MemAvailable_bytes":
		// 0 is read as no number, not as a guest without free memory (titan-ops features/vm/design.md#vm-metrics).
		if sample.Value != 0 {
			series.metrics.MemoryAvailableBytes = bytesOf(sample.Value)
		}
	case "incus_filesystem_size_bytes":
		// Only the agent reads filesystems: any of them, even one of size 0, says the agent answered.
		series.hasFilesystem = true
		if isRootFs {
			series.metrics.RootFsSizeBytes = bytesOf(sample.Value)
		}
	case "incus_filesystem_avail_bytes":
		if isRootFs {
			series.metrics.RootFsAvailBytes = bytesOf(sample.Value)
		}
	case "incus_disk_read_bytes_total":
		if wholeDisk.MatchString(sample.Labels["device"]) {
			series.diskRead += sample.Value
		}
	case "incus_disk_written_bytes_total":
		if wholeDisk.MatchString(sample.Labels["device"]) {
			series.diskWritten += sample.Value
		}
	}
}

// bytesOf turns a byte count Incus prints as a float (1.6e+08) back into the integer it is.
func bytesOf(value float64) *int64 {
	bytes := int64(math.Round(value))
	return &bytes
}
