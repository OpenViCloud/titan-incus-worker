package reconcile

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// recordingKind is a reconciler that notes each cluster it ran in, and fails when told to.
func recordingKind(name string, ranIn *[]string, err error) Kind {
	return Kind{
		Name: name,
		Reconcile: func(ctx context.Context, cp *controlplane.Client, cl *cluster.Cluster, clusterId string, logger *slog.Logger) error {
			*ranIn = append(*ranIn, name+"@"+clusterId)
			return err
		},
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestCycle_RunsEveryKindInOrder_InEveryConfiguredCluster_AndSkipsOneItCannotReach(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/clusters", http.StatusOK, `[{"id":"mock","name":"Mock"},{"id":"gone","name":"Gone"}]`)
	clusters := cluster.Fixed{"mock": {Provider: testsupport.NewProvider()}}
	ran := []string{}
	kinds := []Kind{recordingKind("first", &ran, nil), recordingKind("second", &ran, nil)}
	logger, logs := testsupport.NewLogCapture(t)
	runner := NewRunner(cp.Client(), clusters, kinds, 0, logger)

	runner.Cycle(context.Background())
	runner.Cycle(context.Background())

	want := []string{"first@mock", "second@mock", "first@mock", "second@mock"}
	if len(ran) != len(want) {
		t.Fatalf("ran %v, want %v", ran, want)
	}
	for i := range want {
		if ran[i] != want[i] {
			t.Fatalf("ran %v, want %v", ran, want)
		}
	}
	skipped := logs.Records("reconcile: cannot reach the cluster; skipping it this cycle")
	if len(skipped) != 2 || skipped[0]["clusterId"] != "gone" {
		t.Fatalf("want the unreachable cluster warned once per cycle, got %v", skipped)
	}
	if cp.RequestsTo("GET /internal/clusters")[0].Token == "" {
		t.Fatal("the cluster list was read without the internal token")
	}
}

func TestCycle_OneKindFailing_DoesNotStopTheOthers(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/clusters", http.StatusOK, `[{"id":"mock","name":"Mock"}]`)
	ran := []string{}
	kinds := []Kind{recordingKind("failing", &ran, errors.New("list failed")), recordingKind("after", &ran, nil)}
	runner := NewRunner(cp.Client(), cluster.Fixed{"mock": {Provider: testsupport.NewProvider()}}, kinds, 0, discardLogger())

	runner.Cycle(context.Background())

	ranAfter := len(ran) == 2 && ran[1] == "after@mock"
	if !ranAfter {
		t.Fatalf("a failing kind stopped the next one: ran %v", ran)
	}
}

func TestCycle_HandsEachKindALoggerWithItsKindAndCluster_AndLogsHowLongTheClusterTook(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/clusters", http.StatusOK, `[{"id":"mock","name":"Mock"}]`)
	logger, logs := testsupport.NewLogCapture(t)
	logging := func(name string) Kind {
		return Kind{
			Name: name,
			Reconcile: func(ctx context.Context, cp *controlplane.Client, cl *cluster.Cluster, clusterId string, logger *slog.Logger) error {
				logger.Info("kind ran")
				return nil
			},
		}
	}
	kinds := []Kind{logging("first"), logging("second")}
	runner := NewRunner(cp.Client(), cluster.Fixed{"mock": {Provider: testsupport.NewProvider()}}, kinds, 0, logger)

	runner.Cycle(context.Background())

	ran := logs.Records("kind ran")
	isTagged := len(ran) == 2 && ran[0]["kind"] == "first" && ran[1]["kind"] == "second" &&
		ran[0]["clusterId"] == "mock" && ran[1]["clusterId"] == "mock"
	if !isTagged {
		t.Fatalf("kind lines: %v", ran)
	}
	done := logs.One("reconcile cluster done")
	durations, isObject := done["durationsMs"].(map[string]any)
	if done["clusterId"] != "mock" || !isObject {
		t.Fatalf("cluster line: %v", done)
	}
	for _, name := range []string{"first", "second"} {
		_, isNumber := durations[name].(float64)
		if !isNumber {
			t.Errorf("durationsMs.%s = %v, want a number", name, durations[name])
		}
	}
	_, totalIsNumber := done["totalMs"].(float64)
	if !totalIsNumber {
		t.Errorf("totalMs = %v, want a number", done["totalMs"])
	}
}
