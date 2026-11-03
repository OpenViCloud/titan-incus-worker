package template

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A template.sync that carries a requestId ends that request on the route of its template in its cluster
// (titan-ops architecture/resource-lifecycle.md#callback-routes): Done carrying the alias, or Failed with a fixed
// reason, the raw error cut as detail, and the alias only when Incus has it on the cluster's architecture. The tests
// named WithoutARequest here, and template_test.go, keep the commands without one on the old route.

const requestId = "77777777-6666-5555-4444-333333333333"

const (
	alias            = "tpl-" + templateId
	statusRoute      = "PATCH /internal/templates/" + templateId + "/status"
	templateInIt     = "PATCH /internal/templates/" + templateId + "/clusters/" + clusterId
	requestRoute     = templateInIt + "/requests/" + requestId
	anyRequestRoute  = templateInIt + "/requests/"
	observationRoute = templateInIt + "/observation"
)

func withRequest(l command.Leased) command.Leased {
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	payload["requestId"] = requestId
	l.Payload, _ = json.Marshal(payload)
	return l
}

func syncWith(cp *testsupport.ControlPlane, p *testsupport.Provider) *Sync {
	return NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer)
}

func runSync(h *Sync, l command.Leased) error {
	return h.Run(context.Background(), l, testsupport.DiscardLogger())
}

func giveUpSync(h *Sync, l command.Leased, cause error) error {
	return h.GiveUp(context.Background(), l, cause, testsupport.DiscardLogger())
}

func withAlias(p *testsupport.Provider) *testsupport.Provider {
	p.Aliases[alias] = true
	return p
}

func onlyBodyAt(t *testing.T, cp *testsupport.ControlPlane, prefix string) map[string]any {
	t.Helper()
	requests := cp.RequestsTo(prefix)
	if len(requests) != 1 {
		t.Fatalf("want one %s, got %d: %+v", prefix, len(requests), cp.Requests())
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(requests[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

// requestEnd reads the one report on the request route, and checks nothing went to the old one.
func requestEnd(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	if len(cp.RequestsTo(statusRoute)) != 0 {
		t.Fatalf("a command with a requestId used the old route: %+v", cp.Requests())
	}
	if len(cp.Requests()) != 1 {
		t.Fatalf("want one report, got %+v", cp.Requests())
	}
	return onlyBodyAt(t, cp, requestRoute)
}

func isDoneWithTheAlias(body map[string]any) bool {
	return body["outcome"] == "Done" && body["externalId"] == alias && len(body) == 2
}

// isFailed is a Failed with the given reason and detail, carrying the alias only when wantAlias.
func isFailed(body map[string]any, reason string, detail string, wantAlias bool) bool {
	gotId, hasId := body["externalId"]
	hasRightId := (wantAlias && gotId == alias) || (!wantAlias && !hasId)
	return body["outcome"] == "Failed" && body["reason"] == reason && body["detail"] == detail && hasRightId
}

func TestSync_WithARequest_PullsTheImage_AndEndsItDone_CarryingTheAlias(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := runSync(syncWith(cp, p), withRequest(syncLeased()))

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedImages) != 1 || p.CreatedImages[0].Aliases[0].Name != alias {
		t.Fatalf("pulled %+v", p.CreatedImages)
	}
	if body := requestEnd(t, cp); !isDoneWithTheAlias(body) {
		t.Fatalf("report %v", body)
	}
}

// A resend finds the alias its first run made: nothing is written, and the request ends Done all the same.
func TestSync_WithARequest_AliasPresent_WritesNothing_AndEndsItDone_CarryingTheAlias(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAlias(testsupport.NewProvider())

	err := runSync(syncWith(cp, p), withRequest(syncLeased()))

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedImages) != 0 || len(p.ImportedImages) != 0 || len(p.DeletedImages) != 0 {
		t.Fatalf("wrote Incus: created %+v, imported %d, deleted %v", p.CreatedImages, len(p.ImportedImages),
			p.DeletedImages)
	}
	if body := requestEnd(t, cp); !isDoneWithTheAlias(body) {
		t.Fatalf("report %v", body)
	}
}

func TestSync_WithARequest_AFailedWrite_ReportsNothing_AndReturnsTheError(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(p *testsupport.Provider)
	}{
		{"pull", func(p *testsupport.Provider) {
			p.CreateImageErr = api.StatusErrorf(http.StatusInternalServerError, "busy")
		}},
		{"wrong architecture", func(p *testsupport.Provider) {
			p.ServerArchitectures = []string{"aarch64"}
			p.ImageArchitectures["fp-"+alias] = "x86_64"
		}},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		c.prepare(p)

		err := runSync(syncWith(cp, p), withRequest(syncLeased()))

		if err == nil || len(cp.Requests()) != 0 {
			t.Errorf("%s: err %v, requests %+v", c.name, err, cp.Requests())
		}
	}
}

// The alias is the template's id on the cluster: a give-up that carries it tells the control plane the sync did
// happen, so customers see the template there (titan-ops features/catalog/design.md#template-reports).
func TestSync_WithARequest_GiveUp_AliasOfTheClusterArchitecture_EndsItFailed_CarryingTheAlias(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAlias(testsupport.NewProvider())

	err := giveUpSync(syncWith(cp, p), withRequest(syncLeased()), errors.New("report lost"))

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); !isFailed(body, reasonSyncFailed, "report lost", true) {
		t.Fatalf("report %v", body)
	}
}

func TestSync_WithARequest_GiveUp_AliasAbsent_EndsItFailed_WithoutAnId(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := giveUpSync(syncWith(cp, testsupport.NewProvider()), withRequest(syncLeased()), errors.New("pull timed out"))

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); !isFailed(body, reasonSyncFailed, "pull timed out", false) {
		t.Fatalf("report %v", body)
	}
}

// An image no VM of the cluster can start is no template there. A give-up writes nothing: the image stays for the
// next sync's Run to delete.
func TestSync_WithARequest_GiveUp_AliasOfAnotherArchitecture_EndsItFailed_WithoutAnId_AndDeletesNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAlias(testsupport.NewProvider())
	p.ServerArchitectures = []string{"aarch64"}
	p.ImageArchitectures["fp-"+alias] = "x86_64"
	cause := errors.New("deleting it failed: connection reset")

	err := giveUpSync(syncWith(cp, p), withRequest(syncLeased()), cause)

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); !isFailed(body, reasonSyncFailed, cause.Error(), false) {
		t.Fatalf("report %v", body)
	}
	if len(p.DeletedImages) != 0 || len(p.CreatedImages) != 0 || len(p.ImportedImages) != 0 {
		t.Fatalf("a give-up wrote Incus: deleted %v, created %+v, imported %d", p.DeletedImages, p.CreatedImages,
			len(p.ImportedImages))
	}
}

// transientClusters fails every lookup with an error worth another lease (the control plane unreachable, say).
type transientClusters struct{}

func (transientClusters) Cluster(ctx context.Context, clusterId string) (*cluster.Cluster, error) {
	return nil, errors.New("control plane unreachable")
}

// A read that fails is not "no alias": the next lease gives up again.
func TestSync_WithARequest_GiveUp_AReadThatFails_IsRetryable_AndReportsNothing(t *testing.T) {
	busy := api.StatusErrorf(http.StatusInternalServerError, "busy")
	cases := []struct {
		name     string
		clusters func(p *testsupport.Provider) cluster.Clusters
	}{
		{"cluster", func(p *testsupport.Provider) cluster.Clusters { return transientClusters{} }},
		{"alias", func(p *testsupport.Provider) cluster.Clusters { p.GetImageAliasErr = busy; return fixed(p) }},
		{"images", func(p *testsupport.Provider) cluster.Clusters { p.ListImagesErr = busy; return fixed(p) }},
		{"architectures", func(p *testsupport.Provider) cluster.Clusters {
			p.GetServerArchitecturesErr = busy
			return fixed(p)
		}},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := withAlias(testsupport.NewProvider())

		h := NewSync(cp.Client(), c.clusters(p), imageServer)

		err := giveUpSync(h, withRequest(syncLeased()), errors.New("raw"))

		if err == nil || !command.IsRetryable(err) {
			t.Errorf("%s: want retryable, got %v", c.name, err)
		}
		if len(cp.Requests()) != 0 {
			t.Errorf("%s: reported %+v", c.name, cp.Requests())
		}
	}
}

func fixed(p *testsupport.Provider) cluster.Clusters {
	return cluster.Fixed{clusterId: {Provider: p}}
}

// A cluster the worker cannot reach for good says nothing about the alias.
func TestSync_WithARequest_GiveUp_OnAClusterTheWorkerIsNotConfiguredFor_EndsItFailed_WithoutAnId(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cause := cluster.ErrNoCluster

	err := giveUpSync(NewSync(cp.Client(), cluster.Fixed{}, imageServer), withRequest(syncLeased()), cause)

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); !isFailed(body, reasonNoCluster, cause.Error(), false) {
		t.Fatalf("report %v", body)
	}
}

func TestSync_WithARequest_GiveUp_NamesTheCauseInTheReason(t *testing.T) {
	cases := []struct {
		cause error
		want  string
	}{
		{command.Permanent(fmt.Errorf("%w: x86_64 on aarch64", errWrongArchitecture)), reasonWrongArchitecture},
		{fmt.Errorf("lookup: %w", cluster.ErrNoCluster), reasonNoCluster},
		{errors.New("connection reset"), reasonSyncFailed},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)

		err := giveUpSync(syncWith(cp, testsupport.NewProvider()), withRequest(syncLeased()), c.cause)

		if err != nil {
			t.Fatal(err)
		}
		if body := requestEnd(t, cp); body["reason"] != c.want {
			t.Errorf("%v: report %v", c.cause, body)
		}
	}
}

func TestSync_WithARequest_GiveUp_CutsTheDetailAt1000Runes_NotBytes(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	long := strings.Repeat("é", controlplane.MaxDetail+10)

	err := giveUpSync(syncWith(cp, withAlias(testsupport.NewProvider())), withRequest(syncLeased()), errors.New(long))

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	detail, _ := body["detail"].(string)
	if body["outcome"] != "Failed" || len([]rune(detail)) != controlplane.MaxDetail {
		t.Fatalf("outcome %v, detail of %d runes, want %d", body["outcome"], len([]rune(detail)),
			controlplane.MaxDetail)
	}
}

// A command the old control plane enqueued without a requestId keeps the old route until rollout phase 6
// (titan-ops architecture/resource-lifecycle.md#rollout); its give-up still reads nothing.
func TestSync_WithoutARequest_ReportsOnTheOldRoute_AndItsGiveUpReadsNothing(t *testing.T) {
	cases := []struct {
		name         string
		run          func(h *Sync) error
		wantReport   string
		readsNothing bool
	}{
		{"run", func(h *Sync) error { return runSync(h, syncLeased()) }, "Created", false},
		{"give-up", func(h *Sync) error { return giveUpSync(h, syncLeased(), errors.New("x")) }, "RequestFailed", true},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()

		if err := c.run(syncWith(cp, p)); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(cp.RequestsTo(anyRequestRoute)) != 0 || (c.readsNothing && p.Calls != 0) {
			t.Errorf("%s: reports %+v, Incus calls %d", c.name, cp.Requests(), p.Calls)
			continue
		}
		body := onlyBodyAt(t, cp, statusRoute)
		if body["report"] != c.wantReport || body["clusterId"] != clusterId {
			t.Errorf("%s: report %v, want %s", c.name, body, c.wantReport)
		}
	}
}

func TestReconcile_SendsOnTheObservationRoute_NeverTheOldOne(t *testing.T) {
	for _, there := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		recorded := "Exists"
		if there {
			recorded = "NotCreated"
		}
		cp.Respond(http.MethodGet, "/internal/reconcile/templates?clusterId="+clusterId, http.StatusOK,
			`[{"id":"`+templateId+`","externalTemplateId":"`+alias+`","currentExistence":"`+recorded+`"}]`)
		p := testsupport.NewProvider()
		if there {
			withAlias(p)
		}

		err := Reconcile(context.Background(), cp.Client(), p, clusterId, testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		if len(cp.RequestsTo(statusRoute)) != 0 {
			t.Fatalf("the reconciler used the old route: %+v", cp.Requests())
		}
		body := onlyBodyAt(t, cp, observationRoute)
		if body["exists"] != there || len(body) != 1 {
			t.Errorf("alias there %v: observation %v", there, body)
		}
	}
}
