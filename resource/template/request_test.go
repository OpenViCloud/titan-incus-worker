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

// A template.sync ends the request it carries on the route of its template in its cluster
// (titan-ops architecture/resource-lifecycle.md#callback-routes): Done carrying the alias, or Failed with a fixed
// reason, the raw error cut as detail, and the alias only when Incus has it on the cluster's architecture.

const requestId = "77777777-6666-5555-4444-333333333333"

const (
	alias            = "tpl-" + templateId
	templateInIt     = "PATCH /internal/templates/" + templateId + "/clusters/" + clusterId
	requestRoute     = templateInIt + "/requests/" + requestId
	observationRoute = templateInIt + "/observation"
)

// withoutRequest drops the payload's requestId, or leaves it empty when keepTheKey.
func withoutRequest(l command.Leased, keepTheKey bool) command.Leased {
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	delete(payload, "requestId")
	if keepTheKey {
		payload["requestId"] = ""
	}
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

// requestEnd reads the one report, which must be on the request route.
func requestEnd(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
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

	err := runSync(syncWith(cp, p), syncLeased())

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

// A lease after a lost Done finds the alias its first run made: nothing is written, and the request ends Done all
// the same.
func TestSync_WithARequest_AliasPresent_WritesNothing_AndEndsItDone_CarryingTheAlias(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAlias(testsupport.NewProvider())

	err := runSync(syncWith(cp, p), syncLeased())

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

		err := runSync(syncWith(cp, p), syncLeased())

		if err == nil || len(cp.Requests()) != 0 {
			t.Errorf("%s: err %v, requests %+v", c.name, err, cp.Requests())
		}
	}
}

// The alias is the template's id on the cluster: a give-up that carries it tells the control plane the sync did
// happen, so customers see the template there.
func TestSync_WithARequest_GiveUp_AliasOfTheClusterArchitecture_EndsItFailed_CarryingTheAlias(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAlias(testsupport.NewProvider())

	err := giveUpSync(syncWith(cp, p), syncLeased(), errors.New("report lost"))

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); !isFailed(body, reasonSyncFailed, "report lost", true) {
		t.Fatalf("report %v", body)
	}
}

func TestSync_WithARequest_GiveUp_AliasAbsent_EndsItFailed_WithoutAnId(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := giveUpSync(syncWith(cp, testsupport.NewProvider()), syncLeased(), errors.New("pull timed out"))

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); !isFailed(body, reasonSyncFailed, "pull timed out", false) {
		t.Fatalf("report %v", body)
	}
}

// An alias whose image the list does not show has no architecture to check: no template there.
func TestSync_WithARequest_GiveUp_AliasOfAnUnlistedImage_EndsItFailed_WithoutAnId(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAlias(testsupport.NewProvider())
	p.UnlistedImages = map[string]bool{"fp-" + alias: true}

	err := giveUpSync(syncWith(cp, p), syncLeased(), errors.New("pull timed out"))

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

	err := giveUpSync(syncWith(cp, p), syncLeased(), cause)

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

		err := giveUpSync(h, syncLeased(), errors.New("raw"))

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

	err := giveUpSync(NewSync(cp.Client(), cluster.Fixed{}, imageServer), syncLeased(), cause)

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

		err := giveUpSync(syncWith(cp, testsupport.NewProvider()), syncLeased(), c.cause)

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

	err := giveUpSync(syncWith(cp, withAlias(testsupport.NewProvider())), syncLeased(), errors.New(long))

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

// A sync without a requestId has no request to end: Run fails for good before touching Incus, and a give-up has
// nothing to say (titan-ops architecture/resource-lifecycle.md#callback-routes).
func TestSync_WithoutARequest_FailsForGood_AndCallsNothing(t *testing.T) {
	for _, keepTheKey := range []bool{false, true} {
		cp := testsupport.NewControlPlane(t)
		p := withAlias(testsupport.NewProvider())
		logger, logs := testsupport.NewLogCapture(t)
		h := syncWith(cp, p)
		l := withoutRequest(syncLeased(), keepTheKey)

		runErr := h.Run(context.Background(), l, logger)
		giveUpErr := h.GiveUp(context.Background(), l, errors.New("raw"), logger)

		if runErr == nil || command.IsRetryable(runErr) {
			t.Errorf("empty key %v: Run wants a permanent error, got %v", keepTheKey, runErr)
		}
		if giveUpErr != nil {
			t.Errorf("empty key %v: give-up: %v", keepTheKey, giveUpErr)
		}
		if len(cp.Requests()) != 0 || p.Calls != 0 {
			t.Errorf("empty key %v: reports %+v, Incus calls %d", keepTheKey, cp.Requests(), p.Calls)
		}
		if got := logs.Records("no request id; nothing to end"); len(got) != 1 {
			t.Errorf("empty key %v: want one give-up line, got %v", keepTheKey, got)
		}
	}
}

// With no request there is nothing to end, whatever else the payload lacks: the give-up says so before anything else.
func TestSync_GiveUp_WithoutARequestOrATemplate_SaysThereIsNoRequest(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	logger, logs := testsupport.NewLogCapture(t)
	l := withoutRequest(syncLeased(), false)
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	delete(payload, "templateId")
	l.Payload, _ = json.Marshal(payload)

	p := withAlias(testsupport.NewProvider())
	err := syncWith(cp, p).GiveUp(context.Background(), l, errors.New("raw"), logger)

	if err != nil || len(cp.Requests()) != 0 || p.Calls != 0 {
		t.Fatalf("give-up: %v, reports %+v, Incus calls %d", err, cp.Requests(), p.Calls)
	}
	if got := logs.Records("no request id; nothing to end"); len(got) != 1 {
		t.Fatalf("want one give-up line, got %v", got)
	}
}

func TestReconcile_SendsOnTheObservationRoute(t *testing.T) {
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
		if len(cp.RequestsTo("PATCH")) != 1 {
			t.Fatalf("want one report, got %+v", cp.Requests())
		}
		body := onlyBodyAt(t, cp, observationRoute)
		if body["exists"] != there || len(body) != 1 {
			t.Errorf("alias there %v: observation %v", there, body)
		}
	}
}
