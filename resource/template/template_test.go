package template

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	templateId  = "11111111-2222-3333-4444-555555555555"
	clusterId   = "mock"
	imageServer = "https://images.example.test"
)

func syncLeased() command.Leased {
	encoded, _ := json.Marshal(map[string]any{
		"templateId": templateId, "name": "Alma-9-DB", "description": "",
		"osImageName": "almalinux/9", "rootDiskSizeGb": nil,
	})
	return command.Leased{Kind: "template.sync", ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func onlyBody(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	requests := cp.RequestsTo("PATCH /internal/templates/" + templateId + "/status")
	if len(requests) != 1 {
		t.Fatalf("want one status report, got %+v", cp.Requests())
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(requests[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestSync_AliasPresent_DoesNotPull_AndReportsCreated(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Aliases["tpl-"+templateId] = true

	err := NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).Run(context.Background(), syncLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedImages) != 0 {
		t.Fatal("pulled an image the cluster has")
	}
	body := onlyBody(t, cp)
	if body["report"] != "Created" || body["clusterId"] != clusterId || body["externalId"] != "tpl-"+templateId {
		t.Fatalf("report: %v", body)
	}
}

func TestSync_AliasMissing_PullsTheVmImageUnderTheAlias(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).Run(context.Background(), syncLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	pulled := p.CreatedImages[0]
	source := pulled.Source
	isPull := source.Type == "image" && source.Mode == "pull" && source.Protocol == "simplestreams"
	if !isPull || source.Server != imageServer || source.Alias != "almalinux/9" || source.ImageType != "virtual-machine" {
		t.Fatalf("source: %+v", source)
	}
	if len(pulled.Aliases) != 1 || pulled.Aliases[0].Name != "tpl-"+templateId {
		t.Fatalf("aliases: %+v", pulled.Aliases)
	}
	if onlyBody(t, cp)["report"] != "Created" {
		t.Fatal("want Created")
	}
}

func TestSync_AliasCreatedByAnotherRun_IsCreated(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateImageErr = &provider.OperationFailedError{Status: "Failure", Message: `Alias "tpl-` + templateId + `" already exists`}

	err := NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).Run(context.Background(), syncLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
}

func TestSync_ImageOfAnotherArchitecture_IsDeletedAndFailsForGood(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"aarch64", "armv6l", "armv7l", "armv8l"}
	p.ImageArchitectures["fp-tpl-"+templateId] = "x86_64"

	err := NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).Run(context.Background(), syncLeased(), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent failure, got %v", err)
	}
	if !strings.Contains(err.Error(), "x86_64") || !strings.Contains(err.Error(), "aarch64") {
		t.Fatalf("the reason must name both architectures: %v", err)
	}
	if len(p.DeletedImages) != 1 || p.DeletedImages[0] != "fp-tpl-"+templateId {
		t.Fatalf("deleted images: %v — a wrong-architecture image left under the alias reads as present to the reconciler", p.DeletedImages)
	}
	if len(cp.RequestsTo("PATCH /internal/templates/"+templateId+"/status")) != 0 {
		t.Fatal("reported a status: the give-up callback reports the failure, not Run")
	}
}

func TestSync_DeleteOfAWrongArchitectureImageFails_IsRetriedWithTheMismatchAsReason(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"aarch64"}
	p.ImageArchitectures["fp-tpl-"+templateId] = "x86_64"
	p.DeleteImageErr = errors.New("connection reset")

	err := NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).Run(context.Background(), syncLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want a retryable failure, got %v", err)
	}
	if !strings.Contains(err.Error(), "x86_64") || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("the reason the give-up callback reports must name the mismatch and the delete error: %v", err)
	}
}

func TestSync_AliasPresentOnAnotherArchitecture_IsCheckedToo(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Aliases["tpl-"+templateId] = true
	p.ServerArchitectures = []string{"aarch64"}
	p.ImageArchitectures["fp-tpl-"+templateId] = "x86_64"

	err := NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).Run(context.Background(), syncLeased(), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent failure, got %v", err)
	}
	if len(p.CreatedImages) != 0 {
		t.Fatal("pulled again: the image server has no other architecture to offer")
	}
}

func TestSync_ImageOfTheClusterArchitecture_IsCreated(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"aarch64", "armv6l", "armv7l", "armv8l"}
	p.ImageArchitectures["fp-tpl-"+templateId] = "aarch64"

	err := NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).Run(context.Background(), syncLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.DeletedImages) != 0 {
		t.Fatalf("deleted %v", p.DeletedImages)
	}
	if onlyBody(t, cp)["report"] != "Created" {
		t.Fatal("want Created")
	}
}

func TestSync_GiveUp_ReportsRequestFailedWithItsCluster_AFixedReason_AndTheRawError(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewSync(cp.Client(), cluster.Fixed{}, imageServer).GiveUp(context.Background(), syncLeased(), errors.New("pull timed out"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp)
	isRequestFailed := body["report"] == "RequestFailed" && body["clusterId"] == clusterId
	if !isRequestFailed || body["reason"] != reasonSyncFailed || body["detail"] != "pull timed out" {
		t.Fatalf("report: %v", body)
	}
	if _, hasExists := body["exists"]; hasExists {
		t.Fatalf("a give-up says nothing of existence: %v", body)
	}
}

// The reason tells the operator which of the causes they can act on it was: a cluster of another architecture, or a
// cluster that is not configured (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/catalog/design.md#template-reports).
func TestSync_GiveUp_NamesTheCauseInTheReason(t *testing.T) {
	cases := []struct {
		name  string
		cause func() error
		want  string
	}{
		{"wrong architecture", func() error {
			p := testsupport.NewProvider()
			p.ServerArchitectures = []string{"aarch64"}
			p.ImageArchitectures["fp-tpl-"+templateId] = "x86_64"
			cp := testsupport.NewControlPlane(t)
			return NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).Run(context.Background(), syncLeased(), testsupport.DiscardLogger())
		}, reasonWrongArchitecture},
		{"no cluster", func() error {
			cp := testsupport.NewControlPlane(t)
			return NewSync(cp.Client(), cluster.Fixed{}, imageServer).Run(context.Background(), syncLeased(), testsupport.DiscardLogger())
		}, reasonNoCluster},
		{"anything else", func() error { return errors.New("connection reset") }, reasonSyncFailed},
	}
	for _, c := range cases {
		cause := c.cause()
		cp := testsupport.NewControlPlane(t)

		err := NewSync(cp.Client(), cluster.Fixed{}, imageServer).GiveUp(context.Background(), syncLeased(), cause, testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		body := onlyBody(t, cp)
		if body["reason"] != c.want || body["detail"] != cause.Error() {
			t.Errorf("%s: report %v", c.name, body)
		}
	}
}

func TestSync_GiveUp_CutsALongErrorAtTheDetailLimit(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	long := strings.Repeat("é", maxDetail+10)

	err := NewSync(cp.Client(), cluster.Fixed{}, imageServer).GiveUp(context.Background(), syncLeased(), errors.New(long), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	detail, _ := onlyBody(t, cp)["detail"].(string)
	if len([]rune(detail)) != maxDetail {
		t.Fatalf("detail is %d runes, want %d", len([]rune(detail)), maxDetail)
	}
}

func TestDecideReport_SendsOnlyChanges(t *testing.T) {
	present := map[string]bool{"tpl-a": true}
	cases := []struct {
		row        ReconcilableTemplate
		wantExists bool
		send       bool
	}{
		{ReconcilableTemplate{ExternalTemplateId: "tpl-a", CurrentExistence: "Exists"}, true, false},
		{ReconcilableTemplate{ExternalTemplateId: "tpl-a", CurrentExistence: "NotFound"}, true, true},
		{ReconcilableTemplate{ExternalTemplateId: "tpl-a", CurrentExistence: "NotCreated"}, true, true},
		{ReconcilableTemplate{ExternalTemplateId: "tpl-b", CurrentExistence: "Exists"}, false, true},
		{ReconcilableTemplate{ExternalTemplateId: "tpl-b", CurrentExistence: "NotCreated"}, false, true},
		{ReconcilableTemplate{ExternalTemplateId: "tpl-b", CurrentExistence: "NotFound"}, false, false},
	}
	for _, c := range cases {
		exists, send := DecideReport(c.row, present)
		if send != c.send || exists != c.wantExists {
			t.Errorf("%+v: want exists=%v send=%v, got exists=%v send=%v", c.row, c.wantExists, c.send, exists, send)
		}
	}
}

func TestReconcile_ReportsAnAliasGoneWithTheCluster(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/templates?clusterId=mock", http.StatusOK,
		`[{"id":"`+templateId+`","externalTemplateId":"tpl-gone","currentExistence":"Exists"}]`)
	logger, logs := testsupport.NewLogCapture(t)

	err := Reconcile(context.Background(), cp.Client(), testsupport.NewProvider(), "mock", logger)

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp)
	if body["report"] != "Observed" || body["exists"] != false || body["clusterId"] != "mock" {
		t.Fatalf("report: %v", body)
	}
	sent := logs.One("reconcile report sent")
	if sent["templateId"] != templateId || sent["recordedExistence"] != "Exists" || sent["exists"] != false {
		t.Fatalf("report line: %v", sent)
	}
}

// The image-already-there branch is the one a resend takes; without a line the log shows a lease that did nothing.
func TestSync_AliasPresent_SaysSo_InTheLog(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Aliases["tpl-"+templateId] = true
	logger, logs := testsupport.NewLogCapture(t)

	err := NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).Run(context.Background(), syncLeased(), logger)

	if err != nil {
		t.Fatal(err)
	}
	line := logs.One("template image already present")
	if line["alias"] != "tpl-"+templateId {
		t.Fatalf("log line: %v", line)
	}
}
