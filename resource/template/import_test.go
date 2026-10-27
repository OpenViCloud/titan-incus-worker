package template

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const imagePath = "/releases/jammy/release/ubuntu-22.04-server-cloudimg-"

// distroServer plays the distribution's download site: one qcow2 per architecture and a SHA256SUMS next to them.
type distroServer struct {
	*httptest.Server
	mu       sync.Mutex
	images   map[string][]byte // architecture as the distro names it -> bytes
	sums     string            // overrides the computed SHA256SUMS when set
	requests []string
}

func newDistroServer(t *testing.T) *distroServer {
	t.Helper()
	d := &distroServer{images: map[string][]byte{
		"amd64": bytes.Repeat([]byte("amd64-image."), 50000),
		"arm64": bytes.Repeat([]byte("arm64-image."), 40000),
	}}
	d.Server = httptest.NewServer(http.HandlerFunc(d.serve))
	t.Cleanup(d.Close)
	return d
}

func (d *distroServer) serve(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.requests = append(d.requests, r.URL.Path)
	d.mu.Unlock()
	if r.URL.Path == "/releases/jammy/release/SHA256SUMS" {
		_, _ = w.Write([]byte(d.checksums()))
		return
	}
	for arch, content := range d.images {
		if r.URL.Path == imagePath+arch+".img" {
			_, _ = w.Write(content)
			return
		}
	}
	http.NotFound(w, r)
}

func (d *distroServer) checksums() string {
	if d.sums != "" {
		return d.sums
	}
	lines := ""
	for arch, content := range d.images {
		lines += sha256Hex(content) + " *ubuntu-22.04-server-cloudimg-" + arch + ".img\n"
	}
	return lines
}

func (d *distroServer) imageUrl() string {
	return d.URL + imagePath + "{arch}.img"
}

func (d *distroServer) requested() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string{}, d.requests...)
}

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func importLeased(imageUrl string) command.Leased {
	encoded, _ := json.Marshal(map[string]any{
		"templateId": templateId, "name": "Ubuntu-22.04-Standard", "description": "",
		"osImageName": "ubuntu/22.04", "imageUrl": imageUrl, "rootDiskSizeGb": nil,
	})
	return command.Leased{Kind: "template.sync", ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func runImport(t *testing.T, p *testsupport.Provider, imageUrl string) (*testsupport.ControlPlane, error) {
	t.Helper()
	cp := testsupport.NewControlPlane(t)
	err := NewSync(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}, imageServer).
		Run(context.Background(), importLeased(imageUrl), testsupport.DiscardLogger())
	return cp, err
}

func isPermanent(err error) bool {
	permanent, ok := err.(interface{ Permanent() bool })
	return ok && permanent.Permanent()
}

func TestSync_ImageUrl_FetchesTheUrlWithTheClustersArchitecture(t *testing.T) {
	cases := map[string]string{"x86_64": "amd64", "aarch64": "arm64"}
	for clusterArchitecture, distroArchitecture := range cases {
		t.Run(clusterArchitecture, func(t *testing.T) {
			distro := newDistroServer(t)
			p := testsupport.NewProvider()
			p.ServerArchitectures = []string{clusterArchitecture}
			p.ImageArchitectures["fp-tpl-"+templateId] = clusterArchitecture

			_, err := runImport(t, p, distro.imageUrl())

			if err != nil {
				t.Fatal(err)
			}
			want := imagePath + distroArchitecture + ".img"
			if !containsString(distro.requested(), want) {
				t.Fatalf("want a request for %s, got %v", want, distro.requested())
			}
			other := imagePath + cases[otherArchitecture(clusterArchitecture)] + ".img"
			if containsString(distro.requested(), other) {
				t.Fatalf("fetched the other architecture's image: %v", distro.requested())
			}
		})
	}
}

func otherArchitecture(architecture string) string {
	if architecture == "x86_64" {
		return "aarch64"
	}
	return "x86_64"
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestSync_ImageUrl_UploadsTheBytesItDownloaded_UnderTheAlias_AndReportsCreated(t *testing.T) {
	distro := newDistroServer(t)
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"x86_64"}

	cp, err := runImport(t, p, distro.imageUrl())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedImages) != 0 {
		t.Fatalf("pulled from the image server as well: %+v", p.CreatedImages)
	}
	if len(p.ImportedImages) != 1 {
		t.Fatalf("want one import, got %d", len(p.ImportedImages))
	}
	imported := p.ImportedImages[0]
	if imported.RootfsSha256 != sha256Hex(distro.images["amd64"]) {
		t.Fatalf("uploaded bytes differ from the distro's image (%d bytes uploaded)", imported.RootfsBytes)
	}
	if len(imported.Image.Aliases) != 1 || imported.Image.Aliases[0].Name != "tpl-"+templateId {
		t.Fatalf("aliases: %+v", imported.Image.Aliases)
	}
	if !strings.Contains(imported.Metadata, "architecture: x86_64") {
		t.Fatalf("metadata.yaml in the tarball: %q", imported.Metadata)
	}
	body := onlyBody(t, cp)
	if body["report"] != "Created" || body["externalId"] != "tpl-"+templateId {
		t.Fatalf("report: %v", body)
	}
}

func TestSync_ImageUrl_ChecksumMismatch_UploadsNothing_AndIsRetried(t *testing.T) {
	distro := newDistroServer(t)
	distro.sums = strings.Repeat("0", 64) + " *ubuntu-22.04-server-cloudimg-amd64.img\n"
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"x86_64"}

	cp, err := runImport(t, p, distro.imageUrl())

	if err == nil || isPermanent(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
	if !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("error does not name the checksum: %v", err)
	}
	if len(p.ImportedImages) != 0 || p.Aliases["tpl-"+templateId] {
		t.Fatal("uploaded an image whose checksum does not match")
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported before giving up: %+v", cp.Requests())
	}
}

func TestSync_ImageUrl_ChecksumFileWithoutTheImage_IsRetried(t *testing.T) {
	distro := newDistroServer(t)
	distro.sums = sha256Hex([]byte("x")) + " *some-other-file.img\n"
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"x86_64"}

	_, err := runImport(t, p, distro.imageUrl())

	if err == nil || isPermanent(err) || len(p.ImportedImages) != 0 {
		t.Fatalf("want a retryable error and no upload, got %v", err)
	}
}

func TestSync_ImageUrl_NoImageForTheArchitecture_FailsForGood(t *testing.T) {
	distro := newDistroServer(t)
	delete(distro.images, "arm64")
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"aarch64"}

	_, err := runImport(t, p, distro.imageUrl())

	if !isPermanent(err) || !strings.Contains(err.Error(), "arm64") {
		t.Fatalf("want a permanent error naming the missing architecture, got %v", err)
	}
	if len(p.ImportedImages) != 0 || len(p.CreatedImages) != 0 {
		t.Fatal("uploaded or pulled something")
	}
}

func TestSync_ImageUrl_UnknownClusterArchitecture_FailsForGood_WithoutDownloading(t *testing.T) {
	distro := newDistroServer(t)
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"riscv64"}

	_, err := runImport(t, p, distro.imageUrl())

	if !isPermanent(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	if !strings.Contains(err.Error(), "riscv64") {
		t.Fatalf("error does not name the cluster's architecture: %v", err)
	}
	if len(distro.requested()) != 0 || len(p.CreatedImages) != 0 {
		t.Fatalf("downloaded or pulled for an architecture it cannot name: %v", distro.requested())
	}
}

func TestSync_ImageUrl_WithoutArchPlaceholder_FailsForGood_WithoutDownloading(t *testing.T) {
	distro := newDistroServer(t)
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"x86_64"}

	_, err := runImport(t, p, distro.URL+imagePath+"amd64.img")

	if !isPermanent(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	if len(distro.requested()) != 0 {
		t.Fatalf("downloaded an image whose architecture the URL does not follow: %v", distro.requested())
	}
}

func TestSync_ImageUrl_AliasPresent_DownloadsNothing(t *testing.T) {
	distro := newDistroServer(t)
	p := testsupport.NewProvider()
	p.Aliases["tpl-"+templateId] = true

	_, err := runImport(t, p, distro.imageUrl())

	if err != nil {
		t.Fatal(err)
	}
	if len(distro.requested()) != 0 || len(p.ImportedImages) != 0 {
		t.Fatalf("downloaded for an alias the cluster has: %v", distro.requested())
	}
}

func TestSync_ImageUrl_LeavesNoTemporaryFile_WhenTheUploadFails(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	distro := newDistroServer(t)
	p := testsupport.NewProvider()
	p.ServerArchitectures = []string{"x86_64"}
	p.ImportImageErr = testsupport.NotFound("upload refused")

	_, err := runImport(t, p, distro.imageUrl())

	if err == nil {
		t.Fatal("want the upload error")
	}
	entries, readErr := os.ReadDir(tmp)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary files left: %v", entries)
	}
}
