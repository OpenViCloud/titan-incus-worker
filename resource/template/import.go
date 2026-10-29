package template

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// archPlaceholder in a template's imageUrl stands for the architecture as the distro names its files.
const archPlaceholder = "{arch}"

// The distro publishes SHA256SUMS next to its images (cloud-images.ubuntu.com does).
const checksumFileName = "SHA256SUMS"

// distroArchitectures maps an Incus architecture to the name distros use in their image file names.
var distroArchitectures = map[string]string{"x86_64": "amd64", "aarch64": "arm64"}

// importImage downloads the distro's own image for the cluster's architecture, checks it against the checksum the
// distro publishes, and uploads it under the alias (titan-ops features/catalog/design.md#template-is-an-image-alias).
// The image goes to a temporary file, never into memory: it is hundreds of MB and the worker has 256 MiB.
func (h *Sync) importImage(ctx context.Context, p provider.Provider, payload syncPayload, alias string) error {
	// The URL is the only architecture guard on this path: the metadata carries the cluster's own architecture,
	// so the check after the import cannot catch an image of another one.
	if !strings.Contains(payload.ImageUrl, archPlaceholder) {
		return command.Permanent(fmt.Errorf("imageUrl %s has no %s placeholder", payload.ImageUrl, archPlaceholder))
	}
	clusterArchitectures, err := p.GetServerArchitectures(ctx)
	if err != nil {
		return err
	}
	clusterArchitecture, distroArchitecture := distroArchitectureOf(clusterArchitectures)
	if distroArchitecture == "" {
		return command.Permanent(fmt.Errorf("no distro image name for cluster architectures %s",
			strings.Join(clusterArchitectures, "/")))
	}
	imageUrl := strings.ReplaceAll(payload.ImageUrl, archPlaceholder, distroArchitecture)

	file, err := os.CreateTemp("", "titan-image-*.img")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()

	actual, err := download(ctx, imageUrl, file)
	if err != nil {
		return err
	}
	fileName, checksumUrl, err := checksumFileFor(imageUrl)
	if err != nil {
		return command.Permanent(err)
	}
	expected, err := publishedChecksum(ctx, checksumUrl, fileName)
	if err != nil {
		return err
	}
	if actual != expected {
		// Retried: the distro may have published a new build between the two downloads.
		return fmt.Errorf("checksum of %s is %s, %s says %s", imageUrl, actual, checksumFileName, expected)
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	metadata, err := metadataTarball(clusterArchitecture, payload.Name)
	if err != nil {
		return err
	}
	image := api.ImagesPost{Aliases: []api.ImageAlias{{Name: alias, Description: payload.Name}}}
	return p.ImportImage(ctx, image, metadata, file)
}

// distroArchitectureOf picks the first cluster architecture a distro names (x86_64 hosts also list i686).
func distroArchitectureOf(clusterArchitectures []string) (string, string) {
	for _, architecture := range clusterArchitectures {
		name, known := distroArchitectures[architecture]
		if known {
			return architecture, name
		}
	}
	return "", ""
}

// download streams the image into file and returns its SHA-256.
func download(ctx context.Context, imageUrl string, file *os.File) (string, error) {
	response, err := get(ctx, imageUrl)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return "", command.Permanent(fmt.Errorf("the distro publishes no image at %s", imageUrl))
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download %s: HTTP %d", imageUrl, response.StatusCode)
	}
	digest := sha256.New()
	if _, err := io.Copy(io.MultiWriter(file, digest), response.Body); err != nil {
		return "", fmt.Errorf("download %s: %w", imageUrl, err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func checksumFileFor(imageUrl string) (string, string, error) {
	parsed, err := url.Parse(imageUrl)
	if err != nil {
		return "", "", fmt.Errorf("imageUrl %s does not parse: %w", imageUrl, err)
	}
	fileName := path.Base(parsed.Path)
	parsed.Path = path.Join(path.Dir(parsed.Path), checksumFileName)
	return fileName, parsed.String(), nil
}

// publishedChecksum reads the line "<sha256> *<file>" (or "<sha256>  <file>") of fileName.
func publishedChecksum(ctx context.Context, checksumUrl string, fileName string) (string, error) {
	response, err := get(ctx, checksumUrl)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download %s: HTTP %d", checksumUrl, response.StatusCode)
	}
	lines := bufio.NewScanner(response.Body)
	for lines.Scan() {
		fields := strings.Fields(lines.Text())
		if len(fields) != 2 {
			continue
		}
		name := strings.TrimPrefix(fields[1], "*")
		if name == fileName {
			return fields[0], nil
		}
	}
	if err := lines.Err(); err != nil {
		return "", fmt.Errorf("read %s: %w", checksumUrl, err)
	}
	return "", fmt.Errorf("%s has no checksum for %s", checksumUrl, fileName)
}

func get(ctx context.Context, target string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", target, err)
	}
	return response, nil
}

// metadataTarball is the metadata half of a split Incus image: a gzip tarball holding metadata.yaml.
func metadataTarball(architecture string, description string) (io.Reader, error) {
	yaml := "architecture: " + architecture + "\n" +
		"creation_date: " + strconv.FormatInt(time.Now().Unix(), 10) + "\n" +
		"properties:\n" +
		"  description: " + strconv.Quote(description) + "\n"
	var buffer bytes.Buffer
	zipped := gzip.NewWriter(&buffer)
	archive := tar.NewWriter(zipped)
	header := &tar.Header{Name: "metadata.yaml", Mode: 0o644, Size: int64(len(yaml)), ModTime: time.Now()}
	if err := archive.WriteHeader(header); err != nil {
		return nil, err
	}
	if _, err := archive.Write([]byte(yaml)); err != nil {
		return nil, err
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	if err := zipped.Close(); err != nil {
		return nil, err
	}
	return &buffer, nil
}
