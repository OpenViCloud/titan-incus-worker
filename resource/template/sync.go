package template

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// errWrongArchitecture marks a pull Incus completed with an image no VM of the cluster can start.
var errWrongArchitecture = errors.New("wrong architecture")

// errImageNotListed: the alias points to an image the image list does not show, so its architecture is unknown.
var errImageNotListed = errors.New("not in the image list")

type syncPayload struct {
	TemplateId string `json:"templateId"`
	// RequestId names the sync request this command ends
	// (titan-ops architecture/resource-lifecycle.md#callback-routes).
	RequestId      string `json:"requestId"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	OsImageName    string `json:"osImageName"`
	ImageUrl       string `json:"imageUrl"`
	RootDiskSizeGb *int   `json:"rootDiskSizeGb"`
}

// Sync puts the template's VM image into the cluster under the template's alias: the distro's own image when
// the template has an imageUrl, else osImageName pulled from the image server. It ends its request Done carrying the
// alias. Giving up ends it Failed, carrying the alias only when the cluster has it on its own architecture.
type Sync struct {
	cp          *controlplane.Client
	clusters    cluster.Clusters
	imageServer string
}

func NewSync(cp *controlplane.Client, clusters cluster.Clusters, imageServer string) *Sync {
	return &Sync{cp: cp, clusters: clusters, imageServer: imageServer}
}

func (h *Sync) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload syncPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.TemplateId != "" && payload.RequestId != "" && payload.OsImageName != ""
	if !isComplete {
		return command.Permanent(errors.New("template.sync payload lacks templateId, requestId or osImageName"))
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}

	alias := provider.TemplateAlias(payload.TemplateId)
	_, err = cl.Provider.GetImageAlias(ctx, alias)
	isMissing := provider.IsNotFound(err)
	if err != nil && !isMissing {
		return err
	}
	if !isMissing {
		logger.Info("template image already present", "alias", alias)
	}
	if isMissing {
		err := h.makeImage(ctx, cl.Provider, payload, alias)
		// Another run finished the same pull or import first.
		isOurs := provider.IsOperationFailureContaining(err, "already exists")
		if err != nil && !isOurs {
			return err
		}
	}
	if err := ensureClusterArchitecture(ctx, cl.Provider, alias, payload.OsImageName); err != nil {
		return err
	}
	return endRequest(ctx, h.cp, payload.TemplateId, leased.ClusterId, payload.RequestId, done(alias))
}

// ensureClusterArchitecture: when the image server has no VM image of the cluster's architecture, Incus pulls
// another one without an error, and no VM can start from it (titan-ops features/catalog/design.md#template-architecture).
// The image is deleted so the reconciler sees the alias absent instead of reporting it present.
func ensureClusterArchitecture(ctx context.Context, p provider.Provider, alias string, osImageName string) error {
	image, err := readAliasImage(ctx, p, alias)
	if err != nil {
		return err
	}
	if image.fitsTheCluster() {
		return nil
	}

	mismatch := fmt.Sprintf("the image server has no %s VM image of %s: Incus pulled %s",
		strings.Join(image.clusterArchitectures, "/"), osImageName, image.architecture)
	deleteErr := p.DeleteImage(ctx, image.fingerprint)
	isAlreadyGone := provider.IsNotFound(deleteErr)
	if deleteErr != nil && !isAlreadyGone {
		// Retried; if every lease fails, this is the reason the give-up callback reports.
		return fmt.Errorf("%s, and deleting it failed: %w", mismatch, deleteErr)
	}
	return command.Permanent(fmt.Errorf("%w: %s, so the image was deleted", errWrongArchitecture, mismatch))
}

// aliasImage is the image an alias points to, and the architectures of the cluster it is on.
type aliasImage struct {
	fingerprint          string
	architecture         string
	clusterArchitectures []string
}

func (i aliasImage) fitsTheCluster() bool {
	return slices.Contains(i.clusterArchitectures, i.architecture)
}

// readAliasImage only reads: Run deletes what does not fit, a give-up reports it.
func readAliasImage(ctx context.Context, p provider.Provider, alias string) (aliasImage, error) {
	entry, err := p.GetImageAlias(ctx, alias)
	if err != nil {
		return aliasImage{}, err
	}
	images, err := p.ListImages(ctx)
	if err != nil {
		return aliasImage{}, err
	}
	image := aliasImage{fingerprint: entry.Target}
	for _, listed := range images {
		if listed.Fingerprint == entry.Target {
			image.architecture = listed.Architecture
		}
	}
	if image.architecture == "" {
		return aliasImage{}, fmt.Errorf("image %s of alias %s is %w", entry.Target, alias, errImageNotListed)
	}
	image.clusterArchitectures, err = p.GetServerArchitectures(ctx)
	if err != nil {
		return aliasImage{}, err
	}
	return image, nil
}

// makeImage imports the distro's own image when the template names one, else pulls osImageName from the image server.
func (h *Sync) makeImage(ctx context.Context, p provider.Provider, payload syncPayload, alias string) error {
	if payload.ImageUrl != "" {
		return h.importImage(ctx, p, payload, alias)
	}
	return p.CreateImage(ctx, h.pullRequest(payload, alias))
}

func (h *Sync) pullRequest(payload syncPayload, alias string) api.ImagesPost {
	return api.ImagesPost{
		Source: &api.ImagesPostSource{
			ImageSource: api.ImageSource{
				Alias:     payload.OsImageName,
				Protocol:  "simplestreams",
				Server:    h.imageServer,
				ImageType: string(api.InstanceTypeVM),
			},
			Mode: "pull",
			Type: "image",
		},
		Aliases: []api.ImageAlias{{Name: alias, Description: payload.Name}},
	}
}

func (h *Sync) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload syncPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		logger.Warn("no request id; nothing to end")
		return nil
	}
	if payload.TemplateId == "" {
		return command.Permanent(errors.New("template.sync payload lacks templateId: nobody to tell"))
	}
	alias, err := h.usableAlias(ctx, leased.ClusterId, provider.TemplateAlias(payload.TemplateId))
	if err != nil {
		return err
	}
	// The alias is the template's id on the cluster: a Failed carrying it records the template there, as the give-up
	// of a create that finds its object does for every other kind.
	if alias != "" {
		logger.Info("template image present; the give-up carries its alias", "alias", alias)
	}
	return endRequest(ctx, h.cp, payload.TemplateId, leased.ClusterId, payload.RequestId, failed(alias, cause))
}

// usableAlias is the alias when the cluster has it on its own architecture, else "". It writes nothing: an image of
// another architecture stays for the next sync's Run to delete. Absent is Incus answering 404, or an alias whose
// image the image list does not show; any other read that fails is returned, so the next lease gives up again. A
// cluster the worker cannot reach for good (not configured, or its connection refused by the control plane: a
// permanent error) answers nothing, which is no alias.
func (h *Sync) usableAlias(ctx context.Context, clusterId string, alias string) (string, error) {
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		if !command.IsRetryable(err) {
			return "", nil
		}
		return "", err
	}
	image, err := readAliasImage(ctx, cl.Provider, alias)
	isAbsent := provider.IsNotFound(err) || errors.Is(err, errImageNotListed)
	if isAbsent {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !image.fitsTheCluster() {
		return "", nil
	}
	return alias, nil
}

func giveUpReason(cause error) string {
	if errors.Is(cause, errWrongArchitecture) {
		return reasonWrongArchitecture
	}
	if errors.Is(cause, cluster.ErrNoCluster) {
		return reasonNoCluster
	}
	return reasonSyncFailed
}
