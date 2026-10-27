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

// Admin-facing only, but a whole Incus error can be long.
const maxDetail = 1000

// errWrongArchitecture marks a pull Incus completed with an image no VM of the cluster can start.
var errWrongArchitecture = errors.New("wrong architecture")

type syncPayload struct {
	TemplateId     string `json:"templateId"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	OsImageName    string `json:"osImageName"`
	ImageUrl       string `json:"imageUrl"`
	RootDiskSizeGb *int   `json:"rootDiskSizeGb"`
}

// Sync puts the template's VM image into the cluster under the template's alias: the distro's own image when
// the template has an imageUrl, else osImageName pulled from the image server.
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
	isComplete := payload.TemplateId != "" && payload.OsImageName != ""
	if !isComplete {
		return command.Permanent(errors.New("template.sync payload lacks templateId or osImageName"))
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
	return sendStatus(ctx, h.cp, payload.TemplateId, statusReport{
		Report:     reportCreated,
		ClusterId:  leased.ClusterId,
		ExternalId: alias,
	})
}

// ensureClusterArchitecture: when the image server has no VM image of the cluster's architecture, Incus pulls
// another one without an error, and no VM can start from it (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/catalog/design.md#template-architecture).
// The image is deleted so the reconciler sees the alias absent instead of reporting it present.
func ensureClusterArchitecture(ctx context.Context, p provider.Provider, alias string, osImageName string) error {
	entry, err := p.GetImageAlias(ctx, alias)
	if err != nil {
		return err
	}
	images, err := p.ListImages(ctx)
	if err != nil {
		return err
	}
	imageArchitecture := ""
	for _, image := range images {
		if image.Fingerprint == entry.Target {
			imageArchitecture = image.Architecture
		}
	}
	if imageArchitecture == "" {
		return fmt.Errorf("image %s of alias %s is not in the image list", entry.Target, alias)
	}
	clusterArchitectures, err := p.GetServerArchitectures(ctx)
	if err != nil {
		return err
	}
	if slices.Contains(clusterArchitectures, imageArchitecture) {
		return nil
	}

	mismatch := fmt.Sprintf("the image server has no %s VM image of %s: Incus pulled %s",
		strings.Join(clusterArchitectures, "/"), osImageName, imageArchitecture)
	deleteErr := p.DeleteImage(ctx, entry.Target)
	isAlreadyGone := provider.IsNotFound(deleteErr)
	if deleteErr != nil && !isAlreadyGone {
		// Retried; if every lease fails, this is the reason the give-up callback reports.
		return fmt.Errorf("%s, and deleting it failed: %w", mismatch, deleteErr)
	}
	return command.Permanent(fmt.Errorf("%w: %s, so the image was deleted", errWrongArchitecture, mismatch))
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
	if payload.TemplateId == "" {
		return command.Permanent(errors.New("template.sync payload lacks templateId: nobody to tell"))
	}
	detail := []rune(cause.Error())
	if len(detail) > maxDetail {
		detail = detail[:maxDetail]
	}
	return sendStatus(ctx, h.cp, payload.TemplateId, statusReport{
		Report:    reportRequestFailed,
		ClusterId: leased.ClusterId,
		Reason:    giveUpReason(cause),
		Detail:    string(detail),
	})
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
