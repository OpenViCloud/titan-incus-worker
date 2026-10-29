package network

import (
	"context"
	"errors"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

type deletePayload struct {
	NetworkId         string `json:"networkId"`
	ExternalNetworkId string `json:"externalNetworkId"`
	ProjectName       string `json:"projectName"`
}

// Delete runs network.delete. Giving up gives the network back to the tenant
// (titan-ops architecture/desired-and-current.md#abandoned-request).
type Delete struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewDelete(cp *controlplane.Client, clusters cluster.Clusters) *Delete {
	return &Delete{cp: cp, clusters: clusters}
}

func (h *Delete) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.NetworkId != "" && payload.ExternalNetworkId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(errors.New("network.delete payload lacks networkId, externalNetworkId or projectName"))
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	err = cl.Provider.DeleteNetwork(ctx, payload.ProjectName, payload.ExternalNetworkId)
	isAlreadyGone := provider.IsNotFound(err)
	if err != nil && !isAlreadyGone {
		return err
	}
	if isAlreadyGone {
		logger.Info("already gone; nothing to delete", "object", payload.ExternalNetworkId)
	}
	// Incus keeps the ACL after its network goes, and refuses to delete it while the network uses it: second.
	aclName := provider.NetworkAclName(payload.NetworkId)
	err = cl.Provider.DeleteNetworkACL(ctx, payload.ProjectName, aclName)
	aclIsAlreadyGone := provider.IsNotFound(err)
	if err != nil && !aclIsAlreadyGone {
		return err
	}
	if aclIsAlreadyGone {
		logger.Info("already gone; nothing to delete", "object", aclName)
	}
	return sendStatus(ctx, h.cp, payload.NetworkId, statusReport{Report: reportDeleted})
}

func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.NetworkId == "" {
		return command.Permanent(errors.New("network.delete payload lacks networkId: nobody to tell"))
	}
	report := statusReport{Report: reportRequestFailed, Reason: reasonDeleteFailed, Detail: cause.Error()}
	return sendStatus(ctx, h.cp, payload.NetworkId, report)
}
