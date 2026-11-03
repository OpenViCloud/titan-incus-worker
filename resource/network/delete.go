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
	NetworkId string `json:"networkId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes). Empty
	// on a command of the old control plane that the migration gave no id: it keeps the old reporting until rollout
	// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout).
	RequestId         string `json:"requestId"`
	ExternalNetworkId string `json:"externalNetworkId"`
	ProjectName       string `json:"projectName"`
}

// Delete runs network.delete and ends its request Done (Deleted on the old route). Giving up ends it Failed, which
// gives the network back to the tenant (titan-ops architecture/desired-and-current.md#abandoned-request), unless Incus
// no longer has the network nor its ACL.
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
	if payload.RequestId == "" {
		return sendStatus(ctx, h.cp, payload.NetworkId, statusReport{Report: reportDeleted})
	}
	return endRequest(ctx, h.cp, payload.NetworkId, payload.RequestId, done("", nil))
}

func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.NetworkId == "" {
		return command.Permanent(errors.New("network.delete payload lacks networkId: nobody to tell"))
	}
	if payload.RequestId == "" {
		report := statusReport{Report: reportRequestFailed, Reason: reasonDeleteFailed, Detail: cause.Error()}
		return sendStatus(ctx, h.cp, payload.NetworkId, report)
	}
	gone, err := h.networkIsGone(ctx, leased.ClusterId, payload)
	if err != nil {
		return err
	}
	// The network is gone: its report never arrived, or Run kept failing on the ACL it deletes second. The give-up
	// removes the ACL once more; with both gone, giving the network back would leave the tenant one Incus does not
	// have, so the request is Done. Any other answer about the ACL ends it Failed: the Incus adapter calls every error
	// transient, so waiting for one to pass could last for ever, and the tenant's next delete tries the ACL again.
	if gone && h.aclRemoved(ctx, leased.ClusterId, payload) {
		logger.Info("already gone; the give-up ends the request done", "object", payload.ExternalNetworkId)
		return endRequest(ctx, h.cp, payload.NetworkId, payload.RequestId, done("", nil))
	}
	end := failed("", nil, reasonDeleteFailed, cause)
	return endRequest(ctx, h.cp, payload.NetworkId, payload.RequestId, end)
}

// aclRemoved deletes the network's ACL: true only when Incus has it no longer.
func (h *Delete) aclRemoved(ctx context.Context, clusterId string, payload deletePayload) bool {
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		return false
	}
	err = cl.Provider.DeleteNetworkACL(ctx, payload.ProjectName, provider.NetworkAclName(payload.NetworkId))
	return err == nil || provider.IsNotFound(err)
}

// networkIsGone reads the network: only Incus answering 404 is gone. A read that fails is returned, so the next lease
// gives up again instead of guessing: a read is refused only for what hits the whole cluster (an outage, a trust that
// lapsed), which an operator mends, never for the network's own state, unlike a delete, which Incus refuses for the
// object's state ("in use"). A payload or cluster that cannot be read from at all tells nothing, which is not gone.
func (h *Delete) networkIsGone(ctx context.Context, clusterId string, payload deletePayload) (bool, error) {
	canRead := payload.ExternalNetworkId != "" && payload.ProjectName != ""
	if !canRead {
		return false, nil
	}
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		if !command.IsRetryable(err) {
			return false, nil
		}
		return false, err
	}
	_, _, err = cl.Provider.GetNetwork(ctx, payload.ProjectName, payload.ExternalNetworkId)
	if provider.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}
