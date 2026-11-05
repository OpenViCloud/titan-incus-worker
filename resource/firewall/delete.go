package firewall

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

type deletePayload struct {
	RuleId string `json:"ruleId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes).
	RequestId      string `json:"requestId"`
	NetworkId      string `json:"networkId"`
	ExternalRuleId string `json:"externalRuleId"`
	ProjectName    string `json:"projectName"`
}

// Delete runs firewall-rule.delete: the rule's entry leaves its ACL, and the request ends Done. Giving up ends it
// Failed, which gives the rule back to the tenant (titan-ops architecture/desired-and-current.md#abandoned-request),
// unless the ACL no longer has the rule.
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
	isComplete := payload.RuleId != "" && payload.RequestId != "" && payload.NetworkId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(errors.New("firewall-rule.delete payload lacks ruleId, requestId, networkId or projectName"))
	}
	aclName, entry, ok := splitProviderId(payload.ExternalRuleId)
	if !ok {
		return command.Permanent(fmt.Errorf("%q is not a firewall rule provider id (<acl>/<ruleId>)", payload.ExternalRuleId))
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}

	acl, etag, err := cl.Provider.GetNetworkACL(ctx, payload.ProjectName, aclName)
	aclIsGone := provider.IsNotFound(err)
	if err != nil && !aclIsGone {
		return err
	}
	ruleIsThere := !aclIsGone && hasRule(acl, entry)
	if !ruleIsThere {
		logger.Info("already gone; nothing to delete", "object", payload.ExternalRuleId)
	}
	if ruleIsThere {
		put := acl.Writable()
		put.Ingress = without(put.Ingress, entry)
		put.Egress = without(put.Egress, entry)
		if err := cl.Provider.UpdateNetworkACL(ctx, payload.ProjectName, aclName, put, etag); err != nil {
			return err
		}
	}
	return endRequest(ctx, h.cp, payload.NetworkId, payload.RuleId, payload.RequestId, done("", nil))
}

func splitProviderId(externalRuleId string) (string, string, bool) {
	aclName, entry, found := strings.Cut(externalRuleId, "/")
	return aclName, entry, found && aclName != "" && entry != ""
}

func without(rules []api.NetworkACLRule, ruleId string) []api.NetworkACLRule {
	kept := []api.NetworkACLRule{}
	for _, rule := range rules {
		if rule.Description != ruleId {
			kept = append(kept, rule)
		}
	}
	return kept
}

func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		logger.Warn("no request id; nothing to end")
		return nil
	}
	if payload.RuleId == "" || payload.NetworkId == "" {
		return command.Permanent(errors.New("firewall-rule.delete payload lacks ruleId or networkId: nobody to tell"))
	}
	gone, err := h.ruleIsGone(ctx, leased.ClusterId, payload)
	if err != nil {
		return err
	}
	// The rule left the ACL but its report never arrived: giving it back would leave the tenant a rule Incus does not
	// apply, so the request is Done.
	if gone {
		logger.Info("already gone; the give-up ends the request done", "object", payload.ExternalRuleId)
		return endRequest(ctx, h.cp, payload.NetworkId, payload.RuleId, payload.RequestId, done("", nil))
	}
	end := failed("", reasonDeleteFailed, cause)
	return endRequest(ctx, h.cp, payload.NetworkId, payload.RuleId, payload.RequestId, end)
}

// ruleIsGone reads the rule's ACL: a rule not in it, or an ACL Incus does not have, is gone. A read that fails is
// returned, so the next lease gives up again; a payload or cluster that cannot be read from at all tells nothing,
// which is not gone.
func (h *Delete) ruleIsGone(ctx context.Context, clusterId string, payload deletePayload) (bool, error) {
	aclName, entry, ok := splitProviderId(payload.ExternalRuleId)
	if !ok || payload.ProjectName == "" {
		return false, nil
	}
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		if !command.IsRetryable(err) {
			return false, nil
		}
		return false, err
	}
	there, err := isInAcl(ctx, cl.Provider, payload.ProjectName, aclName, entry)
	if err != nil {
		return false, err
	}
	return !there, nil
}
