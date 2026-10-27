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
	RuleId         string `json:"ruleId"`
	NetworkId      string `json:"networkId"`
	ExternalRuleId string `json:"externalRuleId"`
	ProjectName    string `json:"projectName"`
}

// Delete runs firewall-rule.delete: the rule's entry leaves its ACL. Giving up gives the rule back to the tenant
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#abandoned-request).
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
	isComplete := payload.RuleId != "" && payload.NetworkId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(errors.New("firewall-rule.delete payload lacks ruleId, networkId or projectName"))
	}
	aclName, entry, found := strings.Cut(payload.ExternalRuleId, "/")
	if !found || aclName == "" || entry == "" {
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
	return sendStatus(ctx, h.cp, payload.NetworkId, payload.RuleId, statusReport{Report: reportDeleted})
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
	if payload.RuleId == "" || payload.NetworkId == "" {
		return command.Permanent(errors.New("firewall-rule.delete payload lacks ruleId or networkId: nobody to tell"))
	}
	report := statusReport{Report: reportRequestFailed, Reason: reasonDeleteFailed, Detail: cause.Error()}
	return sendStatus(ctx, h.cp, payload.NetworkId, payload.RuleId, report)
}
