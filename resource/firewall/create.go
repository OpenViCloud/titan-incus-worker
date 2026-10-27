package firewall

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// Create runs firewall-rule.create: the rule goes into its network's ACL. It reports the provider id (Created); the
// reconciler reports Active.
type Create struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewCreate(cp *controlplane.Client, clusters cluster.Clusters) *Create {
	return &Create{cp: cp, clusters: clusters}
}

func (h *Create) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	payload, err := decodeCreate(leased)
	if err != nil {
		return err
	}
	direction, rule, err := AclRule(payload)
	if err != nil {
		return command.Permanent(err)
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	aclName := provider.NetworkAclName(payload.NetworkId)
	acl, etag, err := cl.Provider.GetNetworkACL(ctx, payload.ProjectName, aclName)
	// network.create makes the ACL with the network's default action; a rule cannot make one without knowing it.
	if provider.IsNotFound(err) {
		return command.Permanent(fmt.Errorf("network %s has no ACL %s: %w", payload.NetworkId, aclName, err))
	}
	if err != nil {
		return err
	}

	// An earlier run wrote it already: writing it again would put the rule in twice.
	alreadyThere := hasRule(acl, payload.RuleId)
	if alreadyThere {
		logger.Info("already exists; treating it as ours", "object", ProviderId(aclName, payload.RuleId))
	}
	if !alreadyThere {
		put := acl.Writable()
		if direction == "ingress" {
			put.Ingress = append(put.Ingress, rule)
		} else {
			put.Egress = append(put.Egress, rule)
		}
		// A 412 (someone wrote the ACL since our read) is retryable: the next lease reads it again.
		if err := cl.Provider.UpdateNetworkACL(ctx, payload.ProjectName, aclName, put, etag); err != nil {
			return err
		}
	}
	report := statusReport{Report: reportCreated, ExternalRuleId: ProviderId(aclName, payload.RuleId)}
	return sendStatus(ctx, h.cp, payload.NetworkId, payload.RuleId, report)
}

// GiveUp reports the give-up status, with the provider id when the rule is in the ACL so its delete still reaches
// Incus.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload CreatePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RuleId == "" || payload.NetworkId == "" {
		return command.Permanent(errors.New("firewall-rule.create payload lacks ruleId or networkId: nobody to tell"))
	}
	report := statusReport{Report: reportRequestFailed, Reason: reasonCreateFailed, Detail: cause.Error()}
	aclName := provider.NetworkAclName(payload.NetworkId)
	if h.ruleExists(ctx, leased.ClusterId, payload.ProjectName, aclName, payload.RuleId) {
		report.ExternalRuleId = ProviderId(aclName, payload.RuleId)
	}
	return sendStatus(ctx, h.cp, payload.NetworkId, payload.RuleId, report)
}

func (h *Create) ruleExists(ctx context.Context, clusterId string, project string, aclName string, ruleId string) bool {
	if project == "" {
		return false
	}
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		return false
	}
	acl, _, err := cl.Provider.GetNetworkACL(ctx, project, aclName)
	if err != nil {
		return false
	}
	return hasRule(acl, ruleId)
}

func decodeCreate(leased command.Leased) (CreatePayload, error) {
	var payload CreatePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return CreatePayload{}, err
	}
	isComplete := payload.RuleId != "" && payload.NetworkId != "" && payload.ProjectName != ""
	if !isComplete {
		return CreatePayload{}, command.Permanent(errors.New("firewall-rule.create payload lacks ruleId, networkId or projectName"))
	}
	return payload, nil
}
