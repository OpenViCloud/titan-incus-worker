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

// Create runs firewall-rule.create: the rule goes into its network's ACL, and the request ends Done with the provider
// id and the rule's state as the ACL then shows it.
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
	state, alreadyThere := ruleState(acl, payload.RuleId)
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
	externalId := ProviderId(aclName, payload.RuleId)
	var observed *Observation
	if alreadyThere {
		observed = &Observation{State: state}
	} else {
		observed = readObservation(ctx, cl.Provider, payload.ProjectName, aclName, payload.RuleId, logger)
	}
	return endRequest(ctx, h.cp, payload.NetworkId, payload.RuleId, payload.RequestId, done(externalId, observed))
}

// readObservation reads the rule back once after the write. A read that fails, or a rule someone took out meanwhile,
// does not undo the write: the report goes without an observation and the reconciler reports the rule next cycle.
func readObservation(ctx context.Context, p provider.Provider, project string, aclName string, ruleId string,
	logger *slog.Logger) *Observation {
	acl, _, err := p.GetNetworkACL(ctx, project, aclName)
	if err != nil {
		logger.Warn("firewall rule not read after its command; the reconciler reports it",
			"object", ProviderId(aclName, ruleId), "error", err)
		return nil
	}
	state, found := ruleState(acl, ruleId)
	if !found {
		return nil
	}
	return &Observation{State: state}
}

// GiveUp ends the request Failed, with the provider id when the rule is in the ACL so its delete still reaches Incus.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload CreatePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		logger.Warn("no request id; nothing to end")
		return nil
	}
	if payload.RuleId == "" || payload.NetworkId == "" {
		return command.Permanent(errors.New("firewall-rule.create payload lacks ruleId or networkId: nobody to tell"))
	}
	aclName := provider.NetworkAclName(payload.NetworkId)
	// Whether the rule is in the ACL decides whether the control plane keeps an id to delete it through: a read that
	// failed answers neither, so the next lease gives up again.
	exists, err := h.ruleIsThere(ctx, leased.ClusterId, payload.ProjectName, aclName, payload.RuleId)
	if err != nil {
		return err
	}
	externalId := ""
	if exists {
		externalId = ProviderId(aclName, payload.RuleId)
	}
	end := failed(externalId, reasonCreateFailed, cause)
	return endRequest(ctx, h.cp, payload.NetworkId, payload.RuleId, payload.RequestId, end)
}

func (h *Create) ruleIsThere(ctx context.Context, clusterId string, project string, aclName string,
	ruleId string) (bool, error) {
	if project == "" {
		return false, nil
	}
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		// A cluster that is not configured never had the rule; a connection that may come back tells nothing yet.
		if !command.IsRetryable(err) {
			return false, nil
		}
		return false, err
	}
	return isInAcl(ctx, cl.Provider, project, aclName, ruleId)
}

func decodeCreate(leased command.Leased) (CreatePayload, error) {
	var payload CreatePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return CreatePayload{}, err
	}
	isComplete := payload.RuleId != "" && payload.RequestId != "" && payload.NetworkId != "" && payload.ProjectName != ""
	if !isComplete {
		return CreatePayload{}, command.Permanent(errors.New("firewall-rule.create payload lacks ruleId, requestId, networkId or projectName"))
	}
	return payload, nil
}
