package firewall

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// The CurrentExistence values DecideReport reads.
const (
	existenceExists   = "Exists"
	existenceNotFound = "NotFound"
)

// ReconcilableRule mirrors ReconcilableFirewallRuleQueryResult: what Titan last recorded, so only a difference is
// reported. CurrentState is null while Titan has seen no Incus state for the rule.
type ReconcilableRule struct {
	Id               string  `json:"id"`
	NetworkId        string  `json:"networkId"`
	ExternalId       string  `json:"externalId"`
	CurrentState     *string `json:"currentState"`
	CurrentExistence string  `json:"currentExistence"`
}

// Reconcile observes one cluster's firewall rules — every ACL of every project in one read — and reports what differs;
// it never acts on Incus. A failed read skips the cycle: reading it as "no rule" would report every rule gone.
func Reconcile(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, logger *slog.Logger) error {
	var rows []ReconcilableRule
	if err := cp.GetJSON(ctx, "/internal/reconcile/firewall-rules?clusterId="+url.QueryEscape(clusterId), &rows); err != nil {
		return fmt.Errorf("list reconcilable firewall rules of cluster %s: %w", clusterId, err)
	}
	if len(rows) == 0 {
		return nil
	}
	acls, err := p.ListNetworkACLsAllProjects(ctx)
	if err != nil {
		return fmt.Errorf("observe network ACLs of cluster %s: %w", clusterId, err)
	}
	stateById := map[string]string{}
	for _, acl := range acls {
		for _, rules := range [][]api.NetworkACLRule{acl.Ingress, acl.Egress} {
			for _, rule := range rules {
				stateById[ProviderId(acl.Name, rule.Description)] = rule.State
			}
		}
	}

	for _, row := range rows {
		report, shouldReport := DecideReport(row, stateById)
		if !shouldReport {
			continue
		}
		err := sendStatus(ctx, cp, row.NetworkId, row.Id, report)
		if err != nil {
			logger.Warn("firewall rule report failed; next cycle sends it again", "ruleId", row.Id, "state", report.State, "error", err)
			continue
		}
		recordedState := ""
		if row.CurrentState != nil {
			recordedState = *row.CurrentState
		}
		logger.Info("reconcile report sent", "ruleId", row.Id, "networkId", row.NetworkId, "recordedState", recordedState,
			"recordedExistence", row.CurrentExistence, "exists", *report.Exists, "state", report.State)
	}
	return nil
}

// DecideReport compares one rule with its ACL: a report when its state or its existence differs from what Titan
// recorded. The state goes as Incus spells it. Absent is NotFound, never Deleted: only a delete command reports that
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#existence-axis).
func DecideReport(row ReconcilableRule, stateById map[string]string) (statusReport, bool) {
	state, present := stateById[row.ExternalId]
	if !present {
		absent := false
		alreadyRecorded := row.CurrentExistence == existenceNotFound
		return statusReport{Report: reportObserved, Exists: &absent}, !alreadyRecorded
	}

	exists := true
	recordedState := ""
	if row.CurrentState != nil {
		recordedState = *row.CurrentState
	}
	existenceChanged := row.CurrentExistence != existenceExists
	stateChanged := state != recordedState
	return statusReport{Report: reportObserved, Exists: &exists, State: state}, existenceChanged || stateChanged
}
