package network

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

// ReconcilableNetwork mirrors ReconcilableNetworkQueryResult: what Titan last recorded, so only a difference is
// reported. CurrentState is null while Titan has seen no Incus status.
type ReconcilableNetwork struct {
	Id               string  `json:"id"`
	ExternalId       string  `json:"externalId"`
	CurrentState     *string `json:"currentState"`
	CurrentExistence string  `json:"currentExistence"`
	ProjectName      string  `json:"projectName"`
}

// Reconcile observes one cluster's networks and reports what differs on their observation route; it never acts on Incus
// and never ends a request. A failed read skips the cycle: reading it as "no network" would report every network gone.
func Reconcile(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, logger *slog.Logger) error {
	var rows []ReconcilableNetwork
	if err := cp.GetJSON(ctx, "/internal/reconcile/networks?clusterId="+url.QueryEscape(clusterId), &rows); err != nil {
		return fmt.Errorf("list reconcilable networks of cluster %s: %w", clusterId, err)
	}
	if len(rows) == 0 {
		return nil
	}
	networks, err := p.ListNetworksAllProjects(ctx)
	if err != nil {
		return fmt.Errorf("observe networks of cluster %s: %w", clusterId, err)
	}
	statusByProjectAndName := StatusByProjectAndName(networks)

	for _, row := range rows {
		report, shouldReport := DecideReport(row, statusByProjectAndName)
		if !shouldReport {
			continue
		}
		status := ""
		if report.Observed != nil {
			status = report.Observed.Status
		}
		err := sendObservation(ctx, cp, row.Id, report)
		if err != nil {
			logger.Warn("network report failed; next cycle sends it again", "networkId", row.Id, "status", status, "error", err)
			continue
		}
		recordedState := ""
		if row.CurrentState != nil {
			recordedState = *row.CurrentState
		}
		logger.Info("reconcile report sent", "networkId", row.Id, "recordedState", recordedState,
			"recordedExistence", row.CurrentExistence, "exists", report.Exists, "status", status)
	}
	return nil
}

// StatusByProjectAndName keys each network's status by provider.ProjectKey of its project and name, and of its name alone
// for a row that names no project.
func StatusByProjectAndName(networks []api.Network) map[string]string {
	statuses := map[string]string{}
	for _, network := range networks {
		statuses[provider.ProjectKey(network.Project, network.Name)] = network.Status
		statuses[provider.ProjectKey("", network.Name)] = network.Status
	}
	return statuses
}

// DecideReport compares one network with Incus: a report when the status or the existence differs from what Titan
// recorded. The status goes as Incus spells it, whatever the word. Absent is NotFound, never Deleted: only the delete
// command reports that, since a reconciler pointed at the wrong cluster also finds nothing
// (titan-ops architecture/desired-and-current.md#existence-axis).
func DecideReport(row ReconcilableNetwork, statuses map[string]string) (ObservationBody, bool) {
	status, present := statuses[provider.ProjectKey(row.ProjectName, row.ExternalId)]
	if !present {
		alreadyRecorded := row.CurrentExistence == existenceNotFound
		return ObservationBody{Exists: false}, !alreadyRecorded
	}

	recordedState := ""
	if row.CurrentState != nil {
		recordedState = *row.CurrentState
	}
	existenceChanged := row.CurrentExistence != existenceExists
	stateChanged := status != recordedState
	return ObservationBody{Exists: true, Observed: &Observation{Status: status}}, existenceChanged || stateChanged
}
