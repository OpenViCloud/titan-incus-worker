package template

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// The control plane's existence of a template in one cluster that a report would not change.
const (
	existenceExists   = "Exists"
	existenceNotFound = "NotFound"
)

// ReconcilableTemplate mirrors ReconcilableTemplateQueryResult.
type ReconcilableTemplate struct {
	Id                 string `json:"id"`
	ExternalTemplateId string `json:"externalTemplateId"`
	CurrentExistence   string `json:"currentExistence"`
}

// Reconcile observes one cluster's template aliases and reports what differs; it never acts on Incus.
func Reconcile(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, logger *slog.Logger) error {
	var rows []ReconcilableTemplate
	if err := cp.GetJSON(ctx, "/internal/reconcile/templates?clusterId="+url.QueryEscape(clusterId), &rows); err != nil {
		return fmt.Errorf("list reconcilable templates of cluster %s: %w", clusterId, err)
	}
	if len(rows) == 0 {
		return nil
	}
	aliases, err := p.ListImageAliases(ctx)
	if err != nil {
		return fmt.Errorf("observe image aliases of cluster %s: %w", clusterId, err)
	}
	present := map[string]bool{}
	for _, alias := range aliases {
		present[alias.Name] = true
	}

	for _, row := range rows {
		exists, shouldReport := DecideReport(row, present)
		if !shouldReport {
			continue
		}
		err := sendStatus(ctx, cp, row.Id, statusReport{Report: reportObserved, ClusterId: clusterId, Exists: &exists})
		if err != nil {
			logger.Warn("template report failed; next cycle sends it again", "templateId", row.Id, "exists", exists, "error", err)
			continue
		}
		logger.Info("reconcile report sent", "templateId", row.Id, "recordedExistence", row.CurrentExistence, "exists", exists)
	}
	return nil
}

// DecideReport returns whether the alias exists and whether that is news: an alias there to a template not recorded
// Exists, an alias gone to one not recorded NotFound.
func DecideReport(row ReconcilableTemplate, present map[string]bool) (bool, bool) {
	isPresent := present[row.ExternalTemplateId]
	if !isPresent {
		return false, row.CurrentExistence != existenceNotFound
	}
	return true, row.CurrentExistence != existenceExists
}
