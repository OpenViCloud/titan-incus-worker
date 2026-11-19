package restore

import (
	"context"
	"log/slog"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// templateObservation mirrors resource/template/report.go's ObservationBody: PATCH
// /internal/templates/{templateId}/clusters/{clusterId}/observation.
type templateObservation struct {
	Exists bool `json:"exists"`
}

// restoreTemplates reports every template missing, the way the template reconciler reports a missing alias: a
// rebuilt host has no template images, so every one Titan lists is reported absent, then one
// POST /internal/templates/sync asks the control plane to re-sync every cluster's templates
// (titan-ops features/backup/design.md#cluster-restore).
func restoreTemplates(ctx context.Context, cp *controlplane.Client, p provider.Provider, rows []titanTemplate, clusterId string,
	logger *slog.Logger, summary *Summary) {
	for _, row := range rows {
		path := "/internal/templates/" + url.PathEscape(row.Id) + "/clusters/" + url.PathEscape(clusterId) + "/observation"
		if err := cp.Patch(ctx, path, templateObservation{Exists: false}); err != nil {
			logger.Warn("reporting a missing template failed", "templateId", row.Id, "error", err)
			summary.Failed++
			continue
		}
		logger.Info("template reported missing", "templateId", row.Id, "clusterId", clusterId)
		summary.Restored++
	}
	if len(rows) == 0 {
		return
	}
	if err := cp.Post(ctx, "/internal/templates/sync", nil); err != nil {
		logger.Warn("asking the control plane to re-sync templates failed", "error", err)
		summary.Failed++
		return
	}
	logger.Info("template sync requested")
}
