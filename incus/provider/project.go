package provider

import (
	"context"
	"fmt"

	"github.com/lxc/incus/v7/shared/api"
)

// projectConfig is the lab's recorded project (lab 01): images come from the default project, everything else is the
// tenant's own.
var projectConfig = map[string]string{
	"features.images":          "false",
	"features.networks":        "true",
	"features.profiles":        "true",
	"features.storage.buckets": "true",
	"features.storage.volumes": "true",
}

// EnsureProject creates the tenant's project on first use (titan-ops architecture/tenancy-and-admin.md#project-name). A 409
// is another run that got there first.
func EnsureProject(ctx context.Context, p Provider, name string) error {
	_, err := p.GetProject(ctx, name)
	if err == nil {
		return nil
	}
	if !IsNotFound(err) {
		return err
	}
	config := map[string]string{}
	for key, value := range projectConfig {
		config[key] = value
	}
	err = p.CreateProject(ctx, api.ProjectsPost{Name: name, ProjectPut: api.ProjectPut{Config: config}})
	if err == nil {
		return nil
	}
	if IsConflict(err) {
		return nil
	}
	return fmt.Errorf("ensure project %s: %w", name, err)
}
