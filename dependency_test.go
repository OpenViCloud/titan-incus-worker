package main

import (
	"go/build"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const modulePath = "github.com/OpenViCloud/titan-incus-worker"

// Each package belongs to one role, named by its first directory (titan-ops architecture/worker-structure.md#worker-layout).
// main wires everything · loop runs the work and knows no resource · resource holds one resource each · incus,
// controlplane and bucket talk to the outside and know neither loop nor resource.
var allowedGroupEdges = map[string][]string{
	"main":         {"loop", "resource", "incus", "controlplane", "bucket"},
	"loop":         {"incus", "controlplane"},
	"resource":     {"loop", "resource", "incus", "controlplane"},
	"incus":        {"incus", "controlplane"},
	"controlplane": {},
	"bucket":       {},
	"testsupport":  {"incus", "controlplane"},
}

// A resource may use another resource only through an edge named here, with its reason.
var resourceEdgeAllowlist = map[string]string{
	"resource/volsnapshot -> resource/volume": "a volume snapshot's callback and the snapshot list are the volume's: the storage reconciler observes both",
}

// Resources implement loop/command's Handler; the reconcile loop calls them, never the other way round.
var forbiddenEdges = map[string]bool{
	"resource -> loop/reconcile": true,
}

func groupOf(pkg string) string {
	if pkg == "" {
		return "main"
	}
	first, _, _ := strings.Cut(pkg, "/")
	return first
}

// moduleImports reads every package of this module and the module packages it imports, tests excluded.
func moduleImports(t *testing.T) map[string][]string {
	t.Helper()
	imports := map[string][]string{}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		isHidden := strings.HasPrefix(entry.Name(), ".") && path != "."
		isSkipped := entry.Name() == "testdata" || isHidden
		if isSkipped {
			return filepath.SkipDir
		}
		pkg, err := build.ImportDir(path, 0)
		_, hasNoGoFiles := err.(*build.NoGoError)
		if hasNoGoFiles {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.ToSlash(path)
		if name == "." {
			name = ""
		}
		moduleTargets := []string{}
		for _, imported := range pkg.Imports {
			if strings.HasPrefix(imported, modulePath+"/") {
				moduleTargets = append(moduleTargets, strings.TrimPrefix(imported, modulePath+"/"))
			}
		}
		imports[name] = moduleTargets
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return imports
}

func TestPackageDependencies_FollowTheRoles(t *testing.T) {
	imports := moduleImports(t)
	if len(imports) < 10 {
		t.Fatalf("read only %d packages; the scan is not seeing the module", len(imports))
	}
	// A modulePath that is not this module's sees every package and no edge, and every rule below then passes.
	edgeCount := 0
	for _, targets := range imports {
		edgeCount += len(targets)
	}
	if edgeCount == 0 {
		t.Fatalf("read no import between packages of %s; modulePath is not this module's", modulePath)
	}

	seenAllowlistRows := map[string]bool{}
	for pkg, targets := range imports {
		group := groupOf(pkg)
		allowed, known := allowedGroupEdges[group]
		if !known {
			t.Errorf("package %q is in no role (%q): add the role to allowedGroupEdges or move the package", pkg, group)
			continue
		}
		for _, target := range targets {
			targetGroup := groupOf(target)
			edge := pkg + " -> " + target
			isAllowedGroup := false
			for _, candidate := range allowed {
				if candidate == targetGroup {
					isAllowedGroup = true
				}
			}
			if !isAllowedGroup {
				t.Errorf("%s: %s may not import %s", edge, group, targetGroup)
			}
			if forbiddenEdges[group+" -> "+target] {
				t.Errorf("%s is forbidden", edge)
			}
			isResourceToResource := group == "resource" && targetGroup == "resource"
			if isResourceToResource {
				_, listed := resourceEdgeAllowlist[edge]
				if !listed {
					t.Errorf("%s: a resource may use another only through resourceEdgeAllowlist", edge)
				}
				seenAllowlistRows[edge] = true
			}
		}
	}

	stale := []string{}
	for edge := range resourceEdgeAllowlist {
		if !seenAllowlistRows[edge] {
			stale = append(stale, edge)
		}
	}
	sort.Strings(stale)
	for _, edge := range stale {
		t.Errorf("resourceEdgeAllowlist names %s, which no longer exists: remove the row", edge)
	}
}
