#!/bin/bash
# Copies the control plane's list of provider command kinds from one commit of OpenViCloud/titan-control-plane into
# contract/ProviderCommandKinds.cs and records that commit in contract/SOURCE. The routing test reads the copy; CI
# checks that the copy is exactly the file at contract/SOURCE.
# Usage: scripts/sync-kinds.sh <control-plane commit, tag or branch>
set -euo pipefail
ref=${1:?usage: scripts/sync-kinds.sh <control-plane commit, tag or branch>}
cd "$(dirname "$0")/.."
path=Titan.Domain/Aggregates/ProviderCommand/ProviderCommandKinds.cs
sha=$(gh api "repos/OpenViCloud/titan-control-plane/commits/$ref" --jq .sha)
gh api "repos/OpenViCloud/titan-control-plane/contents/$path?ref=$sha" \
  -H "Accept: application/vnd.github.raw" > contract/ProviderCommandKinds.cs
echo "$sha" > contract/SOURCE
echo "contract/ProviderCommandKinds.cs synced from OpenViCloud/titan-control-plane@$sha; mention it in the pull request"
