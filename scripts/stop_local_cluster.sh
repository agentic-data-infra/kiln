#!/usr/bin/env bash
# Stops the cluster scripts/start_local_cluster.sh started.
set -euo pipefail

cluster_dir=${KILN_CLUSTER_DIR:-${TMPDIR:-/tmp}/kiln-cluster}
[[ -f "$cluster_dir/pids" ]] || exit 0
# Storage servers first, so they remove their regions while the metadata
# server is still up.
tac "$cluster_dir/pids" | while read -r pid; do
  kill "$pid" 2>/dev/null || true
  while kill -0 "$pid" 2>/dev/null; do sleep 0.05; done
done
rm -f "$cluster_dir/pids"
