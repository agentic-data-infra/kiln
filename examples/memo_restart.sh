#!/usr/bin/env bash
# Memos across a restart: starts a cluster with a journal, memos two results
# (examples/Memo.cpp), restarts the cluster, and looks them up again.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
export KILN_CLUSTER_DIR=${KILN_CLUSTER_DIR:-${TMPDIR:-/tmp}/kiln-memo-example}
rm -rf "$KILN_CLUSTER_DIR"
mkdir -p "$KILN_CLUSTER_DIR"
journal="$KILN_CLUSTER_DIR/journal"
memo=$(cd "$root" && buck2 build //examples:memo --show-full-output 2>/dev/null | awk '{print $2}')
start() { "$root/scripts/start_local_cluster.sh" -d 1 -l 1 -s 64 -p 7480 -j "$journal"; }

metadata=$(start)
echo "== put"
"$memo" --metadata="$metadata" put
echo "== get"
"$memo" --metadata="$metadata" get

echo "== restart the cluster"
"$root/scripts/stop_local_cluster.sh"
metadata=$(start)
echo "== get"
"$memo" --metadata="$metadata" get
echo "== get again"
"$memo" --metadata="$metadata" get
"$root/scripts/stop_local_cluster.sh"
