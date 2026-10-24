#!/usr/bin/env bash
# Starts a Kiln cluster on this machine in the background: kiln_meta plus
# storage servers, as separate processes as they would be on different hosts.
#
#   scripts/start_local_cluster.sh [-d dram_servers] [-l localfs_servers]
#       [-r regions_per_server] [-s region_size_mb] [-b budget_mb]
#       [-g grace_period_s] [-p metadata_port]
#
# Prints the metadata endpoint for `kiln --metadata=` (or $KILN_METADATA).
# Logs and pids go to $KILN_CLUSTER_DIR (default <tmp>/kiln-cluster); stop
# the cluster with scripts/stop_local_cluster.sh.
set -euo pipefail

dram=1
localfs=1
regions=1
region_size_mb=1024
budget_mb=0
grace_period_s=30
port=7470
while getopts "d:l:r:s:b:g:p:" opt; do
  case $opt in
    d) dram=$OPTARG ;;
    l) localfs=$OPTARG ;;
    r) regions=$OPTARG ;;
    s) region_size_mb=$OPTARG ;;
    b) budget_mb=$OPTARG ;;
    g) grace_period_s=$OPTARG ;;
    p) port=$OPTARG ;;
    *) exit 2 ;;
  esac
done

root=$(cd "$(dirname "$0")/.." && pwd)
cluster_dir=${KILN_CLUSTER_DIR:-${TMPDIR:-/tmp}/kiln-cluster}
if [[ -s "$cluster_dir/pids" ]]; then
  echo "A cluster seems to be running ($cluster_dir/pids); stop it first." >&2
  exit 1
fi
mkdir -p "$cluster_dir"
rm -f "$cluster_dir"/*.log

bin() {
  (cd "$root" && buck2 build "//bin:$1" --show-full-output 2>/dev/null | awk '{print $2}')
}
meta_bin=$(bin kiln_meta)
server_bin=$(bin kiln_server)

"$meta_bin" --port="$port" --grace_period_s="$grace_period_s" >"$cluster_dir/meta.log" 2>&1 &
echo $! >>"$cluster_dir/pids"

start_server() {
  local tier=$1 i=$2
  "$server_bin" --metadata="localhost:$port" --host=localhost --tier="$tier" \
    --regions="$regions" --region_size_mb="$region_size_mb" \
    --budget_mb="$budget_mb" \
    >"$cluster_dir/server-$tier-$i.log" 2>&1 &
  echo $! >>"$cluster_dir/pids"
}
for ((i = 0; i < dram; i++)); do start_server dram "$i"; done
for ((i = 0; i < localfs; i++)); do start_server localfs "$i"; done

# Wait until every storage server has registered.
servers=$((dram + localfs))
for _ in $(seq 100); do
  registered=$(cat "$cluster_dir"/server-*.log 2>/dev/null | grep -c "serves" || true)
  [[ "$registered" -ge "$servers" ]] && break
  sleep 0.1
done
if [[ "$registered" -lt "$servers" ]]; then
  echo "Only $registered of $servers storage servers registered; see $cluster_dir" >&2
  exit 1
fi
echo "localhost:$port"
