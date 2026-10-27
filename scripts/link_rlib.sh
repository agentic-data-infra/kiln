#!/bin/bash
# Links an rlib checkout (https://github.com/agentic-data-infra/rlib) into
# the build as the Buck cell 'rlib', for the RDMA transport
# (-c kiln.rdma=true). Buck only takes cells inside the project, and only
# absolute symlinks out of it, so this makes third-party/rlib an absolute
# symlink to the checkout.
#
# Usage: scripts/link_rlib.sh [RLIB_DIR]   (default: ../rlib, next to kiln)

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
dir=${1:-$root/../rlib}
if [[ ! -f "$dir/rlib/endpoint/BUCK" ]]; then
  echo "No rlib checkout in $dir" >&2
  exit 1
fi
dir=$(cd "$dir" && pwd)
if [[ ! -f "$dir/third-party/generated/deps.bzl" ]]; then
  echo "Run $dir/scripts/gen_third_party.py first" >&2
  exit 1
fi
ln -sfn "$dir" "$root/third-party/rlib"
echo "third-party/rlib -> $dir"
