#!/usr/bin/env bash
# Builds Velox's dependencies (folly, fbthrift and the rest that Velox and
# Axiom need, so that Ember can share them) with GCC 12 into /opt/deps-gcc12,
# or $INSTALL_PREFIX, with Velox's own setup script from $VELOX_SRC (default
# ~/source/velox). /usr/local stays as it is. Sources go to $DEPENDENCY_DIR.
set -euo pipefail
export CC=gcc-12 CXX=g++-12 USE_CLANG=false
export INSTALL_PREFIX=${INSTALL_PREFIX:-/opt/deps-gcc12}
export DEPENDENCY_DIR=${DEPENDENCY_DIR:-$HOME/source/deps-gcc12-src}
if [[ ! -w "$INSTALL_PREFIX" ]]; then
  sudo mkdir -p "$INSTALL_PREFIX"
  sudo chown "$(id -u):$(id -g)" "$INSTALL_PREFIX"
fi
cd "${VELOX_SRC:-$HOME/source/velox}"
scripts/setup-ubuntu.sh \
  install_fmt install_protobuf install_grpc install_boost install_fast_float \
  install_flatbuffers install_openzl install_folly install_fizz install_wangle \
  install_mvfst install_fbthrift install_duckdb install_stemmer install_arrow \
  install_xsimd install_simdjson install_geos install_s2geometry
