# kiln
Storage for agents' data.

Kiln is a tiered chunk store in C++, in the style of
[Crail](https://github.com/apache/incubator-crail): data lives in chunks spread
over storage servers and tiers (memory and NVMe first; RDMA and S3 later), and
clients move chunks to and from servers directly while a metadata server keeps
track of where they are. On top of the chunks Kiln has refs, names that point
at chunks, and a git-style catalog of versioned tables, so agents that explore
data in many small, overlapping experiments can share, branch and reuse
results instead of copying them.

Chunks are content addressed: a chunk's id (`kiln/common/ChunkId.h`) is the
128-bit xxHash3 of its bytes. All I/O interfaces are `folly::coro::Task` based.

Kiln is in its first steps: today it has the build, chunk ids, and an
fbthrift RPC smoke test.

## Layout

- `kiln/common`: chunk ids and buffers.
- `kiln/rpc`: RPC between clients and servers, over fbthrift.
- `kiln/client`: the client library.
- `kiln/meta`: the metadata server.
- `kiln/server`: the storage server.
- `kiln/catalog`: refs and the catalog.

## Building

Kiln builds with [Buck2](https://buck2.build). Its dependencies (folly with
folly::coro, fbthrift and its `thrift1` compiler, glog, gflags) are the ones
Velox's setup script (`velox/scripts/setup-ubuntu.sh`) installs under
`/usr/local`, and the system gtest (`libgtest-dev`). They are wrapped as
prebuilt libraries; Kiln does not depend on Velox itself.

Prerequisites: GCC 11+, cmake, ninja, and buck2 on `PATH`.

```
# Generate third-party/generated/deps.bzl from the installed fbthrift.
scripts/gen_third_party.py

buck2 build //...
buck2 test //...
```

Re-run `scripts/gen_third_party.py` whenever folly or fbthrift are reinstalled.

Optional backends are build options in `.buckconfig`, off by default:
`-c kiln.rdma=true` and `-c kiln.s3=true` (no code behind them yet).

GCC 11 miscompiles `co_await` inside coroutine templates with the installed
folly, so Kiln's coroutines are not templates, and Kiln calls fbthrift clients'
`semifuture_*` methods from its coroutines rather than the generated `co_*`
ones (which are templates). Coroutine handlers (`co_*` overrides) work.
