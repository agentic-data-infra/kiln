# kiln
Storage for agents' data.

Kiln is a tiered chunk store in C++, in the style of
[Crail](https://github.com/apache/incubator-crail). Storage servers hold
blocks (1 MiB) inside large regions (1 GiB) of memory or local files; a
metadata server knows the regions and which blocks make up each chunk; and
clients read and write the blocks directly at the servers, at (region,
offset, length), so that with RDMA (later) servers spend no CPU on data. On
top of the chunks come refs, names that point at chunks, and a git-style
catalog, so agents that explore data in many small, overlapping experiments
can share and reuse results instead of copying them.

Chunks are immutable and content addressed: a chunk's id
(`kiln/common/ChunkId.h`) is the 128-bit xxHash3 of its bytes, computed by the
client as it writes them. The first commit of an id wins; a later writer of
the same bytes gets the same id and its copy is dropped. All I/O interfaces
are `folly::coro::Task` based.

Today Kiln has the chunk store core: the metadata server, DRAM and local-FS
storage servers (both ephemeral: their data is gone when they stop), and the
client, which reaches regions over TCP or, on the same host, by mapping their
files. Tiers fill up to the storage servers' byte budgets and then evict
their least recently used chunks. Refs are kept by the metadata server, in a
journal that survives restarts. An object store interface
(`kiln/objectstore`) has a local-directory backend. Write-through to durable
tiers and the catalog come next.

## Refs

A ref is a small named value (name up to 256 bytes, value up to 1 KiB) with
a version, e.g. a pointer to the chunk that holds a catalog's latest commit.
Refs are updated with compare and swap: `co_casRef(name, expectedVersion,
value)` sets the ref only if it is at `expectedVersion`, or, with
`std::nullopt`, only if there is no such ref, and returns the updated ref, or
`std::nullopt` if the version did not match. Versions grow with every update
of any ref, so a ref that is removed and created again never repeats one.
`co_listRefs(prefix)` lists refs by name, and `co_removeRef` removes one,
optionally only at a given version.

With `kiln_meta --journal=FILE`, every ref update is appended to the journal
and synced to disk before the call returns, and a restarted metadata server
replays it (and then rewrites it with just the current refs). Without a
journal, refs are gone when the metadata server stops.

## Eviction

Each storage server registers its regions with a byte budget (`kiln_server
--budget_mb`, default all of its regions). When an allocation would leave
less than 10% of a tier's budget free, the metadata server evicts the tier's
least recently committed or looked up chunks until 20% is free. An evicted
chunk is gone: `co_stat` and `co_read` fail with `kNotFound`.

The blocks of an evicted or removed chunk are reused only after a grace
period (`kiln_meta --grace_period_s`, default 30 s). Clients cache a chunk's
blocks for less than that (`ClientOptions::chunkCacheTtl`, default 10 s), so
a reader that found the chunk just before it went still reads its bytes, and
lookups after the TTL tell the metadata server which chunks are in use. An
allocation that only blocks in their grace period could satisfy waits for
them; one that even evicting everything cannot satisfy (the tier is full of
chunks being written) fails with `kOutOfSpace`.

## Using it

```cpp
kiln::client::Client client({.metadata = kiln::Endpoint::parse("localhost:7470")});

auto writer = co_await client.co_create({.tier = kiln::Tier::kDram});
auto buffer = client.allocateBuffer(8 << 20);
// ... fill the buffer, buffer.setSize(n) ...
buffer = co_await writer->co_append(std::move(buffer));  // as often as needed
kiln::ChunkId id = co_await writer->co_commit();

auto data = co_await client.co_read(id, offset, length, std::move(buffer));
```

A cluster on this machine, and the command line client:

```
metadata=$(scripts/start_local_cluster.sh -d 2 -l 1)   # 2 DRAM, 1 local-FS server; -b budget_mb, -j journal
kiln() { $(buck2 build //bin:kiln --show-full-output | awk '{print $2}') --metadata=$metadata "$@"; }
id=$(kiln put some/file)                # --tier=localfs for the local FS
kiln stat $id
kiln get $id copy                       # --tcp_only to skip shared memory
kiln rm $id
kiln ref set head $id                   # prints the ref's version
kiln ref set head $other_id VERSION     # only if head is still at VERSION
kiln ref get head
kiln ref ls
scripts/stop_local_cluster.sh
```

The examples and the benchmark run against a cluster in their own process
(`kiln/local/LocalCluster.h`, which the tests use too) unless given
`--metadata`:

```
buck2 run //examples:put_get_file -- some/file
buck2 run //examples:range_read
buck2 run //bench:transport_benchmark
```

The benchmark writes and reads a 256 MiB chunk. On a 16-core cloud VM, with
TCP over loopback, it measured:

| tier    | transport | write    | read     |
|---------|-----------|----------|----------|
| DRAM    | TCP       | 1.9 GB/s | 4.3 GB/s |
| DRAM    | shm       | 2.1 GB/s | 4.9 GB/s |
| local FS| TCP       | 2.9 GB/s | 4.6 GB/s |
| local FS| shm       | 1.9 GB/s | 5.0 GB/s |
| plain local file (pwrite/pread) | | 2.7 GB/s | 3.9 GB/s |

## Layout

- `kiln/common`: chunk ids, buffers, the metadata types (`Types.h`), errors.
- `kiln/rpc`: the metadata and storage interfaces (`MetadataRpc.h`,
  `StorageRpc.h`) and serving and calling them over fbthrift.
- `kiln/meta`: the metadata server's state and its journal.
- `kiln/objectstore`: the object store interface and its local-directory
  backend.
- `kiln/server`: the storage server and its regions.
- `kiln/client`: the client library and its transports.
- `kiln/local`: a whole cluster in one process, for tests, examples and
  benchmarks.
- `kiln/catalog`: refs and the catalog (to come).
- `bin`: `kiln_meta`, `kiln_server` and the `kiln` command line client.
- `examples`, `bench`.

## Building

Kiln builds with [Buck2](https://buck2.build). Its dependencies (folly with
folly::coro, fbthrift and its `thrift1` compiler, glog, gflags) are the ones
Velox's setup script (`velox/scripts/setup-ubuntu.sh`) installs under
`/usr/local`, and the system gtest (`libgtest-dev`). They are wrapped as
prebuilt libraries; Kiln does not depend on Velox itself.

Prerequisites: GCC 12 (`g++-12`), cmake, ninja, and buck2 on `PATH`.

```
# Generate third-party/generated/deps.bzl from the installed fbthrift.
scripts/gen_third_party.py

buck2 build //...
buck2 test //...
```

Re-run `scripts/gen_third_party.py` whenever folly or fbthrift are reinstalled.

Optional backends are build options in `.buckconfig`, off by default:
`-c kiln.rdma=true` and `-c kiln.s3=true` (no code behind them yet).

Kiln builds with GCC 12 although folly and fbthrift are built with GCC 11:
GCC 11 miscompiles `co_await` inside coroutine templates with the installed
folly, which rules out fbthrift's generated `co_*` clients.

GCC 12 has a coroutine bug of its own: inside a coroutine, it destroys some
temporaries in a `co_await` expression twice. It happens with a braced
aggregate that has a non-trivial destructor, e.g.
`co_await f(ChunkInfo{...})`, and with a `?:` that makes a temporary, e.g.
`co_await f(cond ? name : "")`. Name such arguments first. Function results,
constructors and trivially destructible aggregates like `Placement{}` are
fine.
