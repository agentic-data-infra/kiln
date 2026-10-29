# kiln
Storage for agents' data.

Kiln is a high-performance, tiered, distributed chunk store written in C++,
designed to be used by AI agents. Storage servers hold
blocks (1 MiB) inside large regions (1 GiB) of memory or local files; a
metadata server knows the regions and which blocks make up each chunk; and
clients read and write the blocks directly at the servers, at (region,
offset, length), so that with RDMA servers spend no CPU on data. On
top of the chunks come refs, names that point at chunks, and a git-style
catalog, so agents that explore data in many small, overlapping experiments
can share and reuse results instead of copying them.

Chunks are immutable and content addressed: a chunk's id
(`kiln/common/ChunkId.h`) is the 128-bit xxHash3 of its bytes, computed by the
client as it writes them. The first commit of an id wins; a later writer of
the same bytes gets the same id and its copy is dropped. All I/O interfaces
are `folly::coro::Task` based.

Today Kiln has the metadata server; DRAM and local-FS storage servers; an
object store interface (`kiln/objectstore`) with a local-directory backend;
and the client, which reaches regions over TCP, with one-sided RDMA (DRAM
regions, in a build with RDMA), or, on the same host, by mapping their
files. Tiers fill up to the storage servers' byte budgets and
then evict their least recently used chunks. Chunks can be written through
to a durable tier, and refs and written-through chunks survive restarts.
The catalog keeps memos of results (below).

## Tiers and write-through

A chunk goes into DRAM or the local FS (`Placement::tier`), and, if
`Placement::writeThrough` is set, also into a second tier before its commit
returns: a durable tier (`isDurable`), the local FS, or the object store
(`ClientOptions::objectStore`), where it is the object `chunks/<id>`. The client writes both copies, and
reads the fastest one it can reach (`co_stat` lists them).

```cpp
auto writer = co_await client.co_create(
    kiln::kDefaultBucket,
    {.tier = kiln::Tier::kDram, .writeThrough = kiln::Tier::kLocalFs});
```

With `Placement::overflow`, a chunk goes into its tier only if it fits into
the blocks free there, without evicting anything; else all of it goes into
the overflow tier, which evicts as usual. The metadata server checks and
takes the blocks in one step, so concurrent writers never see a stale
"full". The decision is made by the chunk's first allocation, so give
`co_create` the chunk's size when you know it: the first append then
allocates blocks for all of it (blocks left over are freed at commit).

```cpp
auto writer = co_await client.co_create(
    kiln::kDefaultBucket,
    {.tier = kiln::Tier::kDram, .overflow = kiln::Tier::kLocalFs},
    size);
```

Eviction drops a chunk's copy in one tier; the chunk is gone when it has no
copy left. A written-through chunk whose DRAM copy was evicted is still read
from its other copy, and writing the same bytes again brings the DRAM copy
back.

With a journal (`kiln_meta --journal=FILE`), the metadata server keeps the
copies of written-through chunks in their write-through tier, along with
refs and local-FS regions, across restarts. A local-FS storage server with
its own directory (`kiln_server --dir`) keeps its region files when it stops
and reattaches them when it starts; storage servers register again every few
seconds, so a restarted metadata server learns of them. Until a chunk's
storage server has registered again, the chunk reads as `kUnavailable`.
Chunks that are not written through, and all of DRAM, are gone after a
restart. Objects of removed chunks stay in the object store for now.

## RDMA

Kiln can move data and metadata calls over RDMA, on
[rlib](https://github.com/agentic-data-infra/rlib). Data uses one-sided
RDMA reads and writes, so the storage server's CPU does no work for them and
the bytes go straight between the region and the caller's buffer, with no
copy on either side. Metadata calls use rlib's RPC. Servers always serve TCP
and add RDMA when asked; each client chooses what to use.

### Building

RDMA is a build option: `-c kiln.rdma=true`, after linking an rlib checkout
with `scripts/link_rlib.sh` (see Building). In a build without it,
`kiln_meta --rdma` fails to start, storage servers serve TCP only with a
warning, and clients use TCP.

### Servers

- `kiln_meta --rdma` also answers metadata calls over RDMA (`--rdma_port`,
  7471 by default) and logs its URLs, e.g.
  `tcp://meta:7470,rdma://10.0.0.1:7471`. It fails to start if it cannot
  serve RDMA.
- `kiln_server --rdma` registers its DRAM regions with the NIC and publishes
  each region's address and key in its `RegionInfo` (`RdmaAccess`).
  Local-FS regions are served over TCP only.
- Both serve RDMA on the first interface with an RDMA device, or on
  `--rdma_host`.
- Registering memory pins it, up to the locked memory limit (`ulimit -l`).
  A storage server whose limit does not fit its regions, or that has no RDMA
  device, serves TCP only, with a warning.
- Storage servers register with the metadata server over TCP;
  `kiln_server --metadata` takes the same URL list as clients.

### Clients

`ClientOptions::metadata` (`kiln --metadata`) is the URL list `kiln_meta`
logs. A bare `host:port` means `tcp://`.

- **Metadata calls:** if the client has an RDMA device and the list has an
  `rdma://` URL, all metadata calls go there; otherwise they go to the
  `tcp://` URL. If neither is usable, creating the client fails with
  `kInvalidArgument`. The choice is made once, when the client is created:
  if the RDMA metadata server cannot be reached later, calls fail with
  `kUnavailable` rather than go over TCP.
- **Data,** region by region: shared memory for servers on the client's own
  host, then RDMA if the region offers it and the client has an RDMA
  device, then TCP.

Switches:

| option | `kiln` flag | effect |
|---|---|---|
| `metaForceTcp` | `--meta_force_tcp` | metadata calls over TCP, even with an `rdma://` URL |
| `dataForceTcp` | `--data_force_tcp` | data over TCP only: no RDMA, no shared memory |
| `useShm = false` | `--no_shm` | no shared memory, so that RDMA is used on one host too |

`Client::stats()` tells what a client chose (`rdmaMetadata`, `rdmaData`)
and how many bytes went over RDMA, straight from registered buffers
(`rdmaBytes`) or through a copy (`rdmaCopiedBytes`).

For zero copy, the buffer must be registered with the NIC too: buffers from
`Client::allocateBuffer` and `Client::registerMemory` are, until they go.
Registering takes time, so keep a pool of buffers and reuse them. Other
memory works, but goes through a copy, with a warning.

```cpp
kiln::client::Client client({.metadata = kiln::MetadataAddress::parse(urls)});
auto buffer = client.allocateBuffer(64 << 20);    // registered once
buffer = co_await client.co_read(id, 0, 64 << 20, std::move(buffer));
```

### A cluster on one machine

`scripts/start_local_cluster.sh -R` builds with RDMA, starts `kiln_meta` and
the storage servers with `--rdma`, and prints the URL list. Use
`kiln --no_shm` with it, as otherwise shared memory is used on one host.
`LocalClusterOptions::rdma` does the same for clusters in one process
(`LocalCluster::metadataAddress()`).

### Message size

RDMA metadata messages have a fixed size of 64 KiB. A chunk takes about 12
bytes per block per copy, so chunks up to about 5 GiB fit (2.5 GiB written
through to a second tier). Calls whose request or response does not fit
fail with `kTooLarge`. The client asks for refs (`co_listRefs`) and regions
in pages that fit.

### Soft-RoCE

Without an RDMA NIC, rlib's `scripts/setup-rxe.sh` sets up Soft-RoCE (rxe)
on an Ethernet interface; then everything above works, and the tests that
need a device run instead of skipping. Soft-RoCE runs in the kernel and
drops packets under load, so Kiln sets RDMA connections' retry counts to the
maximum (7); its numbers in the benchmarks under Using it say little about a real NIC.

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
replays it (and then rewrites it with just its current state). Without a
journal, refs are gone when the metadata server stops.

## Buckets

A bucket groups chunks that are removed together, e.g. those one job
writes; every chunk is in one, `co_create`'s first argument. Chunks in the
default bucket (`kDefaultBucket`), which cannot be deleted, stay until they
are removed or evicted. `co_createBucket()` returns a new bucket's id (ids
are never reused), and `co_deleteBucket(id)` removes the bucket and all its
chunks in one call.
Their blocks are reusable at once, not after the grace period (see
Eviction): the caller knows nobody reads them anymore. `co_deleteBucket(id,
grace)` waits `grace` (at most the grace period) instead.
A commit into a bucket that does not exist, e.g. by a late task of a job
whose bucket is deleted already, fails with `kNotFound` and frees the
chunk's blocks. The bucket is hashed into a chunk's id, so equal bytes in
two buckets are two chunks, and deleting one bucket never takes another's
chunk. Buckets are journaled like refs.

```cpp
const auto bucket = co_await client.co_createBucket();
auto writer = co_await client.co_create(bucket);
// ... append and commit chunks ...
co_await client.co_deleteBucket(bucket);
```

## Eviction

Each storage server registers its regions with a byte budget (`kiln_server
--budget_mb`, default all of its regions). When an allocation would leave
less than 10% of a tier's budget free, the metadata server evicts the tier's
least recently committed or looked up chunks there until 20% is free. A
chunk evicted from its only tier is gone: `co_stat` and `co_read` fail with
`kNotFound`.

The blocks of an evicted or removed chunk are reused only after a grace
period (`kiln_meta --grace_period_s`, default 30 s). Clients cache a chunk's
blocks for less than that (`ClientOptions::chunkCacheTtl`, default 10 s), so
a reader that found the chunk just before it went still reads its bytes, and
lookups after the TTL tell the metadata server which chunks are in use. An
allocation that only blocks in their grace period could satisfy waits for
them; one that even evicting everything cannot satisfy (the tier is full of
chunks being written) fails with `kOutOfSpace`.

## Memos

The catalog (`kiln/catalog`) is a client library on the public client API.
Its memos map a key, e.g. a query plan's fingerprint, to a manifest: the
chunks (or byte ranges of them) that hold the result, an opaque schema, row
and byte counts, and properties.

```cpp
kiln::catalog::Catalog catalog(client);
if (auto manifest = co_await catalog.co_getMemo(key)) {
  // Read manifest->chunks.
} else {
  // Compute the result into chunks, then:
  kiln::catalog::Manifest computed{.chunks = {{.id = id}}, .rows = n, .bytes = size};
  co_await catalog.co_putMemo(key, std::move(computed));
}
```

A manifest is a chunk written through to the local FS, and a memo is the
ref `memo/<hash of the key>` pointing at it, created once. `co_getMemo`
returns a manifest only if all chunks it lists can still be read; a memo
whose chunks are gone (evicted, or lost with DRAM in a restart) is removed,
so the result is recomputed and memoed again. `examples/memo_restart.sh`
shows a memo of written-through data surviving a cluster restart, and one of
DRAM-only data turning into a miss.

## Using it

```cpp
kiln::client::Client client({.metadata = kiln::Endpoint::parse("localhost:7470")});

auto writer = co_await client.co_create(
    kiln::kDefaultBucket, {.tier = kiln::Tier::kDram});
auto buffer = client.allocateBuffer(8 << 20);
// ... fill the buffer, buffer.setSize(n) ...
buffer = co_await writer->co_append(std::move(buffer));  // as often as needed
kiln::ChunkId id = co_await writer->co_commit();

auto data = co_await client.co_read(id, offset, length, std::move(buffer));
```

A cluster on this machine, and the command line client:

```
metadata=$(scripts/start_local_cluster.sh -d 2 -l 1)   # 2 DRAM, 1 local-FS server; -b budget_mb, -j journal, -R RDMA
kiln() { $(buck2 build //bin:kiln --show-full-output | awk '{print $2}') --metadata=$metadata "$@"; }
id=$(kiln put some/file)                # --tier=localfs for the local FS
kiln --write_through=localfs put some/file
kiln --object_store=/some/dir --write_through=objectstore put some/file
kiln stat $id
kiln get $id copy                       # --no_shm to skip shared memory (RDMA with -R)
kiln --data_force_tcp get $id copy      # data over TCP only
kiln --meta_force_tcp stat $id          # metadata over TCP despite an rdma:// URL
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
examples/memo_restart.sh                 # starts and stops its own cluster
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

Built with RDMA, it also measures DRAM over RDMA. Soft-RoCE (rxe0 on the same
VM, where perftest's `ib_read_bw` and `ib_write_bw` get 0.62 and 0.87 GB/s)
runs RDMA in the kernel, so it says little about a real NIC:

| tier | transport | write    | read     |
|------|-----------|----------|----------|
| DRAM | RDMA (rxe)| 0.8 GB/s | 0.87 GB/s |

`buck2 run -c kiln.rdma=true //bench:metadata_benchmark` measures metadata
calls one at a time, over TCP (loopback) and over RDMA (rxe0), on the same
VM:

| call   | TCP p50 / p99 | RDMA p50 / p99 |
|--------|---------------|----------------|
| lookup | 63 / 78 us    | 16 / 30 us     |
| getRef | 62 / 76 us    | 16 / 30 us     |
| commit | 65 / 79 us    | 16 / 30 us     |

## Layout

- `kiln/common`: chunk ids, buffers, the metadata types (`Types.h`), errors.
- `kiln/rpc`: the metadata and storage interfaces (`MetadataRpc.h`,
  `StorageRpc.h`) and serving and calling them over fbthrift.
- `kiln/meta`: the metadata server's state and its journal.
- `kiln/objectstore`: the object store interface and its local-directory
  backend.
- `kiln/server`: the storage server and its regions.
- `kiln/client`: the client library and its transports.
- `kiln/rdma`: RDMA support on rlib (with `kiln.rdma`): the device and
  memory registration, and the metadata calls over rlib's RPC. The RDMA transport is in `kiln/client`, the serving
  of regions in `kiln/server`.
- `kiln/local`: a whole cluster in one process, for tests, examples and
  benchmarks.
- `kiln/catalog`: the catalog's memos, on the public client API.
- `bin`: `kiln_meta`, `kiln_server` and the `kiln` command line client.
- `examples`, `bench`.

## Building

Kiln builds with [Buck2](https://buck2.build) and GCC 12. Its dependencies
(folly with folly::coro, fbthrift and its `thrift1` compiler) are the ones
Velox's setup script (`velox/scripts/setup-ubuntu.sh`) builds, built with GCC
12 into `/opt/deps-gcc12` by `scripts/build_deps_gcc12.sh`; glog, gflags and
gtest come from the system (`libgoogle-glog-dev`, `libgflags-dev`,
`libgtest-dev`). They are wrapped as prebuilt libraries; Kiln does not depend
on Velox itself.

Prerequisites: GCC 12 (`g++-12`), cmake, ninja, and buck2 on `PATH`.

```
# Once: build the dependencies (an hour or two) into /opt/deps-gcc12.
scripts/build_deps_gcc12.sh

# Generate third-party/generated/deps.bzl from them (--prefix for another place).
scripts/gen_third_party.py

buck2 build //...
buck2 test //...
```

Re-run `scripts/gen_third_party.py` whenever the dependencies are rebuilt.

Optional backends are build options in `.buckconfig`, off by default:
`-c kiln.rdma=true` and `-c kiln.s3=true` (no code behind it yet).

The RDMA transport is built on [rlib](https://github.com/agentic-data-infra/rlib),
a separate repository that Buck reaches as the cell `rlib`: a symlink at
`third-party/rlib` to an rlib checkout (by default `../rlib`, next to Kiln),
with its own `third-party/generated/deps.bzl` from the same `/opt/deps-gcc12`.
It also needs rdma-core (`libibverbs-dev`, `librdmacm-dev`). Without
`kiln.rdma`, nothing uses rlib, and the symlink need not exist.

```
(cd ../rlib && scripts/gen_third_party.py)
scripts/link_rlib.sh                     # or scripts/link_rlib.sh <rlib dir>
buck2 test -c kiln.rdma=true //...
```

Tests that need an RDMA device skip themselves when there is none; see
RDMA for Soft-RoCE.

Kiln needs GCC 12: GCC 11 miscompiles `co_await` inside coroutine templates
with this folly, which rules out fbthrift's generated `co_*` clients. folly
must be built with the same compiler, as its class layouts differ between
GCC 11 and 12.

GCC 12 has a coroutine bug of its own: inside a coroutine, it destroys some
temporaries in a `co_await` expression twice. It happens with a braced
aggregate that has a non-trivial destructor, e.g.
`co_await f(ChunkInfo{...})`, and with a `?:` that makes a temporary, e.g.
`co_await f(cond ? name : "")`. Name such arguments first. Function results,
constructors and trivially destructible aggregates like `Placement{}` are
fine.
