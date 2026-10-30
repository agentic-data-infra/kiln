# kiln/catalog

The catalog: a client library on Kiln's public client API (`Client`), with
no access to the metadata server's internals. It has memos
(`Catalog::co_putMemo(key, manifest)`, `co_getMemo(key)`,
`co_removeMemo(key)`) and datasets (`co_publishDataset(name, manifest,
expectedVersion)`, `co_getDataset(name)`, `co_listDatasets(prefix)`,
`co_deleteDataset(name)`), both refs pointing at manifests.

- A manifest lists the chunks (or byte ranges of them) that hold a result
  or a dataset's files, with an opaque schema, row and byte counts, and
  properties. Kiln interprets none of them, nor the keys. Entries can have
  a name, a row count, and a kind: data, or another manifest.
- A manifest is stored as a chunk (`encodeManifest`: a magic, a format
  version, then the fields; format 1, without entry names, kinds, rows and
  bucket, still reads), written through to the local FS by default
  (`CatalogOptions::placement`), in `kDefaultBucket`.
- A memo is the ref `memo/<hash of the key>` pointing at its manifest's
  chunk; the manifest holds the key too, so keys can have any size. Memos
  are created once (compare and swap from no ref).
- `co_getMemo` returns a manifest only if it and all chunks it lists can be
  read. A memo whose chunks are gone is removed, so the caller recomputes
  and puts the result again; one whose chunks are only unavailable for now
  (after a restart, until their storage servers register again) is kept.
- A dataset is the ref `data/<name>` pointing at a manifest whose entries
  are its files (name, chunk, length). `co_publishDataset` writes a new
  manifest and swaps the ref if it is at the expected version (or creates
  it), so a version appears at once; a publish that loses the race removes
  its manifest again. The manifest may name the bucket of its chunks, which
  `co_deleteDataset` deletes with the ref and the manifest. `co_getDataset`
  does not check the chunks: readers find out when they read them.
