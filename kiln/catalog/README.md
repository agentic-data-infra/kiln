# kiln/catalog

The catalog: a client library on Kiln's public client API (`Client`), with
no access to the metadata server's internals. Its first version is memos:
`Catalog::co_putMemo(key, manifest)`, `co_getMemo(key)` and
`co_removeMemo(key)`.

- A manifest lists the chunks (or byte ranges of them) that hold a result,
  with an opaque schema, row and byte counts, and properties. Kiln
  interprets none of them, nor the keys.
- A manifest is stored as a chunk (`encodeManifest`: a magic, a format
  version, then the fields), written through to the local FS by default
  (`CatalogOptions::placement`).
- A memo is the ref `memo/<hash of the key>` pointing at its manifest's
  chunk; the manifest holds the key too, so keys can have any size. Memos
  are created once (compare and swap from no ref).
- `co_getMemo` returns a manifest only if it and all chunks it lists can be
  read. A memo whose chunks are gone is removed, so the caller recomputes
  and puts the result again; one whose chunks are only unavailable for now
  (after a restart, until their storage servers register again) is kept.
