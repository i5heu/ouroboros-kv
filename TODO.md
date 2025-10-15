# TODO

## Priority
- [ ] Enforce Terminology over codebase and docs
  - [ ] Rename the `Data.MetaData` to something that does not interfere with `ContentMeta` aka `KvDataHash` (Also renamed to `KvDataKey` to `ContentMeta`)
- [ ] Push encryption before Reed Solomon
  - [ ] New Buzhash Chunk based meta
    - [ ]  Nonce and EncapsulatedKey per Buzhash chunk
    - [ ]  refcounts per Buzhash chunk
- [ ] switch form SHA512 to keyed BLAKE3 for clear text hashing and unkeyed BLAKE3 for sealed data hashing - using https://github.com/lukechampine/blake3
  - [ ] ouroboros-crypt
  - [ ] ouroboros-kv


## Open Questions

### Deletion
We might not be able to use refcounts for deletion since the distributed nature, that should tolerate split brain scenarios, makes it near impossible to resync refcounts.  

A better approach might be to use a "tombstone" approach, where we write a tombstone entry for each deleted Buzhash Chunk Hash and periodically run a garbage collector with rate limits.  

That way we also do not need to keep `Slice` refcounts at all, we just have to know the `ChunkMeta` to `Slice` mapping, what could lead to more storage space usage if every `Slice` storing node needs to store the `ChunkMeta`.

Also we need to define how long the tombstones should be kept and how to handle situations when the **Tombstone Retention Period** is over but not all `Slice`s have been deleted yet - some kind of deep garbage collection.

### Meta Durability

Currently we store all `ContentMeta`s (and in the future `ChunkMeta`s) without erasure coding which begs the question how we should replicate them.
Since they are very often read, we could just replicate them to all nodes, but at 100TB of stored content, that would be 100GB of `ContentMeta`s per node, which is not too bad but also not great.

Potential size of `ChunkMeta` in Bits:

> 512 BLAKE3 hash of `ClearText`  
> 512 BLAKE3 hash of `ChunkMeta`
> 96 Nonce for AES-GCM
> 1568 EncapsulatedKey for ML-KEM1024
> 64 OriginalSize
> 64 StorageSize

512 + 512 + 96 + 1568 + 64 + 64 = 2816 bits 

For 100TB of data we assume an average chunk size of 230KB which leads to 434782609 chunks and thus with 2816 bits per `ChunkMeta` to a total of 142.5 GiB of `ChunkMeta`.

Since we need to be able to allow for small nodes, it is not an option to just replicate all `ChunkMeta`s to all nodes.
