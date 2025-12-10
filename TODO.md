# TODOs

## Priority
- [ ] Prepare for distributed use.
- [ ] Quadruple check if the AES-GCM Nonce handling is correct and secure (never reuse Nonce with same key and how relevant that is with clear text hash)
- [ ] Quadruple check if cleartext hash leaks to many information and if yes how to mitigate that
- [ ] Deletions as in [Deletion Process](#deletion-process)
- [ ] Versioning of `Data.Content` 

## Done
- [x] Memory/Storage leak tests: Run long running tests that continuously write and delete data while monitoring memory and storage usage to detect potential leaks.


## Open Questions

### Data Structure Distributed Use and Meta Size
Currently we store all `ContentMeta`s (and in the future `Data`s) without erasure coding which begs the question how we should replicate them.
Since they are very often read, we could just replicate them to all nodes, but at 100TB of stored content, that would be 100GB of `ContentMeta`s per node, which is not too bad but also not great.

Potential size of `Data` in Bits:

> 512 BLAKE3 hash of `ClearText`  
> 512 BLAKE3 hash of `Data`
> 96 Nonce for AES-GCM
> 1568 EncapsulatedKey for ML-KEM1024
> 64 OriginalSize
> 64 StorageSize

512 + 512 + 96 + 1568 + 64 + 64 = 2816 bits 

For 100TB of data we assume an average chunk size of 230KB which leads to 434782609 chunks and thus with 2816 bits per `Data` to a total of 142.5 GiB of `Data`.

Since we need to be able to allow for small nodes, it is not an option to just replicate all `Data`s to all nodes.

That does mean that we have to be able to store `Data`s and `SealedSlice`s independently.
Consequences:
  - We will split the entire KV into a part that handles `SealedSlice`s
**
## Closed Questions

### Deletion Process
We might not be able to use refcounts for deletion since the distributed nature, that should tolerate split brain scenarios, makes it near impossible to resync refcounts.  

A better approach might be to use a "tombstone" approach, where we write a tombstone entry for each deleted Buzhash Chunk Hash and periodically run a garbage collector with rate limits.  

That way we also do not need to keep `Slice` refcounts at all, we just have to know the `Data` to `Slice` mapping, what could lead to more storage space usage if every `Slice` storing node needs to store the `Data`. 
We actually do not need to know the `Slice` to `Data` mapping since we can just check if a `Slice` is contained in any `Data` during garbage collection but this would make DHT lookups more important to run extremely fast.

Also we need to define how long the tombstones should be kept and how to handle situations when the **Tombstone Retention Period** is over but not all `Slice`s have been deleted yet - some kind of deep garbage collection.

**Answer:**

We will not handle distributed aspects in this KV.
If a deletion of `Data` is requested:
1. We check if the `Data` exists, if the `Data` has any `Children` or `Versions`. If yes, we return an error and do not delete anything.
2. If deletion request valid, we delete all `SealedSlice`s that are not referenced by any other `Data` right away and return what `SealedSlice`s where not deleted and why.