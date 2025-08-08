# ouroboros-kv

Will provide a key-value store implementation in Go that handles in addition to basic CRUD operations, features like chunking, erasure coding, encryption, compression and parent-child/child-parent relationships between keys.


## Information for Developers

For the storage of values in ouroboros-kv, we use Badger as the underlying database with 2 levels of key-value pairs, the `metadata` and the finished `data shards`.

## Data Processing
The Data when stored is going to be processed through this pipeline in order, if it is retrieved the data will be reconstructed through the reversed pipeline.

- Chunking: The data is divided into predictable chunks to allow for deduplication via Buzhash (hashing by cyclic polynomial).
- Compression: The data is compressed via zstd.
- Encryption: The data is encrypted via AES-256-GCM using a once used ML-KEM1024 EncapsulatedKey.
- Erasure Coding: The data is encoded with Reed-Solomon coding.

### Storage in Badger

