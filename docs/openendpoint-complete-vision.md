# OpenEndpoint — The Developer-First Object Storage Platform

## Complete Product & Architecture Vision

**Organization:** github.com/OpenEndpoint
**Primary Language:** Go 1.22+
**Dashboard:** React/Next.js (separate repo)
**Target:** Self-hosted MinIO alternative, developer-first
**Tagline:** *"Your endpoints. Your data. Your rules."*

---

## Table of Contents

1. [Product Vision & Positioning](#1-product-vision--positioning)
2. [Release Strategy (v1 → v5)](#2-release-strategy)
3. [System Architecture Overview](#3-system-architecture-overview)
4. [Core Engine Deep Dive](#4-core-engine-deep-dive)
5. [Storage Backends](#5-storage-backends)
6. [Metadata Layer](#6-metadata-layer)
7. [S3 API Implementation](#7-s3-api-implementation)
8. [Authentication & Authorization](#8-authentication--authorization)
9. [Multi-Node Clustering](#9-multi-node-clustering)
10. [Multi-Region Federation](#10-multi-region-federation)
11. [Replication, Backup & Mirror](#11-replication-backup--mirror)
12. [CDN Integration & Edge](#12-cdn-integration--edge)
13. [Web Dashboard (Next.js)](#13-web-dashboard-nextjs)
14. [CLI Tool](#14-cli-tool)
15. [Observability & Monitoring](#15-observability--monitoring)
16. [Security Architecture](#16-security-architecture)
17. [Performance Engineering](#17-performance-engineering)
18. [Deployment & Operations](#18-deployment--operations)
19. [SDK & Developer Experience](#19-sdk--developer-experience)
20. [Repository Structure](#20-repository-structure)
21. [Competitive Analysis](#21-competitive-analysis)
22. [Implementation Roadmap](#22-implementation-roadmap)

---

## 1. Product Vision & Positioning

### What is OpenEndpoint?

OpenEndpoint is a **self-hosted, S3-compatible object storage platform** built from scratch in Go. It's designed for developers and teams who need complete control over their data without vendor lock-in.

### Why Not MinIO?

| Pain Point | MinIO | OpenEndpoint |
|-----------|-------|-------------|
| License | AGPLv3 (commercial license $$) | Apache 2.0 |
| Architecture | Monolithic, opinionated | Pluggable backends, modular |
| Web UI | Basic, limited | Full-featured React dashboard |
| Multi-region | Enterprise only ($$$) | Built-in, open source |
| CDN integration | Manual | Native CDN edge support |
| Developer experience | CLI-focused | CLI + Dashboard + SDKs |
| Backup/Mirror | Limited | First-class backup targets |
| Storage backends | Filesystem only | Flat file + Packed volumes + extensible |
| Clustering | Erasure coding only | Replication + Erasure coding |

### Target Users

1. **Solo developers / small teams** — Self-hosted S3 for side projects, media storage
2. **Startups** — Production object storage without AWS bills
3. **On-prem enterprises** — Data sovereignty, compliance requirements
4. **Platform builders** — White-label storage backend for SaaS products
5. **Edge/IoT** — Lightweight storage nodes at edge locations

### Core Principles

- **Single binary, zero external deps** — Download, run, done
- **S3 compatibility first** — Drop-in replacement for AWS S3
- **Pluggable everything** — Storage, metadata, auth, all swappable
- **Observable by default** — Metrics, logs, traces out of the box
- **Developer joy** — Beautiful CLI, intuitive dashboard, great docs

---

## 2. Release Strategy

### v1.0 — "Foundation" (Single Node)
> A solid, production-ready single-node object storage

- S3 API core (CRUD, Multipart, ListV2)
- AWS Signature V4 authentication
- Object versioning
- Lifecycle policies (expiration, noncurrent cleanup)
- Two storage backends (Flat File + Packed Volume)
- Two metadata backends (Pebble + bbolt)
- Prometheus metrics + health endpoints
- CLI tool (openep)
- Docker image + Helm chart
- Basic Web Dashboard (browse buckets, upload/download)

### v2.0 — "Cluster" (Multi-Node)
> Scale horizontally within a datacenter

- Node discovery & membership (gossip protocol)
- Consistent hashing for data placement
- Configurable replication (RF=1 to RF=5)
- Erasure coding (Reed-Solomon)
- Automatic rebalancing on node join/leave
- Cluster-aware Web Dashboard
- Node health monitoring & alerting
- Rolling upgrades (zero-downtime)
- Backup to external targets (S3, GCS, Azure Blob, NFS)
- Mirror mode (continuous replication to another cluster)

### v3.0 — "Federation" (Multi-Region)
> Span multiple datacenters and edge locations

- Multi-region federation protocol
- Geo-aware data placement
- Cross-region async replication
- Conflict resolution (vector clocks / CRDTs)
- Region-aware routing (read from nearest)
- CDN edge integration (presigned URL delegation)
- CDN cache invalidation API
- Global namespace with region affinity
- Bandwidth throttling between regions
- WAN-optimized transfer (compression, dedup)

### v4.0 — "Platform" (Enterprise Features)
> Enterprise-grade features for large organizations

- Multi-tenancy with resource isolation
- IAM system (users, groups, policies, roles)
- Bucket policies (S3-compatible JSON policies)
- Server-side encryption (SSE-S3, SSE-C, SSE-KMS)
- Object Lock (WORM compliance)
- S3 Event Notifications (webhook, NATS, Kafka, AMQP)
- Audit logging (immutable audit trail)
- Compliance reporting (GDPR, HIPAA data residency)
- SLA monitoring & automated failover
- White-label dashboard (custom branding)
- LDAP / OIDC authentication integration

### v5.0 — "Intelligence" (Smart Storage)
> AI-powered storage optimization and analytics

- S3 Select (SQL queries on CSV/JSON/Parquet)
- Intelligent tiering (hot/warm/cold/archive)
- Content-aware deduplication
- Automatic thumbnail generation for images
- Full-text search on stored documents
- Storage analytics & cost optimization
- Predictive scaling recommendations
- Data pipeline integration (Spark, Flink connectors)
- Lambda-style object transformations
- GraphQL API (alongside S3 REST)

---

## 3. System Architecture Overview

### Single Node (v1)

```
                    ┌──────────────────────────────────┐
                    │           Load Balancer           │
                    │        (nginx/caddy/traefik)      │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │        OpenEndpoint Node          │
                    │                                   │
                    │  ┌─────────────────────────────┐  │
                    │  │     S3 API Gateway           │  │
                    │  │   (HTTP/HTTPS :9000)         │  │
                    │  └─────────────┬───────────────┘  │
                    │                │                   │
                    │  ┌─────────────▼───────────────┐  │
                    │  │     Auth Middleware          │  │
                    │  │   (SigV4 + API Keys)        │  │
                    │  └─────────────┬───────────────┘  │
                    │                │                   │
                    │  ┌─────────────▼───────────────┐  │
                    │  │      Core Engine             │  │
                    │  │  ┌──────────┬──────────┐    │  │
                    │  │  │ Object   │Lifecycle │    │  │
                    │  │  │ Service  │ Engine   │    │  │
                    │  │  └────┬─────┴─────┬────┘    │  │
                    │  └───────┼────────────┼────────┘  │
                    │          │            │            │
                    │  ┌───────▼──┐  ┌──────▼────────┐  │
                    │  │ Metadata │  │   Storage     │  │
                    │  │  Store   │  │   Backend     │  │
                    │  │(Pebble)  │  │(Flat/Packed)  │  │
                    │  └──────────┘  └───────────────┘  │
                    │                                   │
                    │  ┌─────────────────────────────┐  │
                    │  │  Internal API (:9001)        │  │
                    │  │  Prometheus + Health + pprof │  │
                    │  └─────────────────────────────┘  │
                    └───────────────────────────────────┘
```

### Multi-Node Cluster (v2)

```
                         ┌───────────────────┐
                         │   Load Balancer    │
                         │   (L7 / DNS RR)    │
                         └─────────┬─────────┘
                    ┌──────────────┼──────────────┐
                    │              │              │
             ┌──────▼─────┐┌──────▼─────┐┌──────▼─────┐
             │   Node 1    ││   Node 2    ││   Node 3    │
             │  :9000      ││  :9000      ││  :9000      │
             │             ││             ││             │
             │ S3 API ─────┤│ S3 API ─────┤│ S3 API      │
             │ Engine      ││ Engine      ││ Engine      │
             │ Meta+Store  ││ Meta+Store  ││ Meta+Store  │
             └──────┬──────┘└──────┬──────┘└──────┬──────┘
                    │              │              │
                    └──────────────┼──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │     Gossip Mesh Network      │
                    │   (memberlist, port :9002)    │
                    │                              │
                    │  • Node discovery            │
                    │  • Health checking            │
                    │  • Metadata propagation       │
                    │  • Consistent hash ring       │
                    └──────────────────────────────┘

Data Flow (PUT with RF=3):
  Client → Node1 (coordinator)
    → Node1 writes locally
    → Node1 forwards to Node2 (async or sync based on consistency level)
    → Node1 forwards to Node3
    → All 3 ACK → 200 OK to client

Consistency Levels:
  • ONE    — 1 write ACK (fastest, risk of data loss)
  • QUORUM — majority ACK (balanced)
  • ALL    — all replicas ACK (strongest, slowest)
```

### Multi-Region Federation (v3)

```
                          ┌─────────────────────────┐
                          │    Global DNS / GeoDNS   │
                          │  (Route53 / Cloudflare)  │
                          └────────────┬────────────┘
                ┌──────────────────────┼──────────────────────┐
                │                      │                      │
     ┌──────────▼──────────┐┌──────────▼──────────┐┌──────────▼──────────┐
     │   Region: EU-West    ││  Region: US-East    ││  Region: AP-South   │
     │   (Frankfurt)        ││  (Virginia)         ││  (Mumbai)           │
     │                      ││                      ││                      │
     │  ┌────────────────┐  ││  ┌────────────────┐  ││  ┌────────────────┐  │
     │  │  Cluster (3N)  │  ││  │  Cluster (5N)  │  ││  │  Cluster (3N)  │  │
     │  │  ┌──┐┌──┐┌──┐ │  ││  │ ┌──┐┌──┐┌──┐  │  ││  │  ┌──┐┌──┐┌──┐ │  │
     │  │  │N1││N2││N3│ │  ││  │ │N1││N2││..│  │  ││  │  │N1││N2││N3│ │  │
     │  │  └──┘└──┘└──┘ │  ││  │ └──┘└──┘└──┘  │  ││  │  └──┘└──┘└──┘ │  │
     │  └────────────────┘  ││  └────────────────┘  ││  └────────────────┘  │
     │                      ││                      ││                      │
     │  ┌────────────────┐  ││  ┌────────────────┐  ││  ┌────────────────┐  │
     │  │ Region Gateway  │  ││  │ Region Gateway  │  ││  │ Region Gateway  │  │
     │  │ :9003           │  ││  │ :9003           │  ││  │ :9003           │  │
     │  └───────┬────────┘  ││  └───────┬────────┘  ││  └───────┬────────┘  │
     └──────────┼───────────┘└──────────┼───────────┘└──────────┼───────────┘
                │                       │                       │
                └───────────────────────┼───────────────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │   Federation Control Plane   │
                         │                              │
                         │  • Region registry           │
                         │  • Replication policy engine  │
                         │  • Conflict resolution        │
                         │  • Bandwidth management       │
                         │  • Global metadata index      │
                         └──────────────────────────────┘

Replication Modes:
  ┌─────────────────────────────────────────────────────┐
  │ ASYNC  — Write to local → ACK → replicate later     │
  │          Best for: High throughput, eventual consistency│
  │                                                       │
  │ SEMI   — Write to local + 1 remote → ACK             │
  │          Best for: Balance of speed and durability     │
  │                                                       │
  │ SYNC   — Write to ALL regions → ACK                   │
  │          Best for: Critical data, strong consistency   │
  └─────────────────────────────────────────────────────┘
```

### Full Platform with CDN (v3+)

```
                              End Users
                           ┌─────┴─────┐
                           │            │
                    ┌──────▼──┐   ┌─────▼───┐
                    │  CDN     │   │  CDN     │
                    │  Edge    │   │  Edge    │
                    │  (PoP)   │   │  (PoP)   │
                    └────┬─────┘   └────┬─────┘
                         │              │
              Cache Miss │    Cache Miss │
                         │              │
                    ┌────▼──────────────▼────┐
                    │   CDN Origin Shield     │
                    │   (optional, reduces    │
                    │    origin load)          │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   OpenEndpoint           │
                    │   Origin Cluster         │
                    │                          │
                    │   /cdn/v1/{bucket}/{key}  │
                    │   • Presigned URL gen     │
                    │   • Cache-Control headers │
                    │   • Range request support │
                    │   • Conditional requests  │
                    │     (ETag, Last-Modified) │
                    └──────────────────────────┘
```

---

## 4. Core Engine Deep Dive

### ObjectService — The Orchestrator

```go
package engine

// ObjectService is the central orchestrator.
// It coordinates between metadata, storage, auth, versioning,
// and lifecycle — but contains NO storage logic itself.
type ObjectService struct {
    meta       metadata.MetadataStore
    storage    storage.StorageBackend
    locker     *ShardedLocker
    lifecycle  *LifecycleEngine
    placement  cluster.PlacementStrategy  // v1: LocalPlacement
    registry   cluster.NodeRegistry       // v1: SingleNodeRegistry
    replicator *Replicator                // v2: handles cross-node replication
    metrics    *Metrics
    logger     *zap.Logger
}

// PutObject — The complete write path
func (s *ObjectService) PutObject(ctx context.Context, req PutObjectRequest) (PutObjectResponse, error) {
    // 1. Validate request
    if err := validateBucketName(req.Bucket); err != nil {
        return PutObjectResponse{}, err
    }
    if err := validateObjectKey(req.Key); err != nil {
        return PutObjectResponse{}, err
    }

    // 2. Check bucket exists and get versioning state
    bucket, err := s.meta.GetBucket(ctx, req.Bucket)
    if err != nil {
        return PutObjectResponse{}, ErrNoSuchBucket
    }

    // 3. Determine placement (v1: always local)
    nodes, err := s.placement.PlaceObject(req.Bucket, req.Key, bucket.ReplicationFactor)
    if err != nil {
        return PutObjectResponse{}, err
    }

    // 4. Acquire per-object lock
    unlock := s.locker.Lock(req.Bucket, req.Key)
    defer unlock()

    // 5. Generate version ID
    versionID := ""
    if bucket.VersioningEnabled {
        versionID = generateUUIDv7()
    }

    // 6. Hash the data while writing (streaming, no buffering)
    hashReader := NewHashingReader(req.Body) // computes MD5 + SHA256 on the fly

    // 7. Write to storage backend
    timer := s.metrics.StorageWriteDuration.Start()
    storageID, err := s.storage.Put(ctx, hashReader, req.ContentLength)
    timer.Stop()
    if err != nil {
        return PutObjectResponse{}, fmt.Errorf("storage write: %w", err)
    }

    // 8. Build metadata
    meta := metadata.ObjectMeta{
        Bucket:      req.Bucket,
        Key:         req.Key,
        VersionID:   versionID,
        StorageID:   storageID,
        Size:        hashReader.BytesRead(),
        ETag:        hashReader.MD5Hex(),
        ContentType: req.ContentType,
        UserMeta:    req.UserMeta,
        IsLatest:    true,
        CreatedAt:   time.Now().UTC(),
    }

    // 9. Apply lifecycle rules (compute expiration)
    if bucket.LifecycleRules != nil {
        meta.ExpiresAt = s.lifecycle.ComputeExpiration(meta, bucket.LifecycleRules)
    }

    // 10. Write metadata (returns old StorageID if overwriting)
    oldStorageID, err := s.meta.PutObjectMeta(ctx, meta)
    if err != nil {
        // Rollback: delete the data we just wrote
        s.storage.Delete(ctx, storageID)
        return PutObjectResponse{}, fmt.Errorf("metadata write: %w", err)
    }

    // 11. Async cleanup of old version data
    if oldStorageID != nil {
        go s.storage.Delete(context.Background(), *oldStorageID)
    }

    // 12. Async replication to other nodes (v2+)
    if len(nodes) > 1 {
        go s.replicator.ReplicateObject(context.Background(), meta, nodes[1:])
    }

    // 13. Metrics
    s.metrics.ObjectsPut.Inc()
    s.metrics.BytesWritten.Add(float64(meta.Size))

    return PutObjectResponse{
        ETag:      meta.ETag,
        VersionID: meta.VersionID,
    }, nil
}
```

### Streaming Architecture — Zero-Copy Where Possible

```
PutObject Request Flow (zero unnecessary copies):

  HTTP Body (io.Reader)
       │
       ▼
  ┌────────────────┐
  │ HashingReader   │ ← Wraps original reader
  │ (MD5 + SHA256)  │    Computes hashes on-the-fly
  └───────┬────────┘    No buffering in memory
          │
          ▼
  ┌────────────────┐
  │ StorageBackend  │
  │                │
  │ Flat: io.Copy  │ ← Direct to file descriptor
  │   to file      │    Uses sendfile(2) when possible
  │                │
  │ Packed: io.Copy│ ← Direct append to volume file
  │   to volume    │    Single sequential write
  └────────────────┘

GetObject Response Flow:

  ┌────────────────┐
  │ StorageBackend  │
  │                │
  │ Flat: os.Open  │ ← File descriptor
  │                │
  │ Packed: pread  │ ← Positional read, no seek contention
  └───────┬────────┘
          │
          ▼
  ┌────────────────┐
  │ http.ServeContent │ ← Handles Range requests automatically
  │ or io.Copy     │    Uses sendfile(2) kernel → socket
  └───────┬────────┘    Zero user-space copies
          │
          ▼
  HTTP Response Body
```

---

## 5. Storage Backends

### 5.1 Backend Interface

```go
package storage

type Backend interface {
    // Data operations
    Put(ctx context.Context, reader io.Reader, size int64) (ObjectID, error)
    Get(ctx context.Context, id ObjectID) (io.ReadCloser, error)
    GetRange(ctx context.Context, id ObjectID, offset, length int64) (io.ReadCloser, error)
    Delete(ctx context.Context, id ObjectID) error
    Stat(ctx context.Context, id ObjectID) (ObjectStat, error)

    // Maintenance
    SpaceInfo(ctx context.Context) (SpaceInfo, error)
    Compact(ctx context.Context) error          // packed: volume compaction
    Verify(ctx context.Context, id ObjectID) error  // integrity check

    // Lifecycle
    Init(ctx context.Context) error
    Close() error

    // Backend info
    Name() string                               // "flatfile" | "packed"
    Capabilities() BackendCapabilities
}

type BackendCapabilities struct {
    SupportsRangeRead    bool
    SupportsAtomicWrite  bool
    SupportsCompaction   bool
    MaxObjectSize        int64
    RecommendedObjSize   Range  // optimal object size range
}
```

### 5.2 Flat File Backend

```
Disk Layout:
/var/lib/openendpoint/data/
├── flatfile/
│   ├── ab/                          # 2-char hex prefix (256 dirs)
│   │   ├── ab3f7e...uuid.dat       # object data
│   │   ├── ab3f7e...uuid.dat.meta  # optional: inline metadata cache
│   │   └── ab8c2a...uuid.dat
│   ├── cd/
│   │   └── ...
│   └── tmp/                         # atomic write staging
│       └── .write-{uuid}.tmp
```

**Write Strategy:**
```
1. Create temp file in tmp/ directory
2. io.Copy from reader → temp file
3. fsync temp file
4. Rename temp → final path (atomic on POSIX)
5. fsync parent directory (ensures directory entry is durable)
```

**Optimization: O_DIRECT for Large Objects**
```go
func (f *FlatFile) Put(ctx context.Context, r io.Reader, size int64) (ObjectID, error) {
    id := ObjectID(uuid.Must(uuid.NewV7()).String())
    finalPath := f.objectPath(id)
    tmpPath := f.tempPath(id)

    flags := os.O_CREATE | os.O_WRONLY | os.O_EXCL
    if size > f.directIOThreshold { // default: 4MB
        flags |= syscall.O_DIRECT
    }

    file, err := os.OpenFile(tmpPath, flags, 0o640)
    if err != nil {
        return "", fmt.Errorf("create temp: %w", err)
    }

    var writer io.Writer = file
    if flags&syscall.O_DIRECT != 0 {
        // O_DIRECT requires aligned writes
        writer = NewAlignedWriter(file, 4096) // 4KB alignment
    }

    hash := md5.New()
    written, err := io.Copy(io.MultiWriter(writer, hash), r)
    // ... fsync, rename, return
}
```

**Recommended for:** Objects > 1MB, simple deployments, debugging ease.

### 5.3 Packed Volume Backend (Haystack-inspired)

```
Disk Layout:
/var/lib/openendpoint/data/
├── packed/
│   ├── volumes/
│   │   ├── vol-000001.dat          # 1GB volume file
│   │   ├── vol-000001.idx          # persisted index snapshot
│   │   ├── vol-000002.dat          # sealed volume
│   │   ├── vol-000002.idx
│   │   └── vol-000003.dat          # active (writable) volume
│   ├── wal/
│   │   ├── 000001.wal             # write-ahead log segments
│   │   └── 000002.wal
│   └── compaction/
│       └── .compact-{uuid}.tmp     # compaction staging
```

**Volume File Binary Format:**

```
Volume File (.dat):
┌─────────────────────────────────────────────────────────────┐
│ Volume Header (128 bytes)                                    │
│ ┌──────────┬─────────┬──────────┬──────────┬───────────────┐│
│ │  Magic   │ Version │ VolumeID │ Created  │  Flags        ││
│ │  (8B)    │  (4B)   │  (4B)    │  (8B)    │  (4B)         ││
│ │ OPENEPV1 │  0x01   │  uint32  │ unix_ns  │ sealed/active ││
│ └──────────┴─────────┴──────────┴──────────┴───────────────┘│
│ │ MaxSize  │ NeedleCount │ DataSize  │ Reserved (76B)       ││
│ │  (8B)    │   (8B)      │  (8B)     │                      ││
│ └──────────┴─────────────┴───────────┴──────────────────────┘│
├─────────────────────────────────────────────────────────────┤
│ Needle 0                                                     │
│ ┌────────┬───────┬──────┬───────┬─────────┬────────────────┐│
│ │ Magic  │ Flags │ ID   │ Size  │  CRC32  │  Padding       ││
│ │ (4B)   │ (1B)  │(16B) │ (8B)  │  (4B)   │  (0-7B)        ││
│ │0xNEEDLE│       │ UUID │uint64 │ of data │  to 8B align   ││
│ └────────┴───────┴──────┴───────┴─────────┴────────────────┘│
│ │                    Data Payload                           ││
│ │               (variable length, 8-byte aligned)           ││
│ └───────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────┤
│ Needle 1 ...                                                 │
├─────────────────────────────────────────────────────────────┤
│ Needle N ...                                                 │
└─────────────────────────────────────────────────────────────┘

Needle Flags:
  0x00 = Active
  0x01 = Deleted (tombstone)
  0x02 = Compressed (zstd)
  0x04 = Encrypted (AES-256-GCM, v4)

Total Needle overhead: 33 bytes + padding (max 40 bytes)
```

**Index File Format:**

```
Index (.idx) — Memory-mapped for fast lookups:
┌──────────────────────────────────────────────┐
│ Index Header (64 bytes)                       │
│ │ Magic │ Version │ VolumeID │ EntryCount │   │
│ └───────┴─────────┴──────────┴────────────┘   │
├──────────────────────────────────────────────┤
│ Entry 0:  [ObjectID(16B)] [Offset(8B)] [Size(8B)] [Flags(1B)] │
│ Entry 1:  [ObjectID(16B)] [Offset(8B)] [Size(8B)] [Flags(1B)] │
│ ...                                          │
│ Entry N:  ...                                │
└──────────────────────────────────────────────┘

In-memory: loaded into map[ObjectID]NeedleLocation on startup
Persistence: snapshot every 60s + WAL for crash recovery
```

**Compaction Process:**

```
Phase 1: Analysis
  - Scan volume, count active vs deleted needles
  - If deleted_ratio < threshold (30%), skip

Phase 2: Copy
  - Create new volume file
  - Copy all active needles (sequential read → sequential write)
  - Build new index

Phase 3: Swap (atomic)
  - Rename new volume → old volume path
  - Swap in-memory index atomically
  - Delete old volume file

Phase 4: Verify
  - Read-verify random sample of needles in new volume
  - Compare CRC32 checksums

Compaction is:
  - Non-blocking (reads continue from old volume until swap)
  - Rate-limited (configurable IO bandwidth cap)
  - Resumable (checkpoint progress to WAL)
```

**Recommended for:** Small objects (< 1MB), high IOPS workloads, massive object counts.

### 5.4 Future: Tiered Backend (v5)

```go
// Tiered backend routes objects to different backends based on rules
type TieredBackend struct {
    hot    Backend  // Packed volumes on NVMe SSD
    warm   Backend  // Flat files on HDD
    cold   Backend  // Compressed flat files on object storage (S3/GCS)
    rules  []TieringRule
}

type TieringRule struct {
    Condition  TieringCondition  // age, access pattern, size
    TargetTier string            // "hot", "warm", "cold"
}
```

---

## 6. Metadata Layer

### 6.1 Key Schema Design

All metadata is stored in a key-value store with carefully designed key schemas for efficient queries.

```
Key Prefixes and Schemas:

BUCKET METADATA:
  b:{bucket_name}                         → BucketMeta (JSON)

OBJECT METADATA (Latest Pointer):
  o:{bucket}:{key}                        → LatestPointer {versionID, isDeleteMarker}

OBJECT VERSIONS:
  v:{bucket}:{key}:{versionID}            → ObjectMeta (JSON)
  │                    │
  │                    └── UUID v7 (time-sortable, lexicographic order)
  │                        Newest version = last in range scan
  └── Enables efficient prefix scan for "list all versions of key"

DELETE MARKERS:
  d:{bucket}:{key}:{versionID}            → DeleteMarkerMeta

OBJECT LISTING INDEX (for ListObjectsV2):
  l:{bucket}:{key}                        → ObjectListEntry {size, etag, lastModified}
  │
  └── Separate index for listing = fast prefix scan
      without loading full ObjectMeta

MULTIPART UPLOADS:
  m:{uploadID}                            → MultipartMeta {bucket, key, created}
  p:{uploadID}:{partNumber:05d}           → PartMeta {storageID, size, etag}

LIFECYCLE INDEX:
  e:{expiresAt:unix}:{bucket}:{key}       → ExpirationEntry
  │
  └── Time-prefixed = range scan for "expired before now" is trivial

STORAGE BACKEND MAPPING (for backend migration):
  s:{storageID}                           → StorageLocation {backend, volumeID, offset}

CLUSTER METADATA (v2):
  c:nodes:{nodeID}                        → NodeInfo
  c:ring                                  → HashRingSnapshot
  c:rebalance:{taskID}                    → RebalanceTask

REPLICATION LOG (v2):
  r:{timestamp}:{bucket}:{key}            → ReplicationEntry
```

### 6.2 Pebble Implementation Notes

```go
package pebblestore

import (
    "github.com/cockroachdb/pebble"
)

type PebbleStore struct {
    db     *pebble.DB
    cache  *pebble.Cache
}

func NewPebbleStore(dir string, cacheSize int64) (*PebbleStore, error) {
    cache := pebble.NewCache(cacheSize) // default: 256MB
    defer cache.Unref()

    opts := &pebble.Options{
        Cache:                       cache,
        MaxConcurrentCompactions:    func() int { return 4 },
        L0CompactionThreshold:       4,
        L0StopWritesThreshold:       12,
        LBaseMaxBytes:               64 << 20, // 64MB
        MaxOpenFiles:                1000,
        MemTableSize:                64 << 20, // 64MB
        MemTableStopWritesThreshold: 4,

        // Bloom filters for point lookups
        Levels: []pebble.LevelOptions{
            {TargetFileSize: 16 << 20, FilterPolicy: bloom.FilterPolicy(10)},
            {TargetFileSize: 32 << 20, FilterPolicy: bloom.FilterPolicy(10)},
            {TargetFileSize: 64 << 20},
            {TargetFileSize: 128 << 20},
            {TargetFileSize: 256 << 20},
            {TargetFileSize: 512 << 20},
            {TargetFileSize: 512 << 20},
        },
    }

    db, err := pebble.Open(dir, opts)
    if err != nil {
        return nil, err
    }

    return &PebbleStore{db: db, cache: cache}, nil
}

// ListObjects uses prefix iteration — very efficient in LSM trees
func (p *PebbleStore) ListObjects(ctx context.Context, bucket string, opts ListOptions) (ListResult, error) {
    prefix := []byte(fmt.Sprintf("l:%s:", bucket))
    if opts.Prefix != "" {
        prefix = append(prefix, []byte(opts.Prefix)...)
    }

    iter, _ := p.db.NewIter(&pebble.IterOptions{
        LowerBound: prefix,
        UpperBound: incrementBytes(prefix), // prefix + 1 for range end
    })
    defer iter.Close()

    var result ListResult
    count := 0

    // Seek to StartAfter position if paginating
    if opts.StartAfter != "" {
        startKey := []byte(fmt.Sprintf("l:%s:%s", bucket, opts.StartAfter))
        iter.SeekGE(startKey)
        if iter.Valid() {
            iter.Next() // skip the StartAfter key itself
        }
    } else {
        iter.First()
    }

    for ; iter.Valid() && count < opts.MaxKeys; iter.Next() {
        key := string(iter.Key())
        objectKey := extractObjectKey(key, bucket)

        // Handle delimiter (directory simulation)
        if opts.Delimiter != "" {
            afterPrefix := strings.TrimPrefix(objectKey, opts.Prefix)
            if idx := strings.Index(afterPrefix, opts.Delimiter); idx >= 0 {
                commonPrefix := opts.Prefix + afterPrefix[:idx+len(opts.Delimiter)]
                result.CommonPrefixes = appendUnique(result.CommonPrefixes, commonPrefix)
                // Skip all keys with this common prefix
                skipTo := []byte(fmt.Sprintf("l:%s:%s", bucket, commonPrefix))
                iter.SeekGE(incrementBytes(skipTo))
                continue
            }
        }

        var entry ObjectListEntry
        json.Unmarshal(iter.Value(), &entry)
        result.Objects = append(result.Objects, entry.ToObjectMeta(bucket, objectKey))
        count++
    }

    if iter.Valid() {
        result.IsTruncated = true
        result.NextMarker = result.Objects[len(result.Objects)-1].Key
    }

    return result, nil
}
```

### 6.3 Metadata Consistency Guarantees

| Operation | Guarantee | Implementation |
|-----------|-----------|----------------|
| PutObjectMeta | Atomic per-key | Pebble batch write (all-or-nothing) |
| GetObjectMeta | Read-your-writes | Pebble point lookup (always sees latest) |
| ListObjects | Snapshot consistency | Pebble snapshot iterator |
| DeleteObjectMeta | Atomic | Pebble batch delete + insert (delete marker) |
| BucketVersioning toggle | Atomic | Single key update |

---

## 7. S3 API Implementation

### 7.1 API Coverage Matrix

```
v1 API Coverage:
═══════════════════════════════════════════════════════════════
 Bucket Operations                          Status
───────────────────────────────────────────────────────────────
 CreateBucket          PUT /{bucket}         ✅ v1
 DeleteBucket          DELETE /{bucket}      ✅ v1
 HeadBucket            HEAD /{bucket}        ✅ v1
 ListBuckets           GET /                 ✅ v1
 GetBucketLocation     GET /?location        ✅ v1
 GetBucketVersioning   GET /?versioning      ✅ v1
 PutBucketVersioning   PUT /?versioning      ✅ v1
 PutBucketLifecycle    PUT /?lifecycle       ✅ v1
 GetBucketLifecycle    GET /?lifecycle       ✅ v1
 DeleteBucketLifecycle DELETE /?lifecycle    ✅ v1

 Object Operations
───────────────────────────────────────────────────────────────
 PutObject             PUT /{bucket}/{key}   ✅ v1
 GetObject             GET /{bucket}/{key}   ✅ v1
 HeadObject            HEAD /{bucket}/{key}  ✅ v1
 DeleteObject          DELETE /{bucket}/{key} ✅ v1
 DeleteObjects         POST /?delete         ✅ v1 (batch)
 CopyObject            PUT + x-amz-copy-src  ✅ v1
 ListObjectsV2         GET /?list-type=2     ✅ v1
 ListObjectVersions    GET /?versions        ✅ v1

 Multipart Upload
───────────────────────────────────────────────────────────────
 InitiateMultipart     POST /?uploads        ✅ v1
 UploadPart            PUT ?partNumber&upId   ✅ v1
 CompleteMultipart     POST ?uploadId         ✅ v1
 AbortMultipart        DELETE ?uploadId       ✅ v1
 ListParts             GET ?uploadId          ✅ v1
 ListMultipartUploads  GET /?uploads          ✅ v1

 Presigned URLs
───────────────────────────────────────────────────────────────
 Presigned GET         query string auth     ✅ v1
 Presigned PUT         query string auth     ✅ v1

v2+ API Coverage:
═══════════════════════════════════════════════════════════════
 PutBucketPolicy       PUT /?policy          🔮 v4
 GetBucketPolicy       GET /?policy          🔮 v4
 PutBucketEncryption   PUT /?encryption      🔮 v4
 PutObjectLockConfig   PUT /?object-lock     🔮 v4
 PutBucketNotification PUT /?notification    🔮 v4
 SelectObjectContent   POST /?select         🔮 v5
 PutBucketReplication  PUT /?replication     🔮 v3
```

### 7.2 Request Routing Logic

```go
// S3 has complex routing — same path, different operations based on query params
func (h *Handler) routeRequest(w http.ResponseWriter, r *http.Request) {
    bucket, key := parsePath(r)
    query := r.URL.Query()

    switch {
    // Bucket-level operations (no key)
    case key == "":
        switch r.Method {
        case "GET":
            if bucket == "" {
                h.ListBuckets(w, r)                    // GET /
            } else if query.Has("uploads") {
                h.ListMultipartUploads(w, r)           // GET /bucket?uploads
            } else if query.Has("versioning") {
                h.GetBucketVersioning(w, r)            // GET /bucket?versioning
            } else if query.Has("lifecycle") {
                h.GetBucketLifecycle(w, r)             // GET /bucket?lifecycle
            } else if query.Has("versions") {
                h.ListObjectVersions(w, r)             // GET /bucket?versions
            } else if query.Has("location") {
                h.GetBucketLocation(w, r)              // GET /bucket?location
            } else {
                h.ListObjectsV2(w, r)                  // GET /bucket (+ list-type=2)
            }
        case "PUT":
            if query.Has("versioning") {
                h.PutBucketVersioning(w, r)
            } else if query.Has("lifecycle") {
                h.PutBucketLifecycle(w, r)
            } else {
                h.CreateBucket(w, r)
            }
        case "DELETE":
            h.DeleteBucket(w, r)
        case "HEAD":
            h.HeadBucket(w, r)
        }

    // Object-level operations (bucket + key)
    default:
        switch r.Method {
        case "GET":
            if query.Has("uploadId") {
                h.ListParts(w, r)
            } else {
                h.GetObject(w, r)
            }
        case "PUT":
            if query.Has("partNumber") && query.Has("uploadId") {
                h.UploadPart(w, r)
            } else if r.Header.Get("x-amz-copy-source") != "" {
                h.CopyObject(w, r)
            } else {
                h.PutObject(w, r)
            }
        case "DELETE":
            if query.Has("uploadId") {
                h.AbortMultipartUpload(w, r)
            } else {
                h.DeleteObject(w, r)
            }
        case "POST":
            if query.Has("uploads") {
                h.InitiateMultipartUpload(w, r)
            } else if query.Has("uploadId") {
                h.CompleteMultipartUpload(w, r)
            } else if query.Has("delete") {
                h.DeleteObjects(w, r)
            }
        case "HEAD":
            h.HeadObject(w, r)
        }
    }
}
```

### 7.3 Virtual-Hosted Style Support

```go
// S3 supports two URL styles:
//   Path-style:    https://s3.example.com/bucket/key
//   Virtual-host:  https://bucket.s3.example.com/key

func (h *Handler) extractBucketKey(r *http.Request) (string, string) {
    host := r.Host

    // Check for virtual-hosted style
    if h.config.VirtualHostDomain != "" {
        suffix := "." + h.config.VirtualHostDomain
        if strings.HasSuffix(host, suffix) {
            bucket := strings.TrimSuffix(host, suffix)
            key := strings.TrimPrefix(r.URL.Path, "/")
            return bucket, key
        }
    }

    // Fall back to path-style
    parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
    bucket := parts[0]
    key := ""
    if len(parts) > 1 {
        key = parts[1]
    }
    return bucket, key
}
```

---

## 8. Authentication & Authorization

### 8.1 AWS Signature V4 (v1)

```
Authorization: AWS4-HMAC-SHA256
  Credential=AKID/20240101/us-east-1/s3/aws4_request,
  SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date,
  Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024

Verification Steps:
  1. Parse Authorization header
  2. Extract Access Key → look up Secret Key
  3. Build Canonical Request:
     - HTTP method
     - URI (path-encoded)
     - Query string (sorted)
     - Canonical headers (lowercase, sorted)
     - Signed headers list
     - Payload hash (x-amz-content-sha256 header)
  4. Build String to Sign:
     - Algorithm: AWS4-HMAC-SHA256
     - Timestamp: x-amz-date header
     - Credential scope: date/region/s3/aws4_request
     - SHA256(Canonical Request)
  5. Derive signing key:
     - HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), "s3"), "aws4_request")
  6. Calculate HMAC-SHA256(signing_key, string_to_sign)
  7. Compare with provided signature (constant-time)
  8. Check timestamp (±15 minutes)

Special Cases:
  - Chunked uploads: x-amz-content-sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
  - Unsigned payload: x-amz-content-sha256 = "UNSIGNED-PAYLOAD"
  - Presigned URLs: signature in query params, not header
```

### 8.2 Presigned URL Implementation

```go
// Presigned URLs allow temporary access without credentials
func (a *AuthService) GeneratePresignedURL(bucket, key string, expiry time.Duration, method string) (string, error) {
    now := time.Now().UTC()
    credential := fmt.Sprintf("%s/%s/%s/s3/aws4_request",
        a.accessKey,
        now.Format("20060102"),
        a.region,
    )

    params := url.Values{
        "X-Amz-Algorithm":     {"AWS4-HMAC-SHA256"},
        "X-Amz-Credential":    {credential},
        "X-Amz-Date":          {now.Format("20060102T150405Z")},
        "X-Amz-Expires":       {strconv.Itoa(int(expiry.Seconds()))},
        "X-Amz-SignedHeaders":  {"host"},
    }

    // Build canonical request with query params
    canonicalRequest := buildCanonicalRequestForPresign(method, bucket, key, params)
    stringToSign := buildStringToSign(now, canonicalRequest)
    signature := sign(a.signingKey, stringToSign)

    params.Set("X-Amz-Signature", signature)

    return fmt.Sprintf("%s/%s/%s?%s",
        a.endpoint, bucket, key, params.Encode()), nil
}
```

### 8.3 IAM System (v4)

```
Role-Based Access Control:

┌─────────────────────────────────────────────┐
│                   Tenant                     │
│  ┌─────────┐  ┌─────────┐  ┌─────────────┐ │
│  │  Users   │  │ Groups  │  │  Policies   │ │
│  │          │  │         │  │             │ │
│  │ alice ───┼──► admins ─┼──► FullAccess  │ │
│  │ bob   ───┼──► devs  ──┼──► ReadOnly    │ │
│  │ carol ───┼──► ops   ──┼──► WriteMedia  │ │
│  └─────────┘  └─────────┘  └─────────────┘ │
└─────────────────────────────────────────────┘

Policy Format (S3-compatible):
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:ListBucket"],
    "Resource": [
      "arn:openep:s3:::my-bucket",
      "arn:openep:s3:::my-bucket/*"
    ],
    "Condition": {
      "IpAddress": {"aws:SourceIp": "192.168.1.0/24"},
      "StringLike": {"s3:prefix": "public/*"}
    }
  }]
}
```

---

## 9. Multi-Node Clustering (v2)

### 9.1 Node Discovery — Gossip Protocol

```go
import "github.com/hashicorp/memberlist"

type ClusterManager struct {
    list       *memberlist.Memberlist
    events     chan MemberEvent
    hashRing   *ConsistentHashRing
    localNode  NodeInfo
}

func NewClusterManager(config ClusterConfig) (*ClusterManager, error) {
    mlConfig := memberlist.DefaultLANConfig()
    mlConfig.Name = config.NodeName
    mlConfig.BindPort = config.GossipPort        // default: 9002
    mlConfig.AdvertiseAddr = config.AdvertiseAddr

    cm := &ClusterManager{
        events:   make(chan MemberEvent, 100),
        hashRing: NewConsistentHashRing(config.VirtualNodes), // default: 256 vnodes
    }

    mlConfig.Events = &memberlistEventDelegate{ch: cm.events}
    mlConfig.Delegate = &stateDelegate{node: cm.localNode}

    list, err := memberlist.Create(mlConfig)
    if err != nil {
        return nil, err
    }

    // Join existing cluster
    if len(config.JoinAddrs) > 0 {
        _, err = list.Join(config.JoinAddrs)
    }

    cm.list = list
    go cm.handleEvents() // process join/leave events
    return cm, nil
}

func (cm *ClusterManager) handleEvents() {
    for event := range cm.events {
        switch event.Type {
        case NodeJoin:
            cm.hashRing.AddNode(event.Node)
            cm.triggerRebalance(event.Node)
        case NodeLeave:
            cm.hashRing.RemoveNode(event.Node)
            cm.triggerReRepair(event.Node)
        case NodeUpdate:
            cm.hashRing.UpdateNode(event.Node)
        }
    }
}
```

### 9.2 Consistent Hashing Ring

```
Hash Ring with Virtual Nodes:

             Node A (vnodes: a1, a2, a3, ...)
               │
    ┌──────────┼───────────────────────────────────────┐
    │          │                                       │
    │    ┌─────▼─────┐                                 │
    │    │    a1      │                                 │
    │    └───────────┘                                 │
    │          ┌─────────┐                             │
    │          │   c2     │  Node C                    │
    │          └─────────┘                             │
    │                    ┌─────────┐                   │
    │                    │   b1     │  Node B          │
    │                    └─────────┘                   │
    │   ┌─────────┐                                   │
    │   │   a2     │  Node A                          │
    │   └─────────┘                                   │
    │              ┌─────────┐                        │
    │              │   b2     │  Node B               │
    │              └─────────┘                        │
    │                       ┌─────────┐              │
    │                       │   c1     │  Node C     │
    │                       └─────────┘              │
    │   ┌─────────┐                                   │
    │   │   a3     │  Node A                          │
    │   └─────────┘                                   │
    └─────────────────────────────────────────────────┘

Object Placement (RF=3):
  hash("my-bucket/photo.jpg") = 0x7A3F...
  → Walk ring clockwise → find 3 distinct physical nodes
  → Primary: Node B (b1)
  → Replica1: Node C (c1)
  → Replica2: Node A (a3)
```

```go
type ConsistentHashRing struct {
    mu           sync.RWMutex
    ring         []ringEntry        // sorted by hash
    vnodeCount   int                // virtual nodes per physical node
    nodes        map[NodeID]NodeInfo
    replicaCount int                // replication factor
}

type ringEntry struct {
    hash   uint64
    nodeID NodeID
}

func (r *ConsistentHashRing) GetNodes(key string, count int) []NodeID {
    r.mu.RLock()
    defer r.mu.RUnlock()

    hash := xxhash.Sum64String(key)
    idx := sort.Search(len(r.ring), func(i int) bool {
        return r.ring[i].hash >= hash
    })

    seen := make(map[NodeID]bool)
    var result []NodeID

    for len(result) < count && len(result) < len(r.nodes) {
        entry := r.ring[idx%len(r.ring)]
        if !seen[entry.nodeID] {
            seen[entry.nodeID] = true
            result = append(result, entry.nodeID)
        }
        idx++
    }

    return result
}
```

### 9.3 Write Path (Clustered)

```
Client PUT /bucket/key
         │
         ▼
    ┌──────────┐
    │  Any Node │ ← receives request (coordinator)
    │ (Gateway) │
    └─────┬────┘
          │
          ▼
    Hash("bucket/key") → Ring Lookup → [NodeA, NodeB, NodeC]
          │
          ├──────────────────────────────────────────┐
          │                                          │
    ┌─────▼──────┐  ┌──────────────┐  ┌─────────────▼────┐
    │  Primary    │  │  Replica 1    │  │   Replica 2      │
    │  (NodeA)    │  │  (NodeB)      │  │   (NodeC)        │
    │             │  │               │  │                  │
    │ Write data  │  │ Write data    │  │  Write data      │
    │ Write meta  │  │ Write meta    │  │  Write meta      │
    │ ──ACK──►    │  │ ──ACK──►      │  │  ──ACK──►        │
    └─────────────┘  └──────────────┘  └──────────────────┘
          │                │                    │
          └────────────────┼────────────────────┘
                           │
                    Consistency Level:
                    ┌──────┴──────┐
                    │   QUORUM    │ ← wait for 2 of 3 ACKs
                    │  (default)  │
                    └─────────────┘
                           │
                           ▼
                    200 OK to Client
```

### 9.4 Read Repair

```
When a read detects inconsistency:

Client GET /bucket/key
         │
         ▼
    ┌──────────┐
    │Coordinator│
    │   Node    │
    └─────┬────┘
          │
    ┌─────┼────────────────────┐
    │     │                    │
    ▼     ▼                    ▼
  NodeA  NodeB               NodeC
  v=3    v=3                 v=2 ← stale!
    │     │                    │
    └─────┼────────────────────┘
          │
    Coordinator detects version mismatch
          │
          ▼
    Background: push v=3 to NodeC (read repair)
    Foreground: return v=3 to client immediately
```

### 9.5 Anti-Entropy (Merkle Trees)

```
Periodic consistency check between replicas:

Node A                              Node B
  │                                    │
  ├── Build Merkle tree for bucket ────┤
  │                                    │
  │   Root: abc123                     │   Root: abc123 ✓ (match)
  │   ├── Left: def456                 │   ├── Left: def456 ✓
  │   └── Right: 789abc               │   └── Right: DIFFER ✗
  │       ├── RL: aaa111              │       ├── RL: aaa111 ✓
  │       └── RR: bbb222              │       └── RR: ccc333 ✗
  │                                    │
  │   Only sync the objects in RR subtree
  │   (logarithmic comparison instead of full scan)
  │                                    │
  ├── Transfer missing/updated objects─►│
  │                                    │
```

### 9.6 Rebalancing on Node Join

```
Before: 3 nodes, RF=2
  Data distribution: ~33% per node

New Node D joins:
  ┌─────────────────────────────────────────────┐
  │  1. Update hash ring (add D's vnodes)       │
  │  2. Calculate ownership changes              │
  │     - Some ranges move from A,B,C → D       │
  │  3. Start background transfer                │
  │     - Rate-limited (configurable bandwidth)  │
  │     - Prioritize: newest data first          │
  │  4. During transfer:                         │
  │     - Reads: serve from old owner (redirect) │
  │     - Writes: go to new owner immediately    │
  │  5. Transfer complete:                       │
  │     - Old copies marked for deletion         │
  │     - Compaction reclaims space              │
  └─────────────────────────────────────────────┘

After: 4 nodes, RF=2
  Data distribution: ~25% per node
  Zero downtime during entire process
```

---

## 10. Multi-Region Federation (v3)

### 10.1 Federation Architecture

```
Each region runs an independent cluster.
Federation = metadata synchronization + async data replication.

┌─────────────────────────────────────────────────────────────┐
│                    Federation Control Plane                   │
│                                                              │
│  ┌──────────────────┐  ┌──────────────────────────────────┐ │
│  │ Region Registry   │  │ Replication Policy Engine        │ │
│  │                   │  │                                  │ │
│  │ EU-West: active   │  │ Rule: bucket "media-*"           │ │
│  │ US-East: active   │  │   → replicate to ALL regions     │ │
│  │ AP-South: active  │  │   → mode: ASYNC                 │ │
│  │ EU-North: standby │  │   → max_lag: 1h                 │ │
│  └──────────────────┘  │                                  │ │
│                         │ Rule: bucket "logs-*"            │ │
│  ┌──────────────────┐  │   → replicate to US-East only    │ │
│  │ Conflict Resolver │  │   → mode: SEMI                  │ │
│  │                   │  └──────────────────────────────────┘ │
│  │ Strategy:         │                                       │
│  │  Last-Write-Wins  │  ┌──────────────────────────────────┐ │
│  │  (vector clock)   │  │ Bandwidth Manager                │ │
│  │                   │  │                                  │ │
│  │ Custom resolvers  │  │ EU↔US: 500 Mbps limit           │ │
│  │ per bucket        │  │ EU↔AP: 200 Mbps limit           │ │
│  └──────────────────┘  │ US↔AP: 300 Mbps limit           │ │
│                         └──────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 10.2 Replication Protocol

```
Cross-Region Replication Flow:

Source Region (EU-West)           Target Region (US-East)
  │                                        │
  │  PutObject("photo.jpg")                │
  │  → Write locally (sync)                │
  │  → Append to replication log           │
  │                                        │
  │  Replication Agent picks up entry      │
  │  ├── Read object data                  │
  │  ├── Compress with zstd               │
  │  ├── Encrypt for transit (TLS)         │
  │  └── Stream to target region ─────────►│
  │                                        │ Receive + decompress
  │                                        │ Write to local storage
  │                                        │ Write metadata
  │                          ◄── ACK ──────│
  │  Mark replication entry as complete    │
  │                                        │
  │  Replication lag metric updated        │
  │                                        │

Replication Log Format:
  ┌──────┬────────┬─────────┬───────┬──────────┬─────────┐
  │SeqNum│Timestamp│ Bucket  │  Key  │Operation │ Status  │
  │      │        │         │       │PUT/DELETE│Pending/ │
  │      │        │         │       │         │Complete │
  └──────┴────────┴─────────┴───────┴──────────┴─────────┘
```

### 10.3 Conflict Resolution

```
Scenario: Same object written in two regions simultaneously

Region EU writes "photo.jpg" v=A at T=100
Region US writes "photo.jpg" v=B at T=101

Conflict detection via vector clocks:
  EU version: {EU:1, US:0} → "photo.jpg" = data_A
  US version: {EU:0, US:1} → "photo.jpg" = data_B

Neither dominates → CONFLICT

Resolution strategies (configurable per bucket):
  1. Last-Write-Wins (LWW) — timestamp-based, simplest
     → US wins (T=101 > T=100)

  2. Source-Priority — designated primary region wins
     → EU wins (EU is primary for "photos" bucket)

  3. Merge — application-specific (for structured data)
     → Both versions kept, client resolves

  4. Custom webhook — call external resolver
     → POST /resolve with both versions, get winner

Conflict metadata:
  {
    "key": "photo.jpg",
    "conflict_id": "uuid",
    "versions": [
      {"region": "EU", "version_id": "...", "timestamp": 100, "size": 1024},
      {"region": "US", "version_id": "...", "timestamp": 101, "size": 2048}
    ],
    "resolved_by": "LWW",
    "winner": "US"
  }
```

### 10.4 Geo-Aware Routing

```go
// GeoDNS or application-level routing
type GeoRouter struct {
    regions  map[string]RegionEndpoint
    fallback string
}

// Route read requests to nearest region
func (g *GeoRouter) RouteRead(clientIP net.IP, bucket, key string) string {
    // 1. Check if object has region affinity
    affinity := g.getRegionAffinity(bucket, key)
    if affinity != "" {
        return g.regions[affinity].Endpoint
    }

    // 2. Find nearest region by GeoIP
    clientRegion := g.geoIP.Lookup(clientIP)
    nearest := g.findNearest(clientRegion)

    // 3. Check if nearest region has the data
    if g.regionHasObject(nearest, bucket, key) {
        return g.regions[nearest].Endpoint
    }

    // 4. Fallback to primary region
    return g.regions[g.getPrimaryRegion(bucket)].Endpoint
}

// Route write requests based on bucket policy
func (g *GeoRouter) RouteWrite(bucket string) string {
    // Writes always go to the primary region for the bucket
    // Replication handles cross-region distribution
    return g.regions[g.getPrimaryRegion(bucket)].Endpoint
}
```

---

## 11. Replication, Backup & Mirror

### 11.1 Backup Targets

```
OpenEndpoint can back up to multiple target types:

┌─────────────────────────────────────────────────────┐
│  OpenEndpoint Cluster                                │
│                                                     │
│  Backup Agent                                       │
│  ├── Schedule: daily 02:00 UTC                      │
│  ├── Type: incremental (only changed objects)       │
│  ├── Compression: zstd (level 3)                    │
│  └── Encryption: AES-256-GCM                        │
│                                                     │
│  ┌──────────┐                                       │
│  │ Change   │ ← tracks all mutations since last     │
│  │ Journal  │   backup via sequence numbers          │
│  └────┬─────┘                                       │
│       │                                             │
└───────┼─────────────────────────────────────────────┘
        │
        ├──────────► AWS S3 (any region)
        │            └── s3://backup-bucket/openep/...
        │
        ├──────────► Google Cloud Storage
        │            └── gs://backup-bucket/openep/...
        │
        ├──────────► Azure Blob Storage
        │            └── az://container/openep/...
        │
        ├──────────► Another OpenEndpoint Cluster
        │            └── https://backup.openep.example.com
        │
        ├──────────► NFS / Local Filesystem
        │            └── /mnt/backup/openep/...
        │
        └──────────► SFTP / Rsync Target
                     └── sftp://backup.example.com/openep/...
```

### 11.2 Backup Format

```
Backup Structure:
backup-2024-01-15T020000Z/
├── manifest.json           # backup metadata
│   {
│     "backup_id": "uuid",
│     "timestamp": "2024-01-15T02:00:00Z",
│     "type": "incremental",
│     "base_backup": "uuid-of-last-full",
│     "sequence_range": [10000, 15000],
│     "object_count": 5000,
│     "total_size": "50GB",
│     "compressed_size": "35GB",
│     "checksum": "sha256:abc..."
│   }
├── metadata/
│   ├── buckets.jsonl       # bucket metadata (JSON lines)
│   └── objects.jsonl       # object metadata (JSON lines)
└── data/
    ├── chunk-0001.zst      # compressed data chunks (256MB each)
    ├── chunk-0002.zst
    └── chunk-0003.zst

Restore:
  openep backup restore --from s3://backup-bucket/openep/backup-2024-01-15T020000Z
  → Downloads manifest → validates checksums → restores metadata → restores data
  → Point-in-time recovery: restore to any backup snapshot
```

### 11.3 Mirror Mode (Continuous Replication)

```
Mirror = real-time, continuous replication to another system.
Different from backup: mirror is always up-to-date, backup is periodic.

Source Cluster                    Mirror Target
  │                                    │
  │  Every write operation:            │
  │  PUT, DELETE, versioning change    │
  │         │                          │
  │         ▼                          │
  │  ┌──────────────┐                 │
  │  │ Mirror Agent  │                 │
  │  │               │                 │
  │  │ • Tail the    │                 │
  │  │   change log  │                 │
  │  │ • Batch       │                 │
  │  │   changes     │                 │
  │  │ • Stream to   ├────────────────►│
  │  │   target      │  Replicate ops  │
  │  │ • Track lag   │                 │
  │  └──────────────┘                 │
  │                                    │
  │  Mirror lag target: < 60 seconds   │
  │                                    │

Configuration:
  mirror:
    enabled: true
    targets:
      - name: "disaster-recovery"
        endpoint: "https://dr.openep.example.com"
        access_key: "mirror-user"
        secret_key: "..."
        buckets: ["*"]                # mirror all buckets
        mode: "async"                 # async | sync
        max_lag: "60s"
        bandwidth_limit: "100MB/s"
        compress: true
      - name: "analytics-copy"
        endpoint: "s3://analytics-bucket"
        buckets: ["logs-*", "events-*"]
        mode: "async"
        max_lag: "5m"
```

---

## 12. CDN Integration & Edge

### 12.1 CDN Architecture

```
OpenEndpoint serves as CDN origin:

                     User Request
                          │
                          ▼
                   ┌─────────────┐
                   │  CDN Edge   │
                   │   (PoP)     │
                   └──────┬──────┘
                          │
              ┌───────────┴───────────┐
              │                       │
         Cache HIT                Cache MISS
              │                       │
              ▼                       ▼
         Return cached         ┌─────────────┐
         content               │ OpenEndpoint │
                               │   Origin     │
                               └──────┬──────┘
                                      │
                               Return content
                               + Cache-Control
                               headers

Supported CDN Providers (via standard HTTP):
  • Cloudflare
  • AWS CloudFront
  • Fastly
  • Bunny CDN
  • Akamai
  • Any HTTP-based CDN
```

### 12.2 CDN-Optimized Endpoints

```go
// Dedicated CDN origin endpoints with optimized headers
router.Route("/cdn/v1", func(r chi.Router) {
    r.Use(CDNOriginMiddleware) // adds Cache-Control, ETag, etc.
    r.Get("/{bucket}/{key:.*}", CDNGetObject)
})

func CDNOriginMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Origin shield: accept X-Forwarded-For from CDN
        // Set aggressive cache headers for CDN
        // Support conditional requests (If-None-Match, If-Modified-Since)
        next.ServeHTTP(w, r)
    })
}

func CDNGetObject(w http.ResponseWriter, r *http.Request) {
    // 1. Check conditional request
    if etag := r.Header.Get("If-None-Match"); etag != "" {
        if objectETag == etag {
            w.WriteHeader(http.StatusNotModified)
            return
        }
    }

    // 2. Set CDN-friendly headers
    w.Header().Set("Cache-Control", getCacheControl(bucket, key))
    w.Header().Set("ETag", `"`+objectMeta.ETag+`"`)
    w.Header().Set("Last-Modified", objectMeta.CreatedAt.Format(http.TimeFormat))
    w.Header().Set("Accept-Ranges", "bytes")

    // 3. Support range requests (for video streaming etc.)
    if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
        serveRangeRequest(w, r, objectMeta)
        return
    }

    // 4. Serve full object
    http.ServeContent(w, r, key, objectMeta.CreatedAt, objectReader)
}
```

### 12.3 Presigned URLs for CDN

```
Flow: Client requests access → Server generates presigned URL → Client uses CDN

  Client                  API Server              CDN
    │                         │                    │
    │ POST /api/files/xyz     │                    │
    │ "I want to view xyz"    │                    │
    │────────────────────────►│                    │
    │                         │                    │
    │  Generate presigned URL:│                    │
    │  cdn.example.com/       │                    │
    │    bucket/key?           │                    │
    │    X-Amz-Expires=3600   │                    │
    │    &X-Amz-Signature=... │                    │
    │                         │                    │
    │◄────────────────────────│                    │
    │ {url: "https://cdn..."}  │                    │
    │                         │                    │
    │ GET cdn.example.com/... │                    │
    │─────────────────────────┼───────────────────►│
    │                         │   Cache MISS        │
    │                         │◄───────────────────│
    │                         │   Verify signature  │
    │                         │   Return object     │
    │                         │────────────────────►│
    │                         │   CDN caches         │
    │◄────────────────────────┼────────────────────│
    │  Content delivered       │                    │

Next request for same URL:
    │ GET cdn.example.com/... │                    │
    │─────────────────────────┼───────────────────►│
    │                         │   Cache HIT!        │
    │◄────────────────────────┼────────────────────│
    │  Instant delivery        │    (no origin)     │
```

### 12.4 Cache Invalidation API

```go
// When an object is updated or deleted, invalidate CDN cache
type CDNManager struct {
    providers []CDNProvider
}

type CDNProvider interface {
    Invalidate(ctx context.Context, paths []string) error
    PurgeAll(ctx context.Context) error
}

// Cloudflare implementation
type CloudflareCDN struct {
    zoneID string
    apiKey string
}

func (c *CloudflareCDN) Invalidate(ctx context.Context, paths []string) error {
    // POST https://api.cloudflare.com/client/v4/zones/{zone}/purge_cache
    // {"files": ["https://cdn.example.com/bucket/key1", ...]}
}

// Hook into object lifecycle
func (s *ObjectService) afterPut(bucket, key string) {
    if s.cdn != nil {
        go s.cdn.Invalidate(context.Background(), []string{
            fmt.Sprintf("/%s/%s", bucket, key),
        })
    }
}
```

---

## 13. Web Dashboard (Next.js)

### 13.1 Repository: github.com/OpenEndpoint/dashboard

```
Technology Stack:
  • Next.js 15 (App Router)
  • React 19
  • TypeScript 5
  • Tailwind CSS v4
  • shadcn/ui components
  • TanStack Query (data fetching)
  • TanStack Table (data grids)
  • Recharts (analytics charts)
  • Monaco Editor (config editing)
  • next-intl (i18n)
```

### 13.2 Dashboard Pages & Features

```
Page Map:
═══════════════════════════════════════════════════════════════

📊 Dashboard (/)
├── Cluster health overview (nodes, regions)
├── Storage usage gauge (total/used/available)
├── Request rate chart (puts/gets/deletes per second)
├── Bandwidth chart (ingress/egress)
├── Top buckets by size
├── Recent errors/alerts
└── Quick actions (create bucket, upload file)

🪣 Buckets (/buckets)
├── List all buckets with stats (size, object count, created)
├── Search/filter buckets
├── Create new bucket (modal)
│   ├── Name, region, versioning toggle
│   └── Lifecycle rules (visual builder)
├── Bucket detail (/buckets/{name})
│   ├── Object browser (file explorer UI)
│   │   ├── Navigate "directories" (delimiter-based)
│   │   ├── Upload files (drag & drop, multipart)
│   │   ├── Download files
│   │   ├── Delete files (with confirmation)
│   │   ├── Preview files (images, text, JSON, video)
│   │   ├── Copy/move files
│   │   ├── Generate presigned URLs
│   │   └── View object metadata + versions
│   ├── Bucket settings
│   │   ├── Versioning (enable/suspend)
│   │   ├── Lifecycle rules (visual editor)
│   │   ├── CORS configuration
│   │   ├── Replication rules (v3)
│   │   └── Access policy (v4)
│   └── Bucket analytics
│       ├── Storage growth over time
│       ├── Request patterns
│       └── Top accessed objects

🖥️ Nodes (/nodes) — v2+
├── Cluster topology visualization
│   └── Interactive node map with health indicators
├── Node list with status, capacity, last seen
├── Node detail (/nodes/{id})
│   ├── CPU, memory, disk, network charts
│   ├── Objects stored, replication status
│   └── Logs viewer (streaming)
├── Add node wizard
└── Remove node (with drain/rebalance)

🌍 Regions (/regions) — v3+
├── World map with region markers
├── Replication status between regions
│   └── Replication lag gauges
├── Region detail
│   ├── Cluster health
│   ├── Cross-region bandwidth usage
│   └── Replication policy editor
└── Add region wizard

💾 Backups (/backups) — v2+
├── Backup schedule overview
├── Backup history (timeline)
├── Create backup (manual trigger)
├── Restore wizard
│   ├── Select backup
│   ├── Choose target (same cluster / new cluster)
│   ├── Preview changes
│   └── Execute restore
└── Backup target management
    ├── Add S3/GCS/Azure/NFS target
    └── Test connectivity

🔑 Access Management (/access)
├── v1: API key management
│   ├── Create/revoke access keys
│   └── Key permissions (per-bucket)
├── v4: Full IAM
│   ├── Users (/access/users)
│   ├── Groups (/access/groups)
│   ├── Policies (/access/policies)
│   │   └── Visual policy builder
│   └── Audit log (/access/audit)

📈 Analytics (/analytics) — v3+
├── Storage analytics
│   ├── Growth trends
│   ├── Cost estimation
│   └── Storage class distribution
├── Traffic analytics
│   ├── Request heatmap (time × bucket)
│   ├── Geographic request distribution
│   ├── Bandwidth by region
│   └── Error rate trends
├── Performance analytics
│   ├── Latency percentiles (p50, p95, p99)
│   ├── Throughput over time
│   └── Backend comparison (flat vs packed)
└── Capacity planning
    ├── Projected growth
    └── Scaling recommendations

⚙️ Settings (/settings)
├── Cluster configuration (YAML editor with Monaco)
├── Network settings (TLS, ports, domains)
├── Storage backend configuration
├── Notification channels (email, Slack, webhook)
├── CDN configuration
├── Maintenance mode toggle
└── System info (version, license, uptime)

🔍 Explorer (/explorer)
├── S3 API explorer (like Swagger/OpenAPI UI)
│   ├── Try any S3 operation interactively
│   ├── Generate code snippets (Go, Python, JS, curl)
│   └── View request/response with headers
└── Connection helper
    ├── Generate config for aws-cli
    ├── Generate config for s3cmd
    ├── Generate SDK initialization code
    └── Test connection button
```

### 13.3 Dashboard Architecture

```
┌────────────────────────────────────────────────────────┐
│  Next.js Dashboard                                      │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │  App Router (RSC + Client Components)             │  │
│  │                                                    │  │
│  │  Server Components:                               │  │
│  │  ├── Layout (navigation, auth check)              │  │
│  │  ├── Dashboard page (initial data fetch)          │  │
│  │  └── Settings page (config loading)               │  │
│  │                                                    │  │
│  │  Client Components:                               │  │
│  │  ├── ObjectBrowser (interactive file explorer)    │  │
│  │  ├── UploadDropzone (drag & drop + progress)     │  │
│  │  ├── ClusterMap (D3.js topology visualization)   │  │
│  │  ├── MetricsCharts (Recharts, real-time)         │  │
│  │  └── PolicyBuilder (visual IAM policy editor)    │  │
│  └──────────────────────────────────────────────────┘  │
│                          │                              │
│                          ▼                              │
│  ┌──────────────────────────────────────────────────┐  │
│  │  API Client Layer                                 │  │
│  │                                                    │  │
│  │  TanStack Query hooks:                            │  │
│  │  ├── useBuckets()                                 │  │
│  │  ├── useObjects(bucket, prefix)                   │  │
│  │  ├── useClusterHealth()                           │  │
│  │  ├── useMetrics(timeRange)                        │  │
│  │  └── useRealtimeEvents() ← WebSocket              │  │
│  └──────────────────────────────────────────────────┘  │
│                          │                              │
└──────────────────────────┼──────────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  OpenEndpoint API       │
              │                        │
              │  S3 API (:9000)        │ ← bucket/object operations
              │  Management API (:9001)│ ← cluster, metrics, config
              │  WebSocket (:9001/ws)  │ ← real-time events
              └────────────────────────┘
```

### 13.4 Management API (non-S3)

```
OpenEndpoint exposes a management API alongside the S3 API:

GET    /api/v1/status                    # cluster health
GET    /api/v1/metrics                   # prometheus-format metrics
GET    /api/v1/config                    # current configuration

GET    /api/v1/nodes                     # list nodes
GET    /api/v1/nodes/{id}               # node details
POST   /api/v1/nodes/{id}/drain         # drain node
DELETE /api/v1/nodes/{id}               # remove node

GET    /api/v1/regions                   # list regions
POST   /api/v1/regions                   # add region
GET    /api/v1/regions/{id}/status       # replication status

POST   /api/v1/backups                   # trigger backup
GET    /api/v1/backups                   # list backups
POST   /api/v1/backups/{id}/restore     # restore from backup

GET    /api/v1/access/keys              # list API keys
POST   /api/v1/access/keys              # create API key
DELETE /api/v1/access/keys/{id}         # revoke API key

WS     /api/v1/events                    # real-time event stream
  Events:
    object.created, object.deleted,
    node.joined, node.left, node.unhealthy,
    backup.started, backup.completed,
    replication.lag_warning,
    lifecycle.objects_expired
```

### 13.5 Object Browser UX

```
File Explorer-style interface:

┌─────────────────────────────────────────────────────────────┐
│  🪣 my-bucket  /  images  /  2024  /                        │
│  ← Back    📤 Upload    📁 New Folder    🗑️ Delete Selected  │
├─────────────────────────────────────────────────────────────┤
│  ☐  Name              Size      Modified         Actions    │
│  ──────────────────────────────────────────────────────────  │
│  ☐  📁 thumbnails/    —         2024-01-10       →          │
│  ☐  📁 originals/     —         2024-01-10       →          │
│  ☐  🖼️ hero.jpg       2.4 MB    2024-01-15 14:30 ⋮         │
│  ☐  🖼️ banner.png     856 KB    2024-01-14 09:15 ⋮         │
│  ☐  📄 metadata.json  1.2 KB    2024-01-13 11:00 ⋮         │
│                                                             │
│  ─────────────────────────────────────────────────          │
│  Page 1 of 5    ◀ Previous  Next ▶    1000 objects          │
├─────────────────────────────────────────────────────────────┤
│  ┌─ Upload Zone ──────────────────────────────────────────┐ │
│  │                                                         │ │
│  │   📎 Drag & drop files here, or click to browse        │ │
│  │                                                         │ │
│  │   Uploading: hero-large.jpg  ████████░░ 78%  12MB/s    │ │
│  │   Queued: 3 files (45 MB total)                        │ │
│  │                                                         │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘

Object Detail Panel (click on object):
┌─────────────────────────────────────────────────────────────┐
│  hero.jpg                                          ✕ Close  │
├─────────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────┐                     │
│  │                                    │                     │
│  │        [Image Preview]             │                     │
│  │                                    │                     │
│  └────────────────────────────────────┘                     │
│                                                             │
│  Key:          images/2024/hero.jpg                         │
│  Size:         2.4 MB                                       │
│  Content-Type: image/jpeg                                   │
│  ETag:         "d41d8cd98f00b204e9800998ecf8427e"          │
│  Last Modified: 2024-01-15 14:30:00 UTC                    │
│  Version ID:   01945a2e-7b3f-7abc-8def-1234567890ab        │
│                                                             │
│  Custom Metadata:                                           │
│  x-amz-meta-author: alice                                   │
│  x-amz-meta-project: website-redesign                       │
│                                                             │
│  Versions:                                                  │
│  ├── v3 (current) — 2024-01-15 — 2.4 MB                   │
│  ├── v2           — 2024-01-10 — 2.1 MB                   │
│  └── v1           — 2024-01-05 — 1.8 MB                   │
│                                                             │
│  [Download]  [Share Link]  [Copy URL]  [Delete]             │
└─────────────────────────────────────────────────────────────┘
```

---

## 14. CLI Tool

### 14.1 CLI Design: `openep`

```bash
# Installation
curl -fsSL https://get.openendpoint.com | sh
# or
go install github.com/OpenEndpoint/openendpoint/cmd/openep@latest

# Configuration
openep config set endpoint http://localhost:9000
openep config set access-key admin
openep config set secret-key changeme

# Bucket operations
openep bucket create my-bucket
openep bucket create my-bucket --versioning enabled
openep bucket list
openep bucket info my-bucket
openep bucket delete my-bucket

# Object operations
openep put my-bucket/photos/cat.jpg ./cat.jpg
openep get my-bucket/photos/cat.jpg ./downloaded-cat.jpg
openep ls my-bucket/photos/
openep ls my-bucket/photos/ --recursive --human-readable
openep rm my-bucket/photos/cat.jpg
openep rm my-bucket/photos/ --recursive --force

# Bulk operations
openep sync ./local-dir/ my-bucket/prefix/ --delete
openep cp my-bucket/source/ my-bucket/dest/ --recursive
openep mv my-bucket/old-name.jpg my-bucket/new-name.jpg

# Presigned URLs
openep presign my-bucket/photos/cat.jpg --expires 1h
openep presign my-bucket/uploads/data.csv --method PUT --expires 30m

# Versioning
openep versions my-bucket/photos/cat.jpg
openep get my-bucket/photos/cat.jpg --version-id abc123

# Cluster management (v2+)
openep cluster status
openep cluster nodes
openep cluster join 10.0.1.5:9002
openep cluster drain node-03
openep cluster rebalance --status

# Region management (v3+)
openep region list
openep region add eu-west --endpoint https://eu.openep.example.com
openep region status
openep region replication-lag

# Backup (v2+)
openep backup create --target s3://backup-bucket
openep backup list
openep backup restore backup-2024-01-15T020000Z
openep backup schedule --cron "0 2 * * *" --target s3://backup-bucket

# Server
openep server start
openep server start --config /etc/openendpoint/config.yaml
openep server start --data-dir /var/lib/openendpoint
```

### 14.2 CLI Output Design

```bash
$ openep cluster status

╔═══════════════════════════════════════════════════╗
║  OpenEndpoint Cluster Status                      ║
╚═══════════════════════════════════════════════════╝

  Cluster:    production
  Nodes:      5/5 healthy
  Regions:    3 (eu-west, us-east, ap-south)
  Storage:    12.4 TB used / 50 TB total (24.8%)
  Objects:    847,293,412
  Uptime:     45d 12h 30m

  Nodes:
  ┌────────────┬──────────┬───────────┬────────────┬─────────┐
  │ Node       │ Region   │ Status    │ Storage    │ Objects │
  ├────────────┼──────────┼───────────┼────────────┼─────────┤
  │ node-01    │ eu-west  │ ● Active  │ 2.5/10 TB  │ 169M    │
  │ node-02    │ eu-west  │ ● Active  │ 2.4/10 TB  │ 168M    │
  │ node-03    │ us-east  │ ● Active  │ 2.6/10 TB  │ 171M    │
  │ node-04    │ us-east  │ ● Active  │ 2.5/10 TB  │ 170M    │
  │ node-05    │ ap-south │ ● Active  │ 2.4/10 TB  │ 169M    │
  └────────────┴──────────┴───────────┴────────────┴─────────┘

  Replication:
  ┌──────────────────┬───────┬─────────┐
  │ Route            │ Lag   │ Status  │
  ├──────────────────┼───────┼─────────┤
  │ eu-west → us-east│ 12s   │ ● OK    │
  │ eu-west → ap-south│ 45s  │ ● OK    │
  │ us-east → ap-south│ 30s  │ ● OK    │
  └──────────────────┴───────┴─────────┘
```

---

## 15. Observability & Monitoring

### 15.1 Metrics (Prometheus)

```
# Storage metrics
openendpoint_storage_bytes_total{backend="flatfile|packed"} gauge
openendpoint_storage_bytes_used{backend="flatfile|packed"} gauge
openendpoint_storage_objects_total{bucket="..."} gauge

# Request metrics
openendpoint_http_requests_total{method, operation, status} counter
openendpoint_http_request_duration_seconds{method, operation} histogram
openendpoint_http_request_size_bytes{method} histogram
openendpoint_http_response_size_bytes{method} histogram

# Backend metrics
openendpoint_storage_put_duration_seconds{backend} histogram
openendpoint_storage_get_duration_seconds{backend} histogram
openendpoint_storage_delete_duration_seconds{backend} histogram

# Cluster metrics (v2)
openendpoint_cluster_nodes_total gauge
openendpoint_cluster_nodes_healthy gauge
openendpoint_replication_lag_seconds{source, target} gauge
openendpoint_replication_bytes_total{source, target} counter
openendpoint_rebalance_progress_ratio gauge

# Lifecycle metrics
openendpoint_lifecycle_objects_expired_total counter
openendpoint_lifecycle_bytes_freed_total counter

# Packed volume metrics
openendpoint_volume_count{state="active|sealed|compacting"} gauge
openendpoint_volume_compaction_duration_seconds histogram
openendpoint_volume_dead_bytes_ratio gauge

# Multipart metrics
openendpoint_multipart_uploads_active gauge
openendpoint_multipart_uploads_completed_total counter
openendpoint_multipart_uploads_aborted_total counter
```

### 15.2 Distributed Tracing

```
Integration with OpenTelemetry:

  PutObject trace:
  ┌─ HTTP Handler (50ms) ──────────────────────────────────┐
  │ ┌─ Auth: SigV4 Verify (2ms) ─┐                        │
  │ └─────────────────────────────┘                        │
  │ ┌─ Storage: Put (35ms) ──────────────────────────┐     │
  │ │ ┌─ Hash computation (5ms) ┐                    │     │
  │ │ └─────────────────────────┘                    │     │
  │ │ ┌─ Disk write (25ms) ─────┐                    │     │
  │ │ └─────────────────────────┘                    │     │
  │ │ ┌─ fsync (5ms) ──────────┐                     │     │
  │ │ └─────────────────────────┘                    │     │
  │ └────────────────────────────────────────────────┘     │
  │ ┌─ Metadata: Put (8ms) ─────────────────────────┐     │
  │ │ ┌─ Pebble batch write (6ms) ┐                 │     │
  │ │ └────────────────────────────┘                 │     │
  │ └────────────────────────────────────────────────┘     │
  │ ┌─ Replication: async (0ms, fire-and-forget) ──┐      │
  │ └──────────────────────────────────────────────┘      │
  └────────────────────────────────────────────────────────┘
```

### 15.3 Alerting Rules

```yaml
# Grafana/Prometheus alerting rules
groups:
  - name: openendpoint
    rules:
      - alert: HighErrorRate
        expr: rate(openendpoint_http_requests_total{status=~"5.."}[5m]) > 0.05
        for: 5m
        labels:
          severity: critical

      - alert: DiskSpaceLow
        expr: openendpoint_storage_bytes_used / openendpoint_storage_bytes_total > 0.85
        for: 10m
        labels:
          severity: warning

      - alert: ReplicationLagHigh
        expr: openendpoint_replication_lag_seconds > 300
        for: 5m
        labels:
          severity: warning

      - alert: NodeUnhealthy
        expr: openendpoint_cluster_nodes_healthy < openendpoint_cluster_nodes_total
        for: 2m
        labels:
          severity: critical

      - alert: CompactionNeeded
        expr: openendpoint_volume_dead_bytes_ratio > 0.4
        for: 1h
        labels:
          severity: info
```

---

## 16. Security Architecture

### 16.1 Defense in Depth

```
Layer 1: Network
  ├── TLS 1.3 (mandatory in production)
  ├── mTLS between cluster nodes
  ├── Network segmentation (management API on separate port)
  └── Rate limiting (per-IP token bucket)

Layer 2: Authentication
  ├── AWS Signature V4 (S3 API)
  ├── API keys (Management API)
  ├── OIDC / LDAP integration (v4)
  └── Short-lived tokens for dashboard

Layer 3: Authorization
  ├── Per-bucket access control (v1)
  ├── IAM policies (v4)
  ├── Resource-based policies (v4)
  └── Principle of least privilege

Layer 4: Data Protection
  ├── At-rest encryption: AES-256-GCM (v4)
  ├── In-transit encryption: TLS 1.3
  ├── Key management: integrated KMS or external (Vault)
  ├── Object Lock / WORM (v4)
  └── Secure deletion (overwrite on delete, optional)

Layer 5: Audit & Compliance
  ├── Immutable audit log (all API calls)
  ├── Access logs (S3-compatible format)
  ├── Data residency enforcement (v3)
  └── Compliance reports (GDPR, HIPAA markers)
```

### 16.2 Input Validation

```go
// Bucket name validation (S3 rules)
func validateBucketName(name string) error {
    if len(name) < 3 || len(name) > 63 {
        return ErrInvalidBucketName
    }
    if !regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`).MatchString(name) {
        return ErrInvalidBucketName
    }
    if strings.Contains(name, "..") {
        return ErrInvalidBucketName
    }
    if net.ParseIP(name) != nil {
        return ErrInvalidBucketName // no IP-format names
    }
    return nil
}

// Object key validation
func validateObjectKey(key string) error {
    if len(key) == 0 || len(key) > 1024 {
        return ErrInvalidObjectKey
    }
    // Prevent path traversal
    if strings.Contains(key, "..") {
        return ErrInvalidObjectKey
    }
    // No null bytes
    if strings.ContainsRune(key, 0) {
        return ErrInvalidObjectKey
    }
    return nil
}
```

---

## 17. Performance Engineering

### 17.1 Performance Targets by Version

| Metric | v1 (Single) | v2 (Cluster) | v3 (Multi-Region) |
|--------|-------------|-------------|-------------------|
| PUT/s (1MB) | 5,000 | 15,000 | 10,000 (per region) |
| GET/s (1MB) | 10,000 | 30,000 | 20,000 (per region) |
| PUT p99 latency | < 10ms | < 15ms | < 20ms (local) |
| GET p99 latency | < 5ms | < 8ms | < 10ms (local) |
| List 1000 keys | < 20ms | < 30ms | < 30ms |
| Max object size | 5TB | 5TB | 5TB |
| Concurrent conns | 10,000 | 50,000 | 100,000 |

### 17.2 Optimization Techniques

```
1. Zero-copy I/O
   ├── sendfile(2) for GET responses (kernel → socket, no userspace)
   ├── splice(2) for PUT data (socket → file, no userspace)
   └── mmap for packed volume index

2. Memory management
   ├── sync.Pool for buffer reuse (avoid GC pressure)
   ├── Pre-allocated byte buffers (32KB, 256KB, 1MB pools)
   └── Arena allocation for request-scoped objects (Go 1.22+)

3. I/O optimization
   ├── O_DIRECT for large objects (bypass page cache)
   ├── io_uring for async I/O (Linux 5.1+, optional)
   ├── Batch fsync (group commit every 10ms)
   └── AIO for concurrent reads from packed volumes

4. Network optimization
   ├── HTTP/2 for multiplexed connections
   ├── Keep-alive connection pooling
   ├── TCP_NODELAY for low-latency responses
   └── SO_REUSEPORT for multi-listener

5. Metadata optimization
   ├── Pebble bloom filters (10-bit, <1% false positive)
   ├── Read-through cache (LRU, 10000 hot object metas)
   ├── Prefix compression in LSM tree
   └── Dedicated WAL disk (separate from data)
```

---

## 18. Deployment & Operations

### 18.1 Deployment Options

```yaml
# 1. Single Binary
wget https://github.com/OpenEndpoint/openendpoint/releases/latest/download/openep-linux-amd64
chmod +x openep-linux-amd64
./openep-linux-amd64 server start

# 2. Docker
docker run -d \
  --name openendpoint \
  -p 9000:9000 \
  -p 9001:9001 \
  -v openep-data:/var/lib/openendpoint \
  openendpoint/openendpoint:latest

# 3. Docker Compose (cluster)
# docker-compose.yml
services:
  openep-1:
    image: openendpoint/openendpoint:latest
    environment:
      OPENEP_NODE_NAME: node-1
      OPENEP_CLUSTER_JOIN: openep-2:9002,openep-3:9002
    volumes:
      - node1-data:/var/lib/openendpoint
    ports:
      - "9000:9000"

  openep-2:
    image: openendpoint/openendpoint:latest
    environment:
      OPENEP_NODE_NAME: node-2
      OPENEP_CLUSTER_JOIN: openep-1:9002,openep-3:9002
    volumes:
      - node2-data:/var/lib/openendpoint

  openep-3:
    image: openendpoint/openendpoint:latest
    environment:
      OPENEP_NODE_NAME: node-3
      OPENEP_CLUSTER_JOIN: openep-1:9002,openep-2:9002
    volumes:
      - node3-data:/var/lib/openendpoint

# 4. Kubernetes (Helm)
helm repo add openendpoint https://charts.openendpoint.com
helm install my-storage openendpoint/openendpoint \
  --set cluster.nodes=5 \
  --set storage.size=100Gi \
  --set storage.backend=packed

# 5. Ansible Playbook (bare metal)
ansible-playbook openendpoint.yml -i inventory.ini
```

### 18.2 Kubernetes Architecture

```yaml
# Helm chart structure
openendpoint/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── statefulset.yaml      # OpenEndpoint nodes
│   ├── service.yaml          # S3 API service
│   ├── service-mgmt.yaml    # Management API service
│   ├── ingress.yaml          # S3 + Dashboard ingress
│   ├── configmap.yaml        # Configuration
│   ├── secret.yaml           # Credentials
│   ├── pvc.yaml              # Persistent volumes
│   ├── pdb.yaml              # Pod disruption budget
│   ├── hpa.yaml              # Horizontal pod autoscaler
│   ├── servicemonitor.yaml   # Prometheus ServiceMonitor
│   └── dashboard/
│       ├── deployment.yaml   # Dashboard (Next.js)
│       └── service.yaml
```

---

## 19. SDK & Developer Experience

### 19.1 Official SDKs (Future)

```
While any S3 SDK works, official SDKs add OpenEndpoint-specific features:

github.com/OpenEndpoint/sdk-go       # Go SDK
github.com/OpenEndpoint/sdk-js       # JavaScript/TypeScript SDK
github.com/OpenEndpoint/sdk-python   # Python SDK

Features beyond standard S3:
  • Cluster management operations
  • Real-time event subscriptions (WebSocket)
  • Presigned URL helpers
  • Multipart upload with automatic chunking
  • Retry with exponential backoff + jitter
  • Connection pooling
```

### 19.2 Developer Onboarding

```bash
# 30-second quickstart
docker run -d -p 9000:9000 -p 9001:9001 openendpoint/openendpoint

# Use with aws-cli (zero config on OpenEndpoint side)
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/

# Dashboard available at http://localhost:9001
```

---

## 20. Repository Structure

```
GitHub Organization: github.com/OpenEndpoint
═══════════════════════════════════════════════

Repo 1: openendpoint/openendpoint (Core)
─────────────────────────────────────────
  The Go server — single binary, all features.

  openendpoint/
  ├── cmd/
  │   ├── openep/                      # CLI + server binary
  │   │   ├── main.go
  │   │   └── commands/
  │   │       ├── server.go            # openep server start
  │   │       ├── bucket.go            # openep bucket create
  │   │       ├── object.go            # openep put/get/ls/rm
  │   │       ├── cluster.go           # openep cluster status
  │   │       ├── region.go            # openep region list
  │   │       ├── backup.go            # openep backup create
  │   │       └── config.go            # openep config set
  │   └── openep-gateway/             # CDN origin gateway (v3)
  │
  ├── internal/
  │   ├── api/                         # S3 HTTP handlers
  │   │   ├── router.go
  │   │   ├── bucket_handler.go
  │   │   ├── object_handler.go
  │   │   ├── multipart_handler.go
  │   │   ├── versioning_handler.go
  │   │   ├── lifecycle_handler.go
  │   │   ├── presign.go
  │   │   ├── response.go
  │   │   └── errors.go
  │   │
  │   ├── mgmt/                        # Management API
  │   │   ├── router.go
  │   │   ├── status.go
  │   │   ├── nodes.go
  │   │   ├── regions.go
  │   │   ├── backups.go
  │   │   ├── access.go
  │   │   └── websocket.go            # real-time events
  │   │
  │   ├── auth/
  │   │   ├── sigv4.go
  │   │   ├── sigv4_test.go
  │   │   ├── presign.go
  │   │   ├── credentials.go
  │   │   └── iam/                     # v4
  │   │       ├── policy.go
  │   │       ├── evaluator.go
  │   │       └── store.go
  │   │
  │   ├── engine/
  │   │   ├── service.go               # ObjectService
  │   │   ├── put.go
  │   │   ├── get.go
  │   │   ├── delete.go
  │   │   ├── list.go
  │   │   ├── copy.go
  │   │   ├── multipart.go
  │   │   ├── versioning.go
  │   │   ├── lifecycle.go
  │   │   └── locker.go               # sharded per-object locking
  │   │
  │   ├── storage/
  │   │   ├── backend.go               # interface
  │   │   ├── flatfile/
  │   │   │   ├── flatfile.go
  │   │   │   ├── flatfile_test.go
  │   │   │   └── directio.go          # O_DIRECT support
  │   │   └── packed/
  │   │       ├── packed.go
  │   │       ├── volume.go
  │   │       ├── needle.go
  │   │       ├── index.go
  │   │       ├── wal.go
  │   │       ├── compactor.go
  │   │       └── packed_test.go
  │   │
  │   ├── metadata/
  │   │   ├── store.go                 # interface
  │   │   ├── types.go
  │   │   ├── pebble/
  │   │   │   ├── pebble.go
  │   │   │   └── pebble_test.go
  │   │   └── bbolt/
  │   │       ├── bbolt.go
  │   │       └── bbolt_test.go
  │   │
  │   ├── cluster/                     # v2
  │   │   ├── manager.go
  │   │   ├── gossip.go
  │   │   ├── hashring.go
  │   │   ├── replicator.go
  │   │   ├── rebalancer.go
  │   │   ├── repair.go               # read repair + anti-entropy
  │   │   └── local.go                # v1 stub
  │   │
  │   ├── federation/                  # v3
  │   │   ├── manager.go
  │   │   ├── replication.go
  │   │   ├── conflict.go
  │   │   ├── geo_router.go
  │   │   └── bandwidth.go
  │   │
  │   ├── backup/                      # v2
  │   │   ├── engine.go
  │   │   ├── snapshot.go
  │   │   ├── restore.go
  │   │   ├── mirror.go
  │   │   └── targets/
  │   │       ├── s3.go
  │   │       ├── gcs.go
  │   │       ├── azure.go
  │   │       ├── nfs.go
  │   │       └── sftp.go
  │   │
  │   ├── cdn/                         # v3
  │   │   ├── handler.go
  │   │   ├── presign.go
  │   │   ├── invalidation.go
  │   │   └── providers/
  │   │       ├── cloudflare.go
  │   │       ├── cloudfront.go
  │   │       └── generic.go
  │   │
  │   ├── config/
  │   │   ├── config.go
  │   │   ├── validate.go
  │   │   └── defaults.go
  │   │
  │   └── telemetry/
  │       ├── metrics.go
  │       ├── tracing.go
  │       └── logging.go
  │
  ├── pkg/
  │   ├── s3types/                     # S3 XML types (shared)
  │   ├── checksum/
  │   └── byteutil/                    # aligned buffers, pools
  │
  ├── test/
  │   ├── integration/
  │   ├── e2e/
  │   ├── benchmark/
  │   └── chaos/                       # chaos engineering tests
  │
  ├── deploy/
  │   ├── docker/
  │   │   ├── Dockerfile
  │   │   └── docker-compose.yml
  │   ├── helm/
  │   │   └── openendpoint/
  │   ├── ansible/
  │   └── terraform/
  │
  ├── docs/
  │   ├── architecture.md
  │   ├── api-reference.md
  │   ├── deployment.md
  │   ├── clustering.md
  │   ├── federation.md
  │   └── performance-tuning.md
  │
  ├── scripts/
  │   ├── build.sh
  │   ├── test.sh
  │   └── release.sh
  │
  ├── go.mod
  ├── go.sum
  ├── Makefile
  ├── LICENSE                          # Apache 2.0
  └── README.md


Repo 2: openendpoint/dashboard (Web UI)
─────────────────────────────────────────
  Next.js dashboard application.

  dashboard/
  ├── src/
  │   ├── app/
  │   │   ├── layout.tsx
  │   │   ├── page.tsx                 # Dashboard home
  │   │   ├── buckets/
  │   │   │   ├── page.tsx             # Bucket list
  │   │   │   └── [name]/
  │   │   │       ├── page.tsx         # Object browser
  │   │   │       └── settings/
  │   │   ├── nodes/                   # v2
  │   │   ├── regions/                 # v3
  │   │   ├── backups/                 # v2
  │   │   ├── access/
  │   │   ├── analytics/              # v3
  │   │   ├── explorer/               # API explorer
  │   │   └── settings/
  │   │
  │   ├── components/
  │   │   ├── ui/                      # shadcn/ui
  │   │   ├── bucket/
  │   │   │   ├── BucketList.tsx
  │   │   │   ├── CreateBucketDialog.tsx
  │   │   │   └── BucketStats.tsx
  │   │   ├── object/
  │   │   │   ├── ObjectBrowser.tsx
  │   │   │   ├── ObjectPreview.tsx
  │   │   │   ├── UploadDropzone.tsx
  │   │   │   ├── VersionHistory.tsx
  │   │   │   └── ShareDialog.tsx
  │   │   ├── cluster/
  │   │   │   ├── ClusterTopology.tsx  # D3 visualization
  │   │   │   ├── NodeCard.tsx
  │   │   │   └── RebalanceProgress.tsx
  │   │   ├── region/
  │   │   │   ├── WorldMap.tsx
  │   │   │   └── ReplicationStatus.tsx
  │   │   ├── charts/
  │   │   │   ├── StorageGauge.tsx
  │   │   │   ├── RequestRateChart.tsx
  │   │   │   └── BandwidthChart.tsx
  │   │   └── layout/
  │   │       ├── Sidebar.tsx
  │   │       ├── Header.tsx
  │   │       └── CommandPalette.tsx   # Cmd+K search
  │   │
  │   ├── hooks/
  │   │   ├── useBuckets.ts
  │   │   ├── useObjects.ts
  │   │   ├── useCluster.ts
  │   │   ├── useMetrics.ts
  │   │   └── useRealtimeEvents.ts
  │   │
  │   ├── lib/
  │   │   ├── api-client.ts            # OpenEndpoint API client
  │   │   ├── s3-client.ts             # S3 operations
  │   │   └── utils.ts
  │   │
  │   └── types/
  │       ├── bucket.ts
  │       ├── object.ts
  │       ├── cluster.ts
  │       └── api.ts
  │
  ├── public/
  ├── package.json
  ├── next.config.ts
  ├── tailwind.config.ts
  ├── tsconfig.json
  └── Dockerfile


Repo 3: openendpoint/docs (Documentation)
─────────────────────────────────────────
  Documentation website (Docusaurus or similar).

  docs/
  ├── docs/
  │   ├── getting-started/
  │   ├── configuration/
  │   ├── s3-compatibility/
  │   ├── clustering/
  │   ├── federation/
  │   ├── backup-restore/
  │   ├── cdn-integration/
  │   ├── security/
  │   ├── performance/
  │   ├── api-reference/
  │   └── troubleshooting/
  └── docusaurus.config.js


Repo 4: openendpoint/helm-charts
Repo 5: openendpoint/terraform-provider
Repo 6: openendpoint/sdk-go (future)
Repo 7: openendpoint/sdk-js (future)
Repo 8: openendpoint/sdk-python (future)
```

---

## 21. Competitive Analysis

```
Feature Comparison Matrix:
═══════════════════════════════════════════════════════════════════

                    OpenEndpoint  MinIO     SeaweedFS  Ceph RGW
─────────────────────────────────────────────────────────────────
License             Apache 2.0    AGPLv3    Apache 2.0 LGPL
Language            Go            Go        Go         C++
Single Binary       ✅            ✅         ✅         ❌
S3 Compatible       ✅            ✅         ✅         ✅
Web Dashboard       ✅ (React)    Basic      ❌         ❌
Multi-Region        ✅ (v3)       Enterprise ✅         ✅
CDN Integration     ✅ (v3)       ❌         ❌         ❌
Pluggable Backends  ✅            ❌         ✅         ❌
Packed Volumes      ✅            ❌         ✅         ❌
Erasure Coding      ✅ (v2)       ✅         ✅         ✅
Object Versioning   ✅            ✅         ❌         ✅
Lifecycle Policies  ✅            ✅         ✅         ✅
Backup Targets      ✅ (v2)       Basic      ❌         ❌
Mirror Mode         ✅ (v2)       ✅         ❌         ✅
IAM Policies        ✅ (v4)       ✅         ❌         ✅
Object Lock/WORM    ✅ (v4)       ✅         ❌         ✅
S3 Select           ✅ (v5)       ✅         ❌         ✅
Event Notifications ✅ (v4)       ✅         ❌         ✅
CLI Quality         ✅            Good       Basic      Complex
Developer DX        ✅✅          Good       Basic      Poor
Memory Footprint    Low           Medium     Low        High
─────────────────────────────────────────────────────────────────
```

---

## 22. Implementation Roadmap

### v1.0 — "Foundation" (12 weeks)

```
Week 1-2: Project Setup + Storage Layer
  ├── Go module, Makefile, CI pipeline
  ├── StorageBackend interface
  ├── Flat file backend implementation
  ├── Unit tests with table-driven tests
  └── Benchmark framework

Week 3-4: Metadata Layer + Core Engine
  ├── MetadataStore interface
  ├── Pebble implementation
  ├── bbolt implementation
  ├── ObjectService (Put, Get, Delete, Head)
  ├── Sharded per-object locking
  └── Integration tests

Week 5-6: S3 API
  ├── HTTP router (chi)
  ├── All CRUD handlers + ListObjectsV2
  ├── S3 XML serialization/deserialization
  ├── AWS Signature V4 verification
  ├── Presigned URL generation + verification
  ├── Virtual-hosted style support
  └── AWS SDK compatibility tests

Week 7-8: Multipart Upload
  ├── InitiateMultipartUpload
  ├── UploadPart + part storage
  ├── CompleteMultipartUpload (concatenation)
  ├── AbortMultipartUpload + cleanup
  ├── ListParts, ListMultipartUploads
  └── Large file upload tests (1GB+)

Week 9-10: Versioning + Lifecycle
  ├── Versioning state machine
  ├── Version-aware CRUD operations
  ├── Delete markers
  ├── ListObjectVersions
  ├── Lifecycle rule engine
  ├── Background expiration processor
  └── Noncurrent version cleanup

Week 11: Packed Volume Backend
  ├── Volume file format (read/write)
  ├── Needle operations
  ├── In-memory index + WAL
  ├── Background compaction
  └── Backend-agnostic integration tests

Week 12: CLI + Dashboard MVP + Polish
  ├── openep CLI (basic commands)
  ├── Dashboard MVP (bucket browse, upload)
  ├── Docker image + Helm chart skeleton
  ├── Prometheus metrics endpoint
  ├── Health + readiness endpoints
  ├── README, quickstart guide
  └── Release v1.0.0

v2.0 — "Cluster" (16 weeks after v1)
v3.0 — "Federation" (16 weeks after v2)
v4.0 — "Platform" (20 weeks after v3)
v5.0 — "Intelligence" (20 weeks after v4)
```

---

*OpenEndpoint — Your endpoints. Your data. Your rules.*

*This document is the complete technical vision for OpenEndpoint.
It evolves with the project. Every decision here has been made
with the goal of building the best self-hosted object storage
platform in the world.*
