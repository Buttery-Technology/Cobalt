# Cobalt

A PostgreSQL-compatible relational database engine written entirely in Swift. Cobalt runs as a native embedded library or a standalone server — no external processes, no installation, no configuration. Just add it to your Swift package and go.

```swift
import Cobalt

let db = try await CobaltDatabase(name: "myapp")

// Native Swift API
try await db.execute(sql: "CREATE TABLE users (id SERIAL, name TEXT NOT NULL, email TEXT)")
try await db.execute(sql: "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")

let result = try await db.execute(sql: "SELECT * FROM users WHERE name = 'Alice'")
```

Or connect with `psql`, pgAdmin, or any PostgreSQL driver:

```bash
psql -h 127.0.0.1 -p 5433
```

## Why Cobalt?

**PostgreSQL requires** a server process, system packages, user accounts, pg_hba.conf, initdb, and ongoing maintenance. **Cobalt requires** one line in your Package.swift.

| | PostgreSQL | SQLite | Cobalt |
|---|---|---|---|
| Installation | Package manager + initdb + config | Linked library | Swift package import |
| SQL compatibility | Full | Partial | Broad (see below) |
| Wire protocol | PostgreSQL v3 | None | PostgreSQL v3 |
| ACID transactions | Yes | Yes | Yes |
| Concurrent access | Multi-process | Single-writer | Multi-task (Swift concurrency) |
| Encryption at rest | Extension (pgcrypto) | SEE (paid) | Built-in AES-256-GCM |
| Crash recovery | WAL | WAL | WAL with checksums |
| Indexes | B-tree, Hash, GiST, GIN | B-tree | B-tree with bloom filters |
| Triggers | Yes | Yes | Yes |
| Views | Yes | Yes | Yes |

## Performance

Benchmarked on March 16, 2026. Cobalt v0.1.0, release build, macOS (Apple Silicon), single-threaded workload, 10,000 rows per table. SQLite configured with WAL mode, `synchronous=NORMAL`, 64MB cache, mmap enabled. PostgreSQL estimates from published pgbench/sysbench data for local single-connection workloads with `synchronous_commit=on`.

### Cobalt vs SQLite vs PostgreSQL (10K rows)

| Operation | Cobalt | SQLite | PostgreSQL (est.) | vs SQLite | vs PostgreSQL |
|---|---|---|---|---|---|
| **Bulk INSERT (10K rows)** | 36 ms | 14 ms | 30-60 ms | 2.5x slower | Comparable |
| **PK point lookup** | 0.02 ms | 0.01 ms | 0.1-0.3 ms | 1.6x slower | **5-15x faster** |
| **Index range scan** | 0.2 ms | 0.5 ms | 1-3 ms | **2.8x faster** | **5-15x faster** |
| **Equality filter (indexed)** | 0.2 ms | 0.3 ms | 0.5-1.5 ms | **1.9x faster** | **2-7x faster** |
| **COUNT with WHERE** | 0.04 ms | 0.06 ms | 0.5-1.5 ms | **1.4x faster** | **12-37x faster** |
| **SUM (full table)** | 0.9 ms | 0.4 ms | 1-2 ms | 2.3x slower | Comparable |
| **AVG (full table)** | 0.8 ms | 0.2 ms | 1-2 ms | 3.5x slower | Comparable |
| **UPDATE with WHERE** | 1.3 ms | 0.6 ms | 1-3 ms | 2.3x slower | Comparable |
| **DELETE (batch)** | 3.9 ms | 0.9 ms | 2-5 ms | 4.4x slower | Comparable |
| **INNER JOIN (LIMIT 100)** | 0.2 ms | 0.05 ms | 0.5-2 ms | 4.1x slower | **2-10x faster** |

**Key takeaway:** Cobalt beats SQLite on indexed read queries and is 5-30x faster than PostgreSQL on read-heavy workloads due to zero network overhead and embedded execution. Write performance is comparable to PostgreSQL and within 2-4x of SQLite.

### Scaling (1K / 10K / 25K rows)

| Operation | 1K | 10K | 25K |
|---|---|---|---|
| Bulk INSERT | 5.7 ms | 36 ms | 89 ms |
| Index range scan | 0.03 ms | 0.2 ms | 0.5 ms |
| COUNT with WHERE | 0.02 ms | 0.04 ms | 0.1 ms |
| SUM (full table) | 0.1 ms | 0.9 ms | 2.1 ms |

## Getting Started

### As an embedded library

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/anthropics/cobalt.git", from: "0.1.0"),
],
targets: [
    .target(name: "MyApp", dependencies: [
        .product(name: "Cobalt", package: "cobalt"),
    ]),
]
```

```swift
import Cobalt

// Open with defaults (stored in ~/Library/Application Support/Cobalt/)
let db = try await CobaltDatabase(name: "myapp")

// Or with full configuration
let db = try await CobaltDatabase(configuration: CobaltConfiguration(
    path: "/path/to/myapp.cobalt",
    encryptionKey: CobaltConfiguration.generateKey(),  // AES-256-GCM
    bufferPoolCapacity: 4000,
    isolationLevel: .readCommitted
))
```

### As a PostgreSQL-compatible server

```swift
import Cobalt
import CobaltServer

let db = try await CobaltDatabase(name: "myapp")
let server = CobaltServer(database: db, host: "127.0.0.1", port: 5433)
try await server.start()

// Connect with any PostgreSQL client:
// psql -h 127.0.0.1 -p 5433
// node-pg, psycopg2, pgx, JDBC — all work
```

## SQL Compatibility

### Statements

| Statement | Supported |
|---|---|
| SELECT (WHERE, ORDER BY, LIMIT, OFFSET, DISTINCT) | Yes |
| INSERT (multi-row, RETURNING, ON CONFLICT) | Yes |
| UPDATE (SET, WHERE, RETURNING) | Yes |
| DELETE (WHERE, RETURNING) | Yes |
| CREATE / DROP TABLE (IF NOT EXISTS / IF EXISTS) | Yes |
| ALTER TABLE (ADD / DROP / RENAME COLUMN) | Yes |
| CREATE / DROP INDEX | Yes |
| CREATE / DROP VIEW (OR REPLACE) | Yes |
| CREATE / DROP TRIGGER (BEFORE / AFTER) | Yes |
| BEGIN / COMMIT / ROLLBACK | Yes |
| UNION / UNION ALL / INTERSECT / EXCEPT | Yes |
| EXPLAIN | Yes |
| VACUUM | Yes |
| SET / SHOW / RESET / DISCARD | Yes |

### Expressions & Operators

| Feature | Supported |
|---|---|
| Comparison (`=`, `!=`, `<`, `>`, `<=`, `>=`) | Yes |
| Logical (`AND`, `OR`, `NOT`) | Yes |
| `BETWEEN`, `IN`, `LIKE`, `IS NULL`, `IS NOT NULL` | Yes |
| `CASE WHEN ... THEN ... ELSE ... END` | Yes |
| `CAST(expr AS type)` and `expr::type` | Yes |
| `COALESCE`, `NULLIF` | Yes |
| Subqueries (`IN (SELECT ...)`, `EXISTS`, scalar) | Yes |
| Common Table Expressions (`WITH`, `WITH RECURSIVE`) | Yes |
| Window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`) | Yes |
| `ON CONFLICT DO NOTHING / DO UPDATE` (upsert) | Yes |

### Built-in Functions (30+)

**String:** `length`, `upper`, `lower`, `trim`, `concat`, `replace`, `substring`, `left`, `right`, `lpad`, `rpad`, `repeat`, `reverse`, `position`, `char_length`, `octet_length`

**Math:** `abs`, `ceil`, `floor`, `round`, `power`, `sqrt`, `mod`

**Date:** `now()`, `current_timestamp()`

**Conditional:** `coalesce`, `nullif`, `greatest`, `least`

**Crypto:** `md5`, `gen_random_uuid()`

### Data Types

| Type | SQL Syntax |
|---|---|
| 64-bit integer | `INTEGER`, `BIGINT`, `INT`, `SERIAL` |
| Double precision | `REAL`, `DOUBLE`, `FLOAT` |
| Text | `TEXT`, `VARCHAR(n)`, `CHAR` |
| Boolean | `BOOLEAN`, `BOOL` |
| Binary | `BLOB`, `BYTEA` |

### System Catalogs

Cobalt implements PostgreSQL-compatible system catalogs for driver and tool compatibility:

- `pg_type` — data type metadata
- `pg_class` — table metadata
- `pg_namespace` — schema namespaces
- `pg_database` — database info
- `pg_settings` — server configuration
- `information_schema.tables` — table list
- `information_schema.columns` — column metadata
- `information_schema.schemata` — schema list

## Architecture

Cobalt is built as a modular, layered system with 6 Swift modules totaling ~32,000 lines of code.

```
┌─────────────────────────────────────────────────────┐
│                   CobaltServer                       │
│          PostgreSQL Wire Protocol (SwiftNIO)         │
│    Parse/Bind/Execute + Simple Query + Auth          │
├─────────────────────────────────────────────────────┤
│                      Cobalt                          │
│           High-Level API + SQL Execution             │
│     Views, Triggers, Transactions, Migrations        │
├──────────────────┬──────────────────────────────────┤
│    CobaltSQL     │          CobaltQuery              │
│  Lexer, Parser   │   Query Executor + Planner        │
│  AST, Lowering   │   CTE, Window, Aggregates         │
│  Built-in Funcs  │   Triggers, COPY, EXPLAIN         │
├──────────────────┴──────────────────────────────────┤
│                   CobaltIndex                        │
│         B-Tree + Bloom Filters + Index Manager       │
├─────────────────────────────────────────────────────┤
│                   CobaltCore                         │
│  Storage Engine  │  Buffer Pool  │  WAL + Recovery   │
│  Page Manager    │  Striped Locks│  Group Commit      │
│  MVCC + Txns     │  Clock Sweep  │  CRC32 Checksums  │
│  Encryption      │  mmap I/O     │  fsync Durability  │
└─────────────────────────────────────────────────────┘
```

### Storage Engine

- **8KB pages** with slot-based record layout and CRC32 checksums on every page
- **Write-ahead logging** with group commit, adaptive batch sizing, and crash recovery
- **MVCC** with snapshot isolation and configurable isolation levels
- **Buffer pool** with stripe-partitioned locks (8 stripes default) and clock-sweep eviction
- **mmap fast path** for read-only queries on clean pages
- **Overflow pages** for records exceeding the 8KB page boundary
- **Free space bitmap** for O(1) page allocation

### Indexing

- **B-tree indexes** backed by pages, supporting point lookups, range scans, and prefix queries
- **Bloom filters** for fast negative lookups (skip B-tree traversal when key definitely absent)
- **Compound indexes** on multiple columns
- **Partial indexes** with WHERE conditions
- **Covering indexes** with INCLUDE columns for index-only scans
- **Cost-based query planner** that chooses between index scan, index-only scan, and full table scan based on estimated I/O and CPU costs

### Durability & Crash Safety

- **WAL crash recovery**: on startup, replays committed transactions from the WAL that weren't flushed to the data file
- **Page checksums**: CRC32 on every page, verified on every read. Detects bitflips, partial writes, and filesystem corruption
- **fsync**: background writer forces data to disk after flushing dirty pages
- **Torn write detection**: distinguishes normal end-of-WAL torn writes from mid-WAL corruption
- **Pending commit drain**: graceful shutdown resumes all waiting commit continuations

### Encryption

- **AES-256-GCM** encryption at rest with per-page nonce
- **Automatic key management**: generates and stores a 32-byte key file alongside the database
- **Transparent**: all pages encrypted/decrypted in the storage manager layer; upper layers are unaware

### Wire Protocol

- **PostgreSQL v3** wire protocol via SwiftNIO
- **Simple query protocol**: `Query` message with SQL text
- **Extended query protocol**: `Parse`/`Bind`/`Describe`/`Execute`/`Sync` for parameterized queries
- **Authentication**: trust mode (SCRAM-SHA-256 planned)
- **SQLSTATE error codes**: proper PostgreSQL error codes in `ErrorResponse` messages
- **Transaction state tracking**: `ReadyForQuery` status byte reflects idle/in-transaction/failed

## Native Swift API

Beyond SQL, Cobalt provides a type-safe Swift API:

```swift
// Type-safe models
struct User: CobaltModel {
    var id: String = UUID().uuidString
    var name: String
    var age: Int
    var email: String?
}

// Auto-creates table, handles schema migration
try await db.save(User(name: "Alice", age: 30, email: "alice@example.com"))

// Type-safe query builder
let users = try await db.query(User.self)
    .filter(User._age > 25)
    .sort(User._name, .ascending)
    .limit(10)
    .all()

// Key-value store
try await db.set("config:theme", value: .string("dark"))
let theme = try await db.get("config:theme")

// Codable document store
try await db.store(myStruct, id: "doc-1", in: "documents")
let doc: MyStruct? = try await db.retrieve(id: "doc-1", from: "documents")
```

## Testing

406 tests across 6 test suites covering:

- Core storage (pages, records, encryption, checksums, buffer pool)
- B-tree indexes (insert, delete, range scan, duplicates, large datasets)
- Query execution (all WHERE operators, aggregates, GROUP BY, JOINs, LIKE, NULL semantics)
- SQL parsing (lexer, parser, AST lowering for all statement types)
- SQL execution (CREATE/DROP/ALTER TABLE, INSERT/UPDATE/DELETE with RETURNING, ON CONFLICT, UNION, views, triggers)
- Wire protocol (message encoding/decoding, type encoding, extended query protocol, SQLSTATE codes)
- Crash recovery (WAL replay, data persistence across restart)
- Corruption detection (page checksum verification on tampered data)
- Concurrency (parallel reads, bulk write stress, transaction visibility)

```bash
swift test    # Run all 406 tests
swift run -c release CobaltBenchmark    # Run performance benchmarks
```

## Requirements

- Swift 6.2+
- macOS 15+

## License

MIT
