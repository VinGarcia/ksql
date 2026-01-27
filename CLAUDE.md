# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KSQL (Keep It Simple SQL) is a Go library providing a simple, well-designed API for SQL database interactions. It prioritizes ease of learning, debugging, and avoiding common pitfalls over introducing new features. The library is decoupled from database backends, working on top of trusted technologies like `pgx` and `database/sql`.

## Development Commands

### Testing

```bash
# Run all tests (requires docker for database containers)
make test

# Run tests for specific module
richgo test ./...
richgo test ./kbuilder/...
richgo test ./adapters/kpgx/...

# Run tests with coverage (used by CI)
./scripts/run-all-tests.sh

# Pre-download database images to avoid test timeouts (run once)
make pre-download-all-images
```

**Important**: Tests use docker-test to spin up database containers. You need:
- Docker installed and running
- Ability to run docker without sudo (add yourself to docker group)
- Pre-downloaded images (via `make pre-download-all-images`) to avoid timeouts

### Linting

```bash
# Run linters (staticcheck and go vet)
make lint
```

### Building and Dependencies

```bash
# Tidy all go.mod files in all modules
make tidy

# Setup development tools
make setup
```

### Benchmarking

```bash
# Run benchmarks (default 5s per benchmark)
make bench

# Run with custom duration
make bench TIME=10s
```

### Examples

```bash
make example_service
make example_logger
make example_overview
```

## Architecture

### Layered Abstraction Model

KSQL uses a clean separation of concerns through interfaces:

```
Application Code
    ↓
ksql.Provider Interface (Public API)
    ↓
DB Client (ksql.go - core implementation)
    ↓
DBAdapter Interface (abstraction layer)
    ↓
Database Adapters (kpgx, kmysql, etc.)
    ↓
Database Drivers (pgx, database/sql, etc.)
```

### Key Design Patterns

1. **Adapter Pattern**: The `DBAdapter` interface decouples KSQL from specific database drivers. Each adapter (kpgx, kmysql, ksqlite3, etc.) wraps a different backend implementation.

2. **Dialect Pattern**: Different databases use different SQL syntax. The `sqldialect.Provider` interface handles placeholders (`$1` vs `?` vs `@p1`), escaping, and insert methods (RETURNING vs LAST_INSERT_ID).

3. **Modifier System**: Extensible field-level transformations via struct tags. Built-in modifiers include `json`, `timeNowUTC`, `skipUpdates`, `skipInserts`. Users can register custom modifiers via `ksqlmodifiers.RegisterAttrModifier()`.

4. **Struct Metadata Caching**: Heavy use of reflection with caching in `internal/structs`. The `StructInfo` type contains parsed field metadata (column names, modifiers, indices) and SELECT query strings are cached per dialect for performance.

5. **Single Error Handling**: Methods return errors once (not multiple times). Key error types: `ErrRecordNotFound`, `ErrNoValuesToUpdate`, `ErrRecordMissingIDs`, `ErrAbortIteration`.

### Package Structure

- **ksql** (root): Core library implementing `Provider` interface. Main types: `DB`, `Table`, `Config`.
- **contracts.go**: Public interfaces (`Provider`, `Table`, `ChunkParser`) and error types.
- **adapters/**: Database-specific implementations (kpgx, kpgx5, kmysql, ksqlserver, ksqlite3, modernc-ksqlite).
- **sqldialect/**: SQL dialect abstraction for different database syntaxes.
- **internal/structs/**: Reflection and struct metadata parsing/caching. Core type: `StructInfo`.
- **internal/modifiers/**: Built-in attribute transformation system.
- **ksqlmodifiers/**: Public modifier API for custom attribute transformations.
- **kbuilder/**: Experimental query builder with fluent API (subject to breaking changes).
- **nullable/**: Pointer constructor helpers (`Int()`, `String()`, etc.) for partial updates.
- **ksqltest/**: Testing utilities and mocks.

### Key Data Flows

**Query Flow**:
1. User calls `db.Query(ctx, &results, "FROM users WHERE id = $1", 42)`
2. `GetTagInfo()` extracts/retrieves cached struct metadata
3. If query starts with "FROM": `buildSelectQuery()` generates SELECT clause from struct fields
4. `db.QueryContext()` executes via adapter → database driver
5. `scanRows()` populates structs using reflection and applies modifiers

**Insert Flow**:
1. User calls `db.Insert(ctx, UsersTable, &user)`
2. `Table.insertMethodFor()` determines strategy based on dialect (RETURNING vs LAST_INSERT_ID vs no ID)
3. Build INSERT query from `StructInfo`
4. Apply modifiers (e.g., `timeNowUTC` sets current time)
5. Execute and populate ID field from result

### Module System

KSQL uses Go workspaces (`go.work`) with multiple modules:
- Root module: `github.com/vingarcia/ksql`
- Each adapter is a separate module: `github.com/vingarcia/ksql/adapters/kpgx`
- `kbuilder` is a separate module
- `examples` and `benchmarks` are separate modules

When making changes to the core library that affect adapters, you may need to use the `run-with-replace.sh` script (in CI) or update all modules.

## Important Conventions

### Struct Tags

- `ksql:"column_name"`: Map field to database column
- `ksql:"column_name,json"`: Store field as JSON
- `ksql:"column_name,timeNowUTC"`: Auto-set to `time.Now().UTC()` on insert/update
- `ksql:"column_name,timeNowUTC/skipUpdates"`: Only set on insert, skip on updates
- `tablename:"table_name"`: For nested structs in JOIN queries

### Placeholder Syntax

Different databases use different placeholders in queries:
- Postgres: `$1`, `$2`, `$3`
- MySQL/SQLite: `?`
- SQLServer: `@p1`, `@p2`, `@p3`

### Query String Omission

When querying into structs, you can omit the SELECT clause. If the query starts with "FROM", KSQL builds the SELECT clause from struct fields:

```go
db.Query(ctx, &users, "FROM users WHERE type = $1", "admin")
// Becomes: SELECT user_id, name, type, ... FROM users WHERE type = $1
```

### Transactions

Use the `Transaction()` method with automatic rollback on error:

```go
db.Transaction(ctx, func(db ksql.Provider) error {
    // Operations here
    return nil // commits, return error rolls back
})
```

### Performance Notes

- When using kpgx adapters (pgx v4/v5), prepared statements are automatically cached by pgx
- Other adapters (MySQL, SQLServer, SQLite) don't cache prepared statements yet (TODO)
- Benchmarks show KSQL is competitive: ~5% slower than sqlx, 42% faster than sqlx when using pgx, 13.66% slower than raw pgx
- Struct metadata caching significantly reduces reflection overhead

## Testing Patterns

- Use `ksql.Mock` for testing code that depends on `ksql.Provider`
- Tests in `adapters/` subdirectories require docker containers
- Each adapter tests against its specific database using docker-test library
- Coverage is tracked across all modules via `coverage.txt` files

## kbuilder Package (Experimental)

The query builder is experimental and subject to breaking changes. It provides fluent API for programmatic query construction:

```go
builder.Insert().Into("users").Data(users).BuildQuery(dialect)
builder.Query().Select("*").From("users").Where("id = ?").BuildQuery(dialect)
```

Current limitations: Only supports SELECT and INSERT. Update and Delete support are TODO items.
