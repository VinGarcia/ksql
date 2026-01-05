[![CI](https://github.com/VinGarcia/ksql/actions/workflows/ci.yml/badge.svg)](https://github.com/VinGarcia/ksql/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/VinGarcia/ksql/branch/master/graph/badge.svg?token=5CNJ867C66)](https://codecov.io/gh/VinGarcia/ksql)
[![Go Reference](https://pkg.go.dev/badge/github.com/vingarcia/ksql.svg)](https://pkg.go.dev/github.com/vingarcia/ksql)
![Go Report Card](https://goreportcard.com/badge/github.com/vingarcia/ksql)

# KSQL the Keep it Simple SQL library

KSQL was created to offer an actually simple and satisfactory
tool for interacting with SQL Databases in Golang.

The core goal of KSQL is not to offer new features that
are unavailable on other libraries (although we do have some),
but to offer a well-thought and well-planned API so that users
have an easier time, learning, debugging, and avoiding common pitfalls.

KSQL is also decoupled from its backend so that
the actual communication with the database is performed by
well-known and trusted technologies, namely: `pgx` and `database/sql`.
You can even create your own backend adapter for KSQL which is
useful in some situations.

In this README you will find examples for "Getting Started" with the library,
for more advanced use-cases [please read our Wiki](https://github.com/VinGarcia/ksql/wiki).

## Outstanding Features

- Every operation returns errors a single time, so its easier to handle them
- Helper functions for everyday operations, namely: Insert, Patch and Delete
- Generic and powerful functions for Querying and Scanning data into structs
- Works on top of existing battle-tested libraries such as `database/sql` and `pgx`
- Supports `sql.Scanner` and `sql.Valuer` and also all `pgx` special types (when using `kpgx`)
- And many other features designed to make your life easier

## Open to Work

> Hi! I'm currently looking for new opportunities as a Senior Software or Platform Engineer—preferably working with Golang, backend systems, or open-source infrastructure. If you're hiring or know someone who is, feel free to reach out: vingarcia00@gmail.com or on [LinkedIn](https://www.linkedin.com/in/vingarcia00/).

## Let's start with some Code:

This short example below is a TLDR version to illustrate how easy it is to use KSQL.

You will find more complete examples in the sections below.

> This example is available on ./examples/overview/main.go if you want to run it

```golang
{{ readFile "examples/overview/main.go" -}}
```

> Note: In the example above we are using the `$1`, `$2` and `$3` as placeholders on the query
> because this example is meant to run on top of Postgres.
>
> If you are running on top of MySQL or SQLite use `?` instead, and if you are running
> on top of SQLServer use `@p1`, `@p2` and `@p3` instead.

## Supported Adapters:

We support a few different adapters,
one of them is illustrated above (`kpgx`),
the other ones have the exact same signature
but work on different databases or driver versions,
they are:

- `kpgx.New(ctx, os.Getenv("DATABASE_URL"), ksql.Config{})` for Postgres, it works on top of `pgxpool`
  and [pgx](https://github.com/jackc/pgx) version 4, download it with:

  ```bash
  go get github.com/vingarcia/ksql/adapters/kpgx
  ```
- `kpgx5.New(ctx, os.Getenv("DATABASE_URL"), ksql.Config{})` for Postgres, it works on top of `pgxpool`
  and [pgx](https://github.com/jackc/pgx) version 5, download it with:

  ```bash
  go get github.com/vingarcia/ksql/adapters/kpgx5
  ```
- `kmysql.New(ctx, os.Getenv("DATABASE_URL"), ksql.Config{})` for MySQL, it works on top of `database/sql`,
  download it with:

  ```bash
  go get github.com/vingarcia/ksql/adapters/kmysql
  ```
- `ksqlserver.New(ctx, os.Getenv("DATABASE_URL"), ksql.Config{})` for SQLServer, it works on top of `database/sql`,
  download it with:

  ```bash
  go get github.com/vingarcia/ksql/adapters/ksqlserver
  ```
- `ksqlite3.New(ctx, os.Getenv("DATBAASE_PATH"), ksql.Config{})` for SQLite3, it works on top of `database/sql`
  and [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) which relies on CGO, download it with:

  ```bash
  go get github.com/vingarcia/ksql/adapters/ksqlite3
  ```
- `ksqlite.New(ctx, os.Getenv("DATABASE_PATH"), ksql.Config{})` for SQLite, it works on top of `database/sql`
  and [modernc.org/sqlite](https://modernc.org/sqlite) which does not require CGO, download it with:

  ```bash
  go get github.com/vingarcia/ksql/adapters/modernc-ksqlite
  ```

For more detailed examples see:
- `./examples/all_adapters/all_adapters.go`

## The KSQL Interface

The current interface contains the methods the users are expected to use,
and it is also used for making it easy to mock the whole library if needed.

This interface is declared in the project as `ksql.Provider` and is displayed below.

We plan on keeping it very simple with a small number
of well-thought functions that cover all use cases,
so don't expect many additions:

```go
// Provider describes the KSQL public behavior
//
// The Insert, Patch, Delete and QueryOne functions return `ksql.ErrRecordNotFound`
// if no record was found or no rows were changed during the operation.
type Provider interface {
	Insert(ctx context.Context, table Table, record interface{}) error
	Patch(ctx context.Context, table Table, record interface{}) error
	Delete(ctx context.Context, table Table, idOrRecord interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error

	Exec(ctx context.Context, query string, params ...interface{}) (Result, error)
	Transaction(ctx context.Context, fn func(Provider) error) error
}
```

## Using KSQL

In the example below we'll cover all the most common use cases such as:

1. Inserting records
2. Updating records
3. Deleting records
4. Querying one or many records
5. Making transactions

More advanced use cases are illustrated on their own pages on [our Wiki](https://github.com/VinGarcia/ksql/wiki):

- [Querying in Chunks for Big Queries](https://github.com/VinGarcia/ksql/wiki/Querying-in-Chunks-for-Big-Queries)
- [Avoiding Code Duplication with the Select Builder](https://github.com/VinGarcia/ksql/wiki/Avoiding-Code-Duplication-with-the-Select-Builder)
- [Reusing Existing Structs on Queries with JOINs](https://github.com/VinGarcia/ksql/wiki/Reusing-Existing-Structs-on-Queries-with-JOINs)
- [Testing Tools and `ksql.Mock`](https://github.com/VinGarcia/ksql/wiki/Testing-Tools-and-ksql.Mock)

For the more common use cases please read the example below,
which is also available [here](./examples/crud/crud.go)
if you want to compile it yourself.

```Go
{{ readFile "examples/crud/crud.go" -}}
```

## Benchmark Comparison

The results of the benchmark are good for KSQL, but not flawless.

The next section summarizes the results so its more comprehensible,
but if you prefer to read the raw benchmark data just scroll down to the
[Benchmark Results](https://github.com/VinGarcia/ksql#benchmark-results) section.

### Summary

For transparency purposes this summary will focus
at the benchmark showing the _worst_ results for KSQL
which is querying multiple lines, this is the summary:

Comparing KSQL running on top of `database/sql` with `sqlx`, `sqlx` is
5% faster than KSQL, which is in practical terms an insignificant difference.
And if KSQL is running on top of `pgx` then KSQL becomes 42% faster
because `pgx` is significantly faster than `sqlx`.
Finally if you are using `sqlx` with prepared statements everytime
then `sqlx` is 7.5% faster than KSQL on top of `pgx`.

So between KSQL vs `sqlx` the performance difference is very small, and
if you are using Postgres odds are KSQL will be much faster.

Comparing KSQL running on top of `pgx` with `pgx` itself, KSQL
is 13.66% slower (on average), which is not insignificant but isn't much either.

Comparing KSQL running on top `pgx` with `gorm`, KSQL is
11.87% faster than `gorm` or inversely `gorm` is 13.4% slower.

> It is worth noting that KSQL is only caching of prepared statements
> when using postgres, because this is performed by `pgx`, and this
> means that when using MySQL, SQLServer or SQLite, if you plan
> on also using prepared statements other libaries such as `sqlx` will
> be significantly faster than KSQL.
>
> We are working on adding support for cached prepared statements for
> these other databases in the future.

### Benchmark Results

To understand the benchmark below you must know
that all tests are performed using Postgres 12.1 and
that we are comparing the following tools:

- KSQL using the adapter that wraps `database/sql`
- KSQL using the adapter that wraps `pgx`
- `database/sql`
- `sqlx`
- `pgx` (with `pgxpool`)
- `gorm`
- `sqlc`
- `sqlboiler`

For each of these tools, we are running 3 different queries:

The `insert-one` query looks like this:

`INSERT INTO users (name, age) VALUES ($1, $2) RETURNING id`

The `single-row` query looks like this:

`SELECT id, name, age FROM users OFFSET $1 LIMIT 1`

The `multiple-rows` query looks like this:

`SELECT id, name, age FROM users OFFSET $1 LIMIT 10`

Keep in mind that some of the tools tested (like GORM) actually build
the queries internally so the actual code used for the benchmark
might differ a little bit from the example ones above.

Without further ado, here are the results:

```bash
$ make bench TIME=5s
{{ readFile "benchmark.tmp" -}}
```

## Running the KSQL tests (for contributors)

The tests use `docker-test` for setting up all the supported databases,
which means that:

- You need to have `docker` installed
- You must be able to run docker without `sudo`, i.e.
  if you are not root you should add yourself to the docker group, e.g.:

  ```bash
  $ sudo usermod <your_username> -aG docker
  ```
  And then restart your login session (or just reboot)
- Finally run `make pre-download-all-images` only once so your tests don't
  timeout downloading the database images.

After that, you can just run the tests by using:

```bash
make test
```

## TODO List

- Add an `Upsert` helper method
- Try to implement an automatic prepared statements cache like pgx does.
- Update `ksqltest.FillStructWith` to work with `ksql:"..,json"` tagged attributes
- Improve error messages (ongoing)
- Finish the `kbuilder` package

## Optimization Opportunities

- Test if using a pointer on the field info is faster or not
- Consider passing the cached structInfo as an argument for all the functions that use it,
  so that we don't need to get it more than once in the same call.
- Use a cache to store often-used queries (like pgx)
- Preload the insert method for all dialects inside `ksql.NewTable()`
- Use prepared statements for the helper functions, `Update`, `Insert` and `Delete`.

## Features for a possible V2

- Change the `.Transaction(db ksql.Provider)` to a `.Transaction(ctx context.Context)`
- Make the `.Query()` method to return a `type Query interface { One(); All(); Chunks(); }`
- Have an `Update()` method that updates without ignoring NULLs as `Patch()` does
  - Have a new Modifier `skipNullUpdates` so that the Update function will do the job of the `Patch`
  - Remove the `Patch` function.
- Rename `NewTable()` to just `Table()` so it feels right to declare it inline when convenient
- Fix insert behavior where null slices are not skipped as null pointers are.
