module github.com/vingarcia/ksql/benchmarks

go 1.22

toolchain go1.22.1

require (
	github.com/friendsofgo/errors v0.9.2
	github.com/jackc/pgx/v4 v4.18.1
	github.com/jmoiron/sqlx v1.3.4
	github.com/lib/pq v1.10.9
	github.com/vingarcia/ksql v1.13.0
	github.com/vingarcia/ksql/adapters/kpgx v0.0.0-00010101000000-000000000000
	github.com/volatiletech/sqlboiler/v4 v4.12.0
	github.com/volatiletech/strmangle v0.0.4
	gorm.io/driver/postgres v1.2.2
	gorm.io/gorm v1.22.3
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.2 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgtype v1.14.0 // indirect
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.2 // indirect
	github.com/mattn/go-sqlite3 v1.14.12 // indirect
	github.com/ory/dockertest/v3 v3.11.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	github.com/volatiletech/inflect v0.0.1 // indirect
	golang.org/x/crypto v0.27.0 // indirect
	golang.org/x/text v0.18.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/vingarcia/ksql => ../
	github.com/vingarcia/ksql/adapters/kpgx => ../adapters/kpgx/
)
