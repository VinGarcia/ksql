module github.com/vingarcia/ksql/adapters/kpgx

go 1.14

require (
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/jackc/pgconn v1.10.0
	github.com/jackc/pgx/v4 v4.13.0
	github.com/lib/pq v1.10.4 // indirect
	github.com/ory/dockertest v3.3.5+incompatible // indirect
	github.com/vingarcia/ksql v0.0.0-00010101000000-000000000000
)

replace github.com/vingarcia/ksql => ../../
