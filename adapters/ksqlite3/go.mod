module github.com/vingarcia/ksql/adapters/ksqlite3

go 1.14

require (
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/mattn/go-sqlite3 v1.14.12
	github.com/ory/dockertest v3.3.5+incompatible // indirect
	github.com/vingarcia/ksql v0.0.0-00010101000000-000000000000
)

replace github.com/vingarcia/ksql => ../../
