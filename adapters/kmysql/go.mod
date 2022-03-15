module github.com/vingarcia/ksql/adapters/kmysql

go 1.14

require (
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/go-sql-driver/mysql v1.6.0
	github.com/ory/dockertest v3.3.5+incompatible // indirect
	github.com/vingarcia/ksql v0.0.0-00010101000000-000000000000
)

replace github.com/vingarcia/ksql => ../../
