module github.com/vingarcia/ksql/adapters/ksqlserver

go 1.14

require (
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/denisenkom/go-mssqldb v0.10.0
	github.com/gotestyourself/gotestyourself v2.2.0+incompatible // indirect
	github.com/ory/dockertest v3.3.5+incompatible
	github.com/vingarcia/ksql v0.0.0-00010101000000-000000000000
	gotest.tools v2.2.0+incompatible // indirect
)

replace github.com/vingarcia/ksql => ../../
