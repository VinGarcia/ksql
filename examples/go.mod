module alladapters

go 1.14

replace (
	github.com/vingarcia/ksql => ../
	github.com/vingarcia/ksql/adapters/kmysql => ../adapters/kmysql
	github.com/vingarcia/ksql/adapters/kpgx => ../adapters/kpgx
	github.com/vingarcia/ksql/adapters/ksqlite3 => ../adapters/ksqlite3
	github.com/vingarcia/ksql/adapters/ksqlserver => ../adapters/ksqlserver
)

require (
	github.com/golang/mock v1.6.0
	github.com/stretchr/testify v1.8.0
	github.com/vingarcia/ksql v1.4.7
	github.com/vingarcia/ksql/adapters/kmysql v0.0.0-00010101000000-000000000000
	github.com/vingarcia/ksql/adapters/kpgx v0.0.0-00010101000000-000000000000
	github.com/vingarcia/ksql/adapters/ksqlite3 v0.0.0-00010101000000-000000000000
	github.com/vingarcia/ksql/adapters/ksqlserver v0.0.0-00010101000000-000000000000
)
