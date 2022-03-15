module alladapters

go 1.14

replace (
	github.com/vingarcia/ksql => ../
	github.com/vingarcia/ksql/adapters/kmysql => ../adapters/kmysql
	github.com/vingarcia/ksql/adapters/kpgx => ../adapters/kpgx
	github.com/vingarcia/ksql/adapters/ksqlite3 => ../adapters/ksqlite3
	github.com/vingarcia/ksql/adapters/ksqlserver => ../adapters/ksqlserver
)
