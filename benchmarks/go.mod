module github.com/vingarcia/ksql/benchmarks

go 1.16

require (
	github.com/jmoiron/sqlx v1.3.4
	github.com/lib/pq v1.10.4
	github.com/vingarcia/ksql v1.1.0
	gorm.io/driver/postgres v1.2.2 // indirect
	gorm.io/gorm v1.22.3 // indirect
)

replace github.com/vingarcia/ksql => ../
