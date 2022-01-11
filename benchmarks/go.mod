module github.com/vingarcia/ksql/benchmarks

go 1.16

require (
	github.com/jackc/pgx/v4 v4.13.0
	github.com/jmoiron/sqlx v1.3.4
	github.com/lib/pq v1.10.4
	github.com/vingarcia/ksql v1.1.0
	gorm.io/driver/postgres v1.2.2
	gorm.io/gorm v1.22.3
)

replace github.com/vingarcia/ksql => ../
