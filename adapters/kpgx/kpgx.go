package kpgx

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/vingarcia/ksql"

	// This is imported here so the user don't
	// have to worry about it when he uses it.
	_ "github.com/lib/pq"
)

// New instantiates a new ksql.Client using pgx as the backend driver
func New(
	ctx context.Context,
	connectionString string,
	config ksql.Config,
) (db ksql.DB, err error) {
	config.SetDefaultValues()

	pgxConf, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return ksql.DB{}, err
	}

	pgxConf.MaxConns = int32(config.MaxOpenConns)

	pool, err := pgxpool.ConnectConfig(ctx, pgxConf)
	if err != nil {
		return ksql.DB{}, err
	}
	if err = pool.Ping(ctx); err != nil {
		return ksql.DB{}, err
	}

	db, err = ksql.NewWithAdapter(NewPGXAdapter(pool), "postgres")
	return db, err
}
