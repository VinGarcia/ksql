package kpgx

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/sqldialect"
)

func TestAdapter(t *testing.T) {
	ctx := context.Background()

	postgresURL, closePostgres := startPostgresDB(ctx, "ksql")
	defer closePostgres()

	ksql.RunTestsForAdapter(t, "kpgx5", sqldialect.PostgresDialect{}, postgresURL, func(t *testing.T) (ksql.DBAdapter, io.Closer) {
		pool, err := pgxpool.New(ctx, postgresURL)
		if err != nil {
			t.Fatal(err.Error())
		}
		return PGXAdapter{pool}, closerAdapter{close: pool.Close}
	})
}

type closerAdapter struct {
	close func()
}

func (c closerAdapter) Close() error {
	c.close()
	return nil
}

func startPostgresDB(ctx context.Context, dbName string) (databaseURL string, closer func()) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "postgres",
			Tag:        "14.0",
			Env: []string{
				"POSTGRES_PASSWORD=postgres",
				"POSTGRES_USER=postgres",
				"POSTGRES_DB=" + dbName,
				"listen_addresses = '*'",
			},
		},
		func(config *docker.HostConfig) {
			// set AutoRemove to true so that stopped container goes away by itself
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		},
	)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	hostAndPort := resource.GetHostPort("5432/tcp")
	databaseUrl := fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", hostAndPort, dbName)

	fmt.Println("Connecting to postgres on url: ", databaseUrl)

	resource.Expire(40) // Tell docker to hard kill the container in 40 seconds

	var sqlDB *pgxpool.Pool
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 10 * time.Second
	pool.Retry(func() error {
		sqlDB, err = pgxpool.New(ctx, databaseUrl)
		if err != nil {
			return err
		}

		return sqlDB.Ping(ctx)
	})
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	sqlDB.Close()

	return databaseUrl, func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
}
