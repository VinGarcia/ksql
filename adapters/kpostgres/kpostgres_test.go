package kpostgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/sqldialect"
)

func TestSQLAdapter(t *testing.T) {
	ctx := context.Background()

	postgresURL, closePostgres := startPostgresDB(ctx, "ksql")
	defer closePostgres()

	ksql.RunTestsForAdapter(t, "kpostgres", sqldialect.PostgresDialect{}, postgresURL, func(t *testing.T) (ksql.DBAdapter, io.Closer) {
		sqldb, err := sql.Open("postgres", postgresURL)
		if err != nil {
			t.Fatal(err.Error())
		}

		return SQLAdapter{sqldb}, sqldb
	})
}

func startPostgresDB(ctx context.Context, dbName string) (databaseURL string, closer func()) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := dockerPool.RunWithOptions(
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

	var sqlDB *sql.DB
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	dockerPool.MaxWait = 10 * time.Second
	dockerPool.Retry(func() error {
		sqlDB, err = sql.Open("postgres", databaseUrl)
		if err != nil {
			return err
		}

		return sqlDB.Ping()
	})
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	sqlDB.Close()

	return databaseUrl, func() {
		if err := dockerPool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
}
