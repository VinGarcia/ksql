package ksql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	dockertest "github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

var connectionString = map[string]string{
	"postgres":  "host=localhost port=5432 user=postgres password=postgres dbname=ksql sslmode=disable",
	"sqlite3":   "/tmp/ksql.db",
	"mysql":     "root:mysql@(127.0.0.1:3306)/ksql?timeout=30s",
	"sqlserver": "sqlserver://sa:Sqls3rv3r@127.0.0.1:1433?databaseName=ksql",
}

func TestMain(m *testing.M) {
	postgresURL, closePostgres := startPostgresDB("ksql")
	connectionString["postgres"] = postgresURL

	exitCode := m.Run()

	// We need to close it without defer because
	//of the os.Exit call below:
	closePostgres()

	os.Exit(exitCode)
}

func startPostgresDB(dbName string) (databaseURL string, closer func()) {
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
	databaseUrl := fmt.Sprintf("postgres://postgres:postgres@%s/"+dbName+"?sslmode=disable", hostAndPort)

	fmt.Println("Connecting to postgres on url: ", databaseUrl)

	resource.Expire(40) // Tell docker to hard kill the container in 20 seconds

	var sqlDB *sql.DB
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 10 * time.Second
	pool.Retry(func() error {
		sqlDB, err = sql.Open("postgres", databaseUrl)
		if err != nil {
			return err
		}
		return sqlDB.Ping()
	})
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	return databaseUrl, func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
}

func toJSON(i interface{}) []byte {
	rawJSON, err := json.Marshal(i)
	if err != nil {
		panic(fmt.Sprintf("error marshalling %v during test", i))
	}
	return rawJSON
}
