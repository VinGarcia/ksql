package ksqlserver

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	"github.com/vingarcia/ksql"
)

func TestAdapter(t *testing.T) {
	sqlServerURL, closeSQLServer := startSQLServerDB("ksql")
	defer closeSQLServer()

	ksql.RunTestsForAdapter(t, "ksqlserver", "sqlserver", sqlServerURL, func(t *testing.T) (ksql.DBAdapter, io.Closer) {
		db, err := sql.Open("sqlserver", sqlServerURL)
		if err != nil {
			t.Fatal(err.Error())
		}
		return SQLAdapter{db}, db
	})
}

func startSQLServerDB(dbName string) (databaseURL string, closer func()) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "mcr.microsoft.com/mssql/server",
			Tag:        "2017-latest",
			Env: []string{
				"SA_PASSWORD=Sqls3rv3r",
				"ACCEPT_EULA=Y",
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

	hostAndPort := resource.GetHostPort("1433/tcp")
	databaseUrl := fmt.Sprintf("sqlserver://sa:Sqls3rv3r@%s?databaseName=%s", hostAndPort, dbName)

	fmt.Println("Connecting to sqlserver on url: ", databaseUrl)

	resource.Expire(40) // Tell docker to hard kill the container in 40 seconds

	var sqlDB *sql.DB
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 10 * time.Second
	pool.Retry(func() error {
		sqlDB, err = sql.Open("sqlserver", databaseUrl)
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
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
}
