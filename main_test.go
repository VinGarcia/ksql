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

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var connectionString map[string]string

func TestMain(m *testing.M) {
	postgresURL, closePostgres := startPostgresDB("ksql")
	mysqlURL, closeMySQL := startMySQLDB("ksql")
	sqlServerURL, closeSQLServer := startSQLServerDB("ksql")

	connectionString = map[string]string{
		"postgres":  postgresURL,
		"sqlite3":   "/tmp/ksql.db",
		"mysql":     mysqlURL,
		"sqlserver": sqlServerURL,
	}

	exitCode := m.Run()

	// We need to close it without defer because
	// of the os.Exit call below:
	closePostgres()
	closeMySQL()
	closeSQLServer()

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
	databaseUrl := fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", hostAndPort, dbName)

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
	sqlDB.Close()

	return databaseUrl, func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
}

func startMySQLDB(dbName string) (databaseURL string, closer func()) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "mysql",
			Tag:        "8.0.27",
			Env: []string{
				"MYSQL_ROOT_PASSWORD=mysql",
				"MYSQL_DATABASE=" + dbName,
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

	hostAndPort := resource.GetHostPort("3306/tcp")
	databaseUrl := fmt.Sprintf("root:mysql@(%s)/%s?timeout=30s", hostAndPort, dbName)

	fmt.Println("Connecting to mysql on url: ", databaseUrl)

	resource.Expire(40) // Tell docker to hard kill the container in 20 seconds

	var sqlDB *sql.DB
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 10 * time.Second
	pool.Retry(func() error {
		sqlDB, err = sql.Open("mysql", databaseUrl)
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

	resource.Expire(40) // Tell docker to hard kill the container in 20 seconds

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

func toJSON(i interface{}) []byte {
	rawJSON, err := json.Marshal(i)
	if err != nil {
		panic(fmt.Sprintf("error marshalling %v during test", i))
	}
	return rawJSON
}
