package kmysql

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
	"github.com/vingarcia/ksql/sqldialect"
)

func TestAdapter(t *testing.T) {
	mysqlURL, closeMySQL := startMySQLDB("ksql")
	defer closeMySQL()

	ksql.RunTestsForAdapter(t, "kmysql", sqldialect.MysqlDialect{}, mysqlURL, func(t *testing.T) (ksql.DBAdapter, io.Closer) {
		db, err := sql.Open("mysql", mysqlURL)
		if err != nil {
			t.Fatal(err.Error())
		}
		return SQLAdapter{db}, db
	})
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
			Repository: "mariadb",
			Tag:        "10.8",
			Env: []string{
				"MARIADB_ROOT_PASSWORD=mysql",
				"MARIADB_DATABASE=" + dbName,
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
	databaseUrl := fmt.Sprintf("root:mysql@(%s)/%s?timeout=30s&parseTime=true", hostAndPort, dbName)

	fmt.Println("Connecting to mariadb on url: ", databaseUrl)

	resource.Expire(40) // Tell docker to hard kill the container in 40 seconds

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
