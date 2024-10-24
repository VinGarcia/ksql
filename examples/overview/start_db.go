package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/vingarcia/ksql"
)

func createTablesAndRecords(ctx context.Context, db ksql.DB) error {
	return errors.Join(
		exec(ctx, db,
			`CREATE TABLE users (
				user_id SERIAL PRIMARY KEY,
				name VARCHAR NOT NULL,
				type VARCHAR NOT NULL DEFAULT 'user'
			)`,
		),
		exec(ctx, db,
			`CREATE TABLE posts (
				post_id SERIAL PRIMARY KEY,
				user_id INT NOT NULL REFERENCES users(user_id),
				title VARCHAR NOT NULL,
				text VARCHAR NOT NULL
			)`,
		),
		exec(ctx, db,
			`CREATE TABLE addresses (
				id SERIAL PRIMARY KEY,
				user_id INT NOT NULL REFERENCES users(user_id),
				full_addr VARCHAR NOT NULL
			)`,
		),
		db.Insert(ctx, UsersTable, &User{
			ID:   42,
			Name: "Julia",
			Type: "admin",
		}),
		db.Insert(ctx, UsersTable, &User{
			ID:   43,
			Name: "June",
			Type: "user",
		}),
		db.Insert(ctx, UsersTable, &User{
			ID:   44,
			Name: "Bob",
			Type: "user",
		}),
		db.Insert(ctx, PostsTable, &Post{
			Title:  "Title1",
			Text:   "How to create foo",
			UserID: 42,
		}),
		db.Insert(ctx, PostsTable, &Post{
			Title:  "Title2",
			Text:   "How to create foo",
			UserID: 42,
		}),
		db.Insert(ctx, PostsTable, &Post{
			Title:  "Title3",
			Text:   "How to create foo",
			UserID: 43,
		}),
		db.Insert(ctx, AddressesTable, &Address{
			UserID:   42,
			FullAddr: "Jorge das Flores St. 46",
		}),
		db.Insert(ctx, AddressesTable, &Address{
			UserID:   43,
			FullAddr: "Maria das Flores St. 47",
		}),
	)
}

func exec(ctx context.Context, db ksql.DB, query string, params ...interface{}) error {
	_, err := db.Exec(ctx, query, params...)
	return err
}

func startPostgresDB(ctx context.Context) (databaseURL string, closer func()) {
	dbName := "intro_db"

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	fmt.Println("the first time this example runs it will download postgres:14.0, which may take a long time")
	fmt.Println("if you want to see the download progress stop this program and run `docker pull postgres:14.0` instead")
	fmt.Println()

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
