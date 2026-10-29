package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/ksqlite3"
	"github.com/vingarcia/ksql/kbuilder"
)

var UsersTable = ksql.NewTable("users")

type User struct {
	ID   int    `ksql:"id"`
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`
}

var PostsTable = ksql.NewTable("posts")

type Post struct {
	ID      int    `ksql:"id"`
	UserID  int    `ksql:"user_id"`
	Content string `ksql:"content"`
}

func main() {
	ctx := context.Background()

	db := setupDB(ctx)

	user1 := User{Name: "Felipe", Age: 42}
	user2 := User{Name: "Rebeca", Age: 42}
	user3 := User{Name: "Lucas", Age: 42}

	err := errors.Join(
		db.Insert(ctx, UsersTable, &user1),
		db.Insert(ctx, PostsTable, &Post{UserID: user1.ID, Content: "Post1"}),
		db.Insert(ctx, PostsTable, &Post{UserID: user1.ID, Content: "Post2"}),
		db.Insert(ctx, UsersTable, &user2),
		db.Insert(ctx, PostsTable, &Post{UserID: user2.ID, Content: "Post1"}),
		db.Insert(ctx, PostsTable, &Post{UserID: user2.ID, Content: "Post2"}),
		db.Insert(ctx, PostsTable, &Post{UserID: user2.ID, Content: "Post3"}),
		db.Insert(ctx, UsersTable, &user3),
		db.Insert(ctx, PostsTable, &Post{UserID: user3.ID, Content: "Post1"}),
	)
	if err != nil {
		log.Fatalf("error setting up db initial values: %s", err)
	}

	posts := ListPosts(ctx, db, user2.ID, ListPostsFilter{
		ContainsSubstring: "Post3",
	})
	printAsJSON("Posts from user2 containing 'Post3'", posts)

	fmt.Println()

	posts = ListPosts(ctx, db, user1.ID, ListPostsFilter{
		Page:     1,
		PageSize: 1, // A very small page just for showcasing the feature
	})
	printAsJSON("The second page of posts of user 1", posts)
}

// Note: All filter attributes are ignored if left unset
type ListPostsFilter struct {
	UserAgeRange      AgeRange
	ContainsSubstring string
	Page              int
	PageSize          int
	OrderBy           string
	SortOrder         string // DESC | ASC
}

type AgeRange struct {
	Min int
	Max int
}

func ListPosts(ctx context.Context, db ksql.Provider, userID int, filter ListPostsFilter) (posts []Post) {
	queryBuilder := kbuilder.Query{
		Select: "p.*",
		From:   "users u JOIN posts p ON u.id = p.user_id",
		// Here we can create expressive conditions for filtering the query:
		Where: kbuilder.
			// A mandatory filter can use `.Where(query, args...)`:
			Where("u.id = %s", userID).
			// Optional filters can use `WhereIf(cond, query, args...)`
			WhereIf(filter.ContainsSubstring != "", "p.content LIKE %s", "%"+filter.ContainsSubstring+"%").
			// Each condition supports multiple input arguments as shown below:
			WhereIf(
				filter.UserAgeRange.Min > 0 && filter.UserAgeRange.Max > 0,
				"u.age > %s AND u.age < %s",
				filter.UserAgeRange.Min, filter.UserAgeRange.Max,
			),
		// Both Offset and Limit are ignored and not added to the query if passed as zero, so it's safe to use it like this:
		Offset: filter.Page * filter.PageSize,
		Limit:  filter.PageSize,

		// OrderBy is ignored if it contains only white spaces so this is also safe:
		OrderBy: filter.OrderBy + " " + filter.SortOrder,
	}

	/**
	 * Note:
	 *
	 * Below we are building the query explicitly, so we can print the output query,
	 * but normally I recommend using the simpler API:
	 *
	 * `err := db.QueryFromBuilder(ctx, &posts, queryBuilder)`
	 */

	query, params, err := queryBuilder.Build("sqlite3")
	if err != nil {
		log.Fatalf("error building query: %s", err)
	}

	fmt.Println("query:", query)
	printAsJSON("params:", params)

	err = db.Query(ctx, &posts, query, params...)
	if err != nil {
		log.Fatalf("error querying posts: %s", err)
	}

	return posts
}

func setupDB(ctx context.Context) ksql.Provider {
	// An in-memory database makes the example self-contained: a file-backed
	// one would accumulate the inserted rows on every run.
	db, err := ksqlite3.New(ctx, ":memory:", ksql.Config{
		MaxOpenConns: 1,
	})
	if err != nil {
		log.Fatalf("unable to open database: %s", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			age INTEGER
		)`)
	if err != nil {
		log.Fatalf("unable to create users table: %s", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS posts (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			content TEXT
		)`)
	if err != nil {
		log.Fatalf("unable to create posts table: %s", err)
	}

	return db
}

func printAsJSON(desc string, obj any) {
	b, _ := json.MarshalIndent(obj, "", "  ")
	fmt.Println(desc, string(b))
}
