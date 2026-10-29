# Welcome to the Keep It Simple Query Builder

This is the Keep It Simple query builder created to work
either in conjunction or separated from the KSQL package.

## Warning: Experimental package

kbuilder is new and might be subject to breaking changes, so if you want
to experiment with this tool know that updating it might require manual changes
to the code

## Usage

### Select Queries

One of the main use-cases for which kbuilder was designed was to facilitate
writing complex queries with many optional filters, the below example illustrates it:

> [Click here to see the full example file](https://github.com/VinGarcia/ksql/blob/master/kbuilder/examples/query/main.go)

```go
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
		Offset:  filter.Page * filter.PageSize,
		Limit:   filter.PageSize,

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
```

## TODO List

- Add support to Update and Delete operations
- Improve support to JOINs by adding the `tablename` tag to the structs
- Add error check for when the Select, Insert and Update attrs are all empty
