package ksql

import (
	"context"
	"fmt"
)

type placeholderAdapter struct {
	Provider

	dialect Dialect
}

func newPlaceholderAdapter(db Provider, dialect Dialect) placeholderAdapter {
	return placeholderAdapter{
		Provider: db,
		dialect:  dialect,
	}
}

func (p placeholderAdapter) Query(ctx context.Context, records interface{}, queryFormat string, params ...interface{}) error {
	query := fmt.Sprintf(queryFormat, buildPlaceholderList(p.dialect, len(params))...)
	return p.Provider.Query(ctx, records, query, params...)
}

func (p placeholderAdapter) QueryOne(ctx context.Context, record interface{}, queryFormat string, params ...interface{}) error {
	query := fmt.Sprintf(queryFormat, buildPlaceholderList(p.dialect, len(params))...)
	return p.Provider.QueryOne(ctx, record, query, params...)
}

func (p placeholderAdapter) QueryChunks(ctx context.Context, parser ChunkParser) error {
	parser.Query = fmt.Sprintf(parser.Query, buildPlaceholderList(p.dialect, len(parser.Params))...)
	return p.Provider.QueryChunks(ctx, parser)
}

func (p placeholderAdapter) Exec(ctx context.Context, queryFormat string, params ...interface{}) error {
	query := fmt.Sprintf(queryFormat, buildPlaceholderList(p.dialect, len(params))...)
	return p.Provider.Exec(ctx, query, params...)
}

func (p placeholderAdapter) Transaction(ctx context.Context, fn func(Provider) error) error {
	return p.Provider.Transaction(ctx, func(db Provider) error {
		db = newPlaceholderAdapter(db, p.dialect)
		return fn(db)
	})
}

func buildPlaceholderList(dialect Dialect, numElements int) []interface{} {
	placeholders := []interface{}{}
	for i := 0; i < numElements; i++ {
		placeholders = append(placeholders, dialect.Placeholder(i))
	}

	return placeholders
}
