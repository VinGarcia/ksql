package kissorm

import (
	"context"
)

// ORMProvider describes the public behavior of this ORM
type ORMProvider interface {
	Find(ctx context.Context, item interface{}, query string, params ...interface{}) error
	Insert(ctx context.Context, items ...interface{}) error
	Delete(ctx context.Context, ids ...interface{}) error
	Update(ctx context.Context, items ...interface{}) error
	Query(ctx context.Context, query string, params ...interface{}) (Iterator, error)
	QueryNext(ctx context.Context, rawIt Iterator, item interface{}) (done bool, err error)
}

// Iterator ...
type Iterator interface {
	Close() error
}
