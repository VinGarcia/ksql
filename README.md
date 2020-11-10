
# KissORM

Welcome to the KissORM project, o the Keep It Simple Stupid micro ORM.

This ORM was created to be used by any developer efficiently and safely.
The goals were:

- We'll not let you shoot your own foot
- Testing should be simple and clear for readers
- The learning curve should be minimal, so we are aiming on a very slim interface

> Note: Currently we use GORM internally because we needed a working
> version asap in order to use it on our next project, however, this is
> temporary and we are already removing this dependency.

### Kiss Interface

The current interface is as follows:

```go
// ORMProvider describes the public behavior of this ORM
type ORMProvider interface {
	Insert(ctx context.Context, records ...interface{}) error
	Delete(ctx context.Context, ids ...interface{}) error
	Update(ctx context.Context, records ...interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error
}
```

You might notice that we are currently lacking an `Exec()`
function and a abstraction for transactions, but it is on our TODO list.
