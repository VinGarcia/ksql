package gpostgres

import (
	"context"

	"github.com/jinzhu/gorm"
)

// Client ...
type Client struct {
	tableName string
	db        *gorm.DB
}

// NewClient ...
func NewClient(tableName string) Client {
	return Client{
		tableName: tableName,
	}
}

// Find one instance from the database, the input struct
// must be passed by reference and the query should
// return only one result.
func (c Client) Find(
	ctx context.Context,
	item interface{},
	query string,
) error {
	it := c.db.Raw(query)
	it.Scan(item)
	return it.Error
}

// Insert one or more instances on the database
//
// If the original instances have been passed by reference
// the ID is automatically updated after insertion is completed.
func (c Client) Insert(
	ctx context.Context,
	items ...interface{},
) error {
	if len(items) == 0 {
		return nil
	}

	for _, item := range items {
		r := c.db.Table(c.tableName).Create(item)
		if r.Error != nil {
			return r.Error
		}
	}

	return nil
}

// Delete deletes one or more instances from the database by id
func (c Client) Delete(
	ctx context.Context,
	entities ...interface{},
) error {
	for _, entity := range entities {
		r := c.db.Table(c.tableName).Delete(entity)
		if r.Error != nil {
			return r.Error
		}
	}

	return nil
}
