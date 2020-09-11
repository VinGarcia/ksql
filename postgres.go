package gpostgres

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jinzhu/gorm"
)

type metaCache struct {
	TableName string
}

type TableNamer interface {
	TableName() string
}

type Client struct {
	db       *gorm.DB
	metadata map[reflect.Type]*metaCache
}

func NewClient() Client {
	return Client{
		metadata: map[reflect.Type]*metaCache{},
	}
}

func (c Client) Get(
	ctx context.Context,
	item interface{},
	query string,
) error {
	it := c.db.Raw(query)
	it.Scan(item)
	return it.Error
}

func (c Client) Insert(
	ctx context.Context,
	items ...interface{},
) error {
	if len(items) == 0 {
		return nil
	}

	m, err := c.getMetadata(items[0])
	if err != nil {
		return err
	}

	vItems := reflect.ValueOf(items)
	for i := 0; i < vItems.Len(); i++ {
		v := vItems.Index(i).Elem()
		if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("entity type must be a pointer to struct, not: %T", v.Interface())
		}

		r := c.db.Table(m.TableName).Create(v.Interface())
		if r.Error != nil {
			return r.Error
		}
	}

	return nil
}

func (c *Client) getMetadata(entity interface{}) (*metaCache, error) {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("entity type must be a struct or a struct pointer, not: %T", entity)
	}

	metadata, found := c.metadata[t]
	if found {
		return metadata, nil
	}

	metadata = &metaCache{}

	if tNamer, ok := entity.(TableNamer); ok {
		metadata.TableName = tNamer.TableName()
	} else {
		n := t.Name()
		if n[len(n)-1] != 's' {
			n += "s"
		}

		metadata.TableName = strings.ToLower(n)
	}

	return metadata, nil
}
