package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"io"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type iter struct {
	db      *gorm.DB
	rows    *sql.Rows
	current *KV

	start []byte
	end   []byte
	limit int
}

func newIter(db *gorm.DB, start []byte, end []byte, limit int) *iter {
	return &iter{
		db:      db,
		current: &KV{},
		start:   start,
		end:     end,
		limit:   limit,
	}
}

func (i *iter) init(ctx context.Context) (err error) {
	db := i.db.WithContext(ctx).
		Model(&KV{}).
		Where("internal_key <= ?", i.start).Where("internal_key >= ?", i.end)

	if i.limit != 0 {
		db = db.Limit(i.limit)
	}
	if bytes.Compare(i.start, i.end) < 0 {
		db = db.Order("internal_key")
	} else {
		db = db.Order("internal_key desc")
	}
	i.rows, err = db.Rows()
	return err
}

func (i *iter) Key() []byte {
	return i.current.InternalKey
}

func (i *iter) Val() []byte {
	return i.current.Val
}

func (i *iter) Next(ctx context.Context) (err error) {
	if i.rows.Next() {
		return nil
	}
	return io.EOF
}

func (i *iter) Close() error {
	return errors.Wrap(i.rows.Close(), "close iter")
}
