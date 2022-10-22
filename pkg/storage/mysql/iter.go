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

	start   []byte
	end     []byte
	startKV *KV
	endKV   *KV
	limit   int
}

func newIter(db *gorm.DB, start []byte, end []byte, limit int) *iter {

	startKV, _ := decode(start)
	endKV, _ := decode(end)

	return &iter{
		db:      db,
		current: &KV{},
		startKV: startKV,
		endKV:   endKV,
		start:   start,
		end:     end,
		limit:   limit,
	}
}

func (i *iter) init(ctx context.Context) (err error) {
	db := i.db.WithContext(ctx).
		Model(&KV{})

	if bytes.Compare(i.start, i.end) > 0 {
		db = db.
			Where("key <= ?", i.startKV.Key).
			Where("rev <= ?", i.startKV.Rev).
			Where("key >= ?", i.endKV.Key).
			Where("rev >= ?", i.endKV.Rev).
			Order("key desc, rev desc")
	} else {
		db = db.
			Where("key <= ?", i.endKV.Key).
			Where("rev <= ?", i.endKV.Rev).
			Where("key >= ?", i.startKV.Key).
			Where("rev >= ?", i.startKV.Rev).
			Order("key, rev")
	}

	if i.limit != 0 {
		db = db.Limit(i.limit)
	}
	i.rows, err = db.Rows()
	return err
}

func (i *iter) Key() []byte {
	return encode(i.current)
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
