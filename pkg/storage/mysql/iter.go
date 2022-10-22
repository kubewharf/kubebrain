package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"k8s.io/klog/v2"

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

	cmp := bytes.Compare(i.startKV.UserKey, i.endKV.UserKey)
	if cmp == 0 {
		start, end := i.startKV.Rev, i.endKV.Rev
		if start > end {
			start, end = end, start
		}
		db = db.Where("user_key = ?", i.startKV.UserKey).
			Where("rev >= ?", start).
			Where("rev <= ?", end).
			//Where("rev BETWEEN ? AND ?", start, end).
			Order("user_key desc, rev desc")
	} else if cmp > 0 {
		db = db.
			Where("user_key < ?", i.startKV.UserKey).
			Where("user_key > ?", i.endKV.UserKey).
			Or("user_key = ? AND rev <= ?", i.startKV.UserKey, i.startKV.Rev).
			Or("user_key = ? AND rev >= ?", i.endKV.UserKey, i.endKV.Rev).
			Order("user_key desc, rev desc")
	} else {
		db = db.
			Where("user_key < ?", i.endKV.UserKey).
			Where("user_key > ?", i.startKV.UserKey).
			Or("user_key = ? AND rev <= ?", i.endKV.UserKey, i.endKV.Rev).
			Or("user_key = ? AND rev >= ?", i.startKV.UserKey, i.startKV.Rev).
			Order("user_key, rev")
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
		kv := &KV{}
		err = i.db.ScanRows(i.rows, kv)
		if err != nil {
			return errors.Wrap(err, "failed to Scan")
		}
		klog.InfoS("cur", "key", string(kv.UserKey), "val", string(kv.Val), "rev", kv.Rev)
		i.current = kv
		return nil
	}
	return io.EOF
}

func (i *iter) Close() error {
	return errors.Wrap(i.rows.Close(), "close iter")
}
