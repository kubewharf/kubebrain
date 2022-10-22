package mysql

import (
	"bytes"
	"context"

	"gorm.io/gorm"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type batch struct {
	err error
	db  *gorm.DB
	fs  []func()
}

func (b *batch) error() error {
	if b.err != nil {
		return b.err
	}
	if b.db.Error != nil {
		return b.db.Error
	}
	return nil
}

func (b *batch) PutIfNotExist(key []byte, val []byte, ttl int64) {
	idx := len(b.fs)
	f := func() {
		kv, _ := decode(key)
		b.db.First(kv)

		if b.error() != nil {
			return
		}

		if len(kv.Val) != 0 {
			b.err = storage.NewErrConflict(idx, key, kv.Val)
			return
		}
		kv, _ = decode(key)
		kv.Val = val
		b.db.Create(kv)
	}

	b.fs = append(b.fs, f)
}

func (b *batch) CAS(key []byte, newVal []byte, oldVal []byte, ttl int64) {
	idx := len(b.fs)
	f := func() {
		kv, _ := decode(key)
		b.db.Where("rev = ?", kv.Rev).First(kv)

		if b.error() != nil {
			return
		}
		klog.InfoS("cur", "key", string(kv.UserKey), "val", string(kv.Val), "rev", kv.Rev)
		if bytes.Compare(kv.Val, oldVal) != 0 {
			b.err = storage.NewErrConflict(idx, key, kv.Val)
			return
		}

		kv, _ = decode(key)
		kv.Val = newVal
		b.db.Where("rev = ?", kv.Rev).Updates(kv)
	}
	b.fs = append(b.fs, f)
}

func (b *batch) rollbackIfErr() {
	if b.error() != nil {
		klog.InfoS("batch error", "err", b.error())
		b.db.Rollback()
	}
}

func (b *batch) Put(key []byte, val []byte, ttl int64) {
	f := func() {
		kv, _ := decode(key)
		kv.Val = val
		klog.InfoS("put", "key", string(kv.UserKey), "val", string(kv.Val), "rev", kv.Rev)
		b.db.Create(kv)
	}

	b.fs = append(b.fs, f)
}

func (b *batch) Del(key []byte) {
	f := func() {
		kv, _ := decode(key)
		b.db.Delete(kv)
	}

	b.fs = append(b.fs, f)
}

func (b *batch) DelCurrent(it storage.Iter) {
	f := func() {
		kv, _ := decode(it.Key())
		b.db.Where("val = ?", it.Val()).Delete(kv)
	}

	b.fs = append(b.fs, f)
}

func (b *batch) Commit(ctx context.Context) error {
	b.db = b.db.WithContext(ctx).Begin()
	klog.Infof("batch %p txn start", b)
	defer func() {
		klog.Infof("batch %p txn end", b)
	}()
	for _, f := range b.fs {
		f()

		if b.error() != nil {
			b.db.Rollback()
			return b.error()
		}
	}

	b.db.Commit()

	return b.error()
}
