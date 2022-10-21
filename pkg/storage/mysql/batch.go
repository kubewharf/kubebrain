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
	f := func() {
		kv := &KV{
			InternalKey: key,
		}
		b.db.First(kv)

		if b.error() != nil {
			return
		}

		if len(kv.Val) != 0 {
			b.err = storage.ErrKeyDuplicated
		}
		kv.InternalKey = key
		kv.Val = val
		b.db.Create(kv)
	}

	b.fs = append(b.fs, f)
}

func (b *batch) CAS(key []byte, newVal []byte, oldVal []byte, ttl int64) {
	f := func() {
		kv := &KV{}
		b.db.First(kv, key)

		if b.error() != nil {
			return
		}

		if bytes.Compare(kv.Val, oldVal) != 0 {
			b.err = storage.ErrCASFailed
		}
		kv.InternalKey = key
		kv.Val = newVal
		b.db.Updates(kv)
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
		kv := &KV{}
		kv.InternalKey = key
		kv.Val = val
		b.db.Create(kv)
	}

	b.fs = append(b.fs, f)
}

func (b *batch) Del(key []byte) {
	f := func() {
		b.db.Delete(&KV{InternalKey: key})
	}

	b.fs = append(b.fs, f)
}

func (b *batch) DelCurrent(it storage.Iter) {
	f := func() {
		b.db.Where("val = ?", it.Val()).Delete(&KV{InternalKey: it.Key()})
	}

	b.fs = append(b.fs, f)
}

func (b *batch) Commit(ctx context.Context) error {
	b.db = b.db.WithContext(ctx)
	for _, f := range b.fs {

		f()

		if b.error() != nil {
			b.db.Rollback()
			return b.error()
		}
	}

	return nil
}
