package mysql

import (
	"context"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	//idx := len(b.fs)
	f := func() {
		//kv, _ := decode(key)
		//b.db.First(kv)
		//
		//if b.error() != nil {
		//	return
		//}
		//
		//if len(kv.Val) != 0 {
		//	b.err = storage.NewErrConflict(idx, key, kv.Val)
		//	return
		//}
		kv, _ := decode(key)
		kv.Val = val
		b.db = b.db.Create(kv)
		if b.db.RowsAffected == 0 {
			klog.InfoS("conflict", "key", string(key))
			b.err = storage.ErrCASFailed
			//b.err = storage.NewErrConflict(idx, key, kv.Val)
		}
	}

	b.fs = append(b.fs, f)
}

func (b *batch) CAS(key []byte, newVal []byte, oldVal []byte, ttl int64) {
	//idx := len(b.fs)
	f := func() {
		//kv, _ := decode(key)
		//b.db.Where("rev = ?", kv.Rev).First(kv)
		//
		//if b.error() != nil {
		//	return
		//}
		//klog.InfoS("cur", "key", string(kv.UserKey), "val", string(kv.Val), "rev", kv.Rev)
		//if bytes.Compare(kv.Val, oldVal) != 0 {
		//	b.err = storage.NewErrConflict(idx, key, kv.Val)
		//	return
		//}

		kv, _ := decode(key)
		kv.Val = newVal
		b.db = b.db.Where("rev = ?", kv.Rev).Where("val = ?", oldVal).Updates(kv)
		if b.db.RowsAffected == 0 {
			b.err = storage.ErrCASFailed
			//b.err = storage.NewErrConflict(idx, key, kv.Val)
			return
		}
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
	//idx := len(b.fs)
	f := func() {
		kv, _ := decode(key)
		kv.Val = val
		klog.InfoS("put", "key", string(kv.UserKey), "val", string(kv.Val), "rev", kv.Rev)
		b.db = b.db.Clauses(clause.OnConflict{
			UpdateAll: true,
		}).Where("rev = ?", kv.Rev).Create(kv)

		//if b.db.RowsAffected == 0 {
		//	b.err = storage.ErrCASFailed
		//	b.err = storage.NewErrConflict(idx, key, kv.Val)
		//	return
		//}
	}
	b.fs = append(b.fs, f)
}

func (b *batch) Del(key []byte) {
	//idx := len(b.fs)
	f := func() {
		kv, _ := decode(key)
		b.db = b.db.Where("rev = ?", kv.Rev).Delete(kv)
		if b.db.RowsAffected == 0 {
			b.err = storage.ErrCASFailed
			//b.err = storage.NewErrConflict(idx, key, kv.Val)
		}
	}

	b.fs = append(b.fs, f)
}

func (b *batch) DelCurrent(it storage.Iter) {
	//idx := len(b.fs)
	f := func() {
		kv, _ := decode(it.Key())
		b.db = b.db.Where("val = ?", it.Val()).Where("rev = ?", kv.Rev).Delete(kv)
		if b.db.RowsAffected == 0 {
			b.err = storage.ErrCASFailed
			//b.err = storage.NewErrConflict(idx, it.Key(), kv.Val)
		}
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
			klog.Infof("batch %p txn rollback", b)
			b.db.Rollback()
			return b.error()
		}
	}

	b.db.Commit()
	if b.error() != nil {
		klog.ErrorS(b.error(), "failed to commit")
		return b.error()
	}

	//if len(b.fs) != int(b.db.RowsAffected) {
	//	klog.InfoS("conflict", "expected", len(b.fs), "actual", b.db.RowsAffected)
	//	return storage.NewErrConflict(-1, nil, nil)
	//}
	return nil
}
