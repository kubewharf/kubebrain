// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type batch struct {
	txn  *txnkv.KVTxn
	list []func(ctx context.Context) error
}

func (b *batch) PutIfNotExist(key []byte, val []byte, ttl int64) {
	idx := len(b.list)
	b.list = append(b.list, func(ctx context.Context) error {
		oldVal, err := b.txn.Get(ctx, key)
		if err != nil && tikverr.IsErrNotFound(err) {
			err = b.txn.Set(key, val)
			if err != nil {
				return errors.Wrapf(err, "fail to create key %s", string(key))
			}
			return nil
		} else if err == nil {
			return storage.NewErrConflict(idx, key, oldVal)
		}
		return errors.Wrapf(err, "fail to get key %s", string(key))
	})
}

func (b *batch) CAS(key []byte, newVal []byte, oldVal []byte, ttl int64) {
	idx := len(b.list)
	b.list = append(b.list, func(ctx context.Context) error {
		val, err := b.txn.Get(ctx, key)
		if err != nil {
			if tikverr.IsErrNotFound(err) {
				return storage.ErrKeyNotFound
			}
			return errors.Wrapf(err, "fail to get key %s", string(key))
		}
		if bytes.Compare(oldVal, val) != 0 {
			return storage.NewErrConflict(idx, key, val)
		}
		err = b.txn.Set(key, newVal)
		if err != nil {
			return errors.Wrapf(err, "fail to set key %s", string(key))
		}
		return nil
	})
}

func (b *batch) Put(key []byte, val []byte, ttl int64) {
	b.list = append(b.list, func(ctx context.Context) error {
		err := b.txn.Set(key, val)
		if err != nil {
			return errors.Wrapf(err, "fail to set key %s", string(key))
		}
		return nil
	})
}

func (b *batch) Del(key []byte) {
	b.list = append(b.list, func(ctx context.Context) error {
		err := b.txn.Delete(key)
		if err != nil {
			return errors.Wrapf(err, "fail to set key %s", string(key))
		}
		return nil
	})
}

func (b *batch) DelCurrent(it storage.Iter) {
	idx := len(b.list)
	b.list = append(b.list, func(ctx context.Context) error {
		tiIter := it.(*iter)
		key := tiIter.iter.Key()
		oldVal, err := b.txn.Get(ctx, key)
		if err != nil {
			if tikverr.IsErrNotFound(err) {
				return storage.NewErrConflict(idx, it.Key(), nil)
			}
			return errors.Wrapf(err, "fail to get key %s", string(key))
		}
		if bytes.Compare(oldVal, tiIter.iter.Value()) != 0 {
			return storage.NewErrConflict(idx, it.Key(), oldVal)
		}
		return b.txn.Delete(tiIter.iter.Key())
	})
}

func (b *batch) Commit(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			b.txn.Rollback()
		}
	}()
	for _, f := range b.list {
		err = f(ctx)
		if err != nil {
			return err
		}
	}

	err = b.txn.Commit(ctx)

	if err != nil {
		if tikverr.IsErrWriteConflict(err) {
			return storage.ErrCASFailed
		}

		for _, uncertainErr := range uncertainErrList {
			if errors.Is(err, uncertainErr) {
				err = storage.NewErrUncertainResult(err)
			}
		}
	}
	return
}

// todo: add other errors
var uncertainErrList = []error{
	context.DeadlineExceeded,
	context.Canceled,
	tikverr.ErrBodyMissing,
	tikverr.ErrTiKVServerTimeout,
	tikverr.ErrUnknown,
}
