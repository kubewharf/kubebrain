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

package badger

import (
	"context"

	"github.com/dgraph-io/badger"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type store struct {
	db *badger.DB
}

type Config struct {
	Dir string
}

func NewKvStorage(config Config) (storage.KvStorage, error) {
	db, err := badger.Open(badger.DefaultOptions(config.Dir))
	if err != nil {
		return nil, err
	}
	return &store{db: db}, nil
}

func (b *store) GetTimestampOracle(ctx context.Context) (timestamp uint64, err error) {
	txn := b.db.NewTransaction(false)
	defer txn.Discard()
	ts := txn.ReadTs()
	return ts, nil
}

func (b *store) SupportTTL() bool {
	return true
}

func (b *store) GetPartitions(ctx context.Context, start, end []byte) (partitions []storage.Partition, err error) {
	return []storage.Partition{{Start: start, End: end}}, nil
}

func (b *store) Get(ctx context.Context, key []byte) (val []byte, err error) {
	txn := b.db.NewTransaction(false)
	defer txn.Discard()
	it, err := txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			err = storage.ErrKeyNotFound
		}
		return nil, err
	}
	return it.ValueCopy(nil)
}

func (b *store) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	i := newIter(b.db, start, end, timestamp, limit)
	return i, nil
}

func (b *store) BeginBatchWrite() storage.BatchWrite {
	batch := &batch{}
	batch.txn = b.db.NewTransaction(true)
	batch.list = make([]func() error, 0, 2)
	return batch
}

func (b *store) Del(ctx context.Context, key []byte) (err error) {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()
	_ = txn.Delete(key)
	return txn.Commit()
}

func (b *store) DelCurrent(ctx context.Context, it storage.Iter) (err error) {
	batch := b.BeginBatchWrite()
	batch.DelCurrent(it)
	return batch.Commit(ctx)
}

func (b *store) Close() error {
	return b.db.Close()
}
