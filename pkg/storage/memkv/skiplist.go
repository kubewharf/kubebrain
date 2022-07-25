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

package memkv

import (
	"context"
	"sync"
	"time"

	"github.com/huandu/skiplist"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

var _ storage.KvStorage = &store{}

// NewKvStorage create a storage.KvStorage instance based on skiplist
func NewKvStorage() storage.KvStorage {
	return &store{
		skl: skiplist.New(skiplist.Bytes),
	}
}

type store struct {
	skl *skiplist.SkipList
	mu  sync.Mutex
}

// SupportTTL implements storage.KvStorage interface
func (s *store) SupportTTL() bool {
	return true
}

// Del implements storage.KvStorage interface
func (s *store) Del(ctx context.Context, key []byte) (err error) {
	return s.del(key)
}

// DelCurrent implements storage.KvStorage interface
func (s *store) DelCurrent(ctx context.Context, iter storage.Iter) (err error) {
	return s.delCurrent(iter)
}

// GetTimestampOracle implements storage.KvStorage interface
func (s *store) GetTimestampOracle(ctx context.Context) (timestamp uint64, err error) {
	return uint64(time.Now().UnixNano()), nil
}

// Get implements storage.KvStorage interface
func (s *store) Get(ctx context.Context, key []byte) (val []byte, err error) {
	elem := s.skl.Get(key)
	if elem == nil {
		return nil, storage.ErrKeyNotFound
	}
	return elem.Value.([]byte), nil
}

// GetPartitions implements storage.KvStorage interface
func (s *store) GetPartitions(ctx context.Context, start, end []byte) (partitions []storage.Partition, err error) {
	return []storage.Partition{{Start: start, End: end}}, err
}

// Iter implements storage.KvStorage interface
func (s *store) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	it := newIter(s, start, end, int(limit))
	return it, nil
}

// BeginBatchWrite implements storage.KvStorage interface
func (s *store) BeginBatchWrite() storage.BatchWrite {
	s.mu.Lock()
	return &batch{store: s, cache: make(map[string]cacheVal)}
}

// Close implements storage.KvStorage interface
func (s *store) Close() error {
	return nil
}
