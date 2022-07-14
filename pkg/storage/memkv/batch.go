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
	"bytes"
	"context"
	"time"

	storage "github.com/kubewharf/kubebrain/pkg/storage"
)

type batch struct {
	store *store
	err   error

	cache   map[string]cacheVal
	opCount int
}

type cacheVal struct {
	isDeleted bool
	val       []byte
	ttl       int64
}

func (s *store) del(key []byte) error {
	b := s.BeginBatchWrite()
	b.Del(key)
	return b.Commit(context.Background())
}

func (s *store) delCurrent(iter storage.Iter) error {
	b := s.BeginBatchWrite()
	b.DelCurrent(iter)
	return b.Commit(context.Background())
}

// PutIfNotExist implements storage.BatchWrite interface
func (b *batch) PutIfNotExist(key []byte, val []byte, ttl int64) {
	if b.err != nil {
		return
	}

	old := b.get(key)
	if old != nil {
		b.err = storage.NewErrConflict(b.opCount, key, old)
		return
	}

	b.cache[string(key)] = cacheVal{
		isDeleted: false,
		val:       val,
		ttl:       ttl,
	}
	b.opCount++
}

// CAS implements storage.BatchWrite interface
func (b *batch) CAS(key []byte, newVal []byte, oldVal []byte, ttl int64) {
	if b.err != nil {
		return
	}

	val := b.get(key)
	if val == nil {
		b.err = storage.NewErrConflict(b.opCount, key, nil)
		return
	}

	if bytes.Compare(val, oldVal) != 0 {
		b.err = storage.NewErrConflict(b.opCount, key, oldVal)
		//b.err = errors.Errorf("cas failed: key %s old val is %s but expect %s", key, string(elem.Value.([]byte)), oldVal)
	}

	b.cache[string(key)] = cacheVal{
		isDeleted: false,
		val:       newVal,
		ttl:       ttl,
	}
	b.opCount++
}

// Put implements storage.BatchWrite interface
func (b *batch) Put(key []byte, val []byte, ttl int64) {
	if b.err != nil {
		return
	}
	b.cache[string(key)] = cacheVal{
		isDeleted: false,
		val:       val,
		ttl:       ttl,
	}
	b.opCount++
}

// Del implements storage.BatchWrite interface
func (b *batch) Del(key []byte) {
	if b.err != nil {
		return
	}
	b.cache[string(key)] = cacheVal{
		isDeleted: true,
	}
	b.opCount++
}

// DelCurrent implements storage.BatchWrite interface
func (b *batch) DelCurrent(it storage.Iter) {
	if b.err != nil {
		return
	}

	if bytes.Compare(b.get(it.Key()), it.Val()) != 0 {
		b.err = storage.ErrCASFailed
	}

	b.cache[string(it.Key())] = cacheVal{
		isDeleted: true,
	}
	b.opCount++
}

func (b *batch) get(key []byte) []byte {
	v, ok := b.cache[string(key)]
	if ok {
		return v.val
	}
	val, _ := b.store.Get(context.Background(), key)
	return val
}

// Commit implements storage.BatchWrite interface
func (b *batch) Commit(ctx context.Context) error {
	defer b.store.mu.Unlock()

	if b.err != nil {
		return b.err
	}

	for k, v := range b.cache {
		keyBytes := []byte(k)
		if v.isDeleted {
			b.store.skl.Remove(keyBytes)
		} else {
			if v.ttl != 0 {
				b.asyncRemove(keyBytes, v.ttl)
			}
			b.store.skl.Set(keyBytes, v.val)
		}

	}

	return nil
}

func (b *batch) asyncRemove(key []byte, seconds int64) {
	if seconds == 0 {
		return
	}

	go func(kvStorage storage.KvStorage) {
		time.AfterFunc(time.Duration(seconds)*time.Second, func() {
			_ = b.store.del(key)
		})
	}(b.store)
}
