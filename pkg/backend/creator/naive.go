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

package creator

import (
	"context"
	"encoding/binary"

	"github.com/pkg/errors"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// naiveCreator is a LeaseController use ttl as lease id and implement gc based on ttl feature provided by kv storage
type naiveCreator struct {
	store storage.KvStorage
	coder coder.Coder
}

// NewNaiveCreator build a naive creator
func NewNaiveCreator(store storage.KvStorage, coder coder.Coder) Creator {
	return &naiveCreator{
		store: store,
		coder: coder,
	}
}

func uint64ToBytes(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

// Create implements Creator interface
func (l *naiveCreator) Create(ctx context.Context, key []byte, value []byte, revision uint64) (err error) {
	return l.CreateWithTTL(ctx, key, value, revision, 0)
}

// CreateWithTTL implements Creator interface
func (l *naiveCreator) CreateWithTTL(ctx context.Context, key []byte, val []byte, revision uint64, ttl int64) (err error) {
	revisionKey := l.coder.EncodeRevisionKey(key)
	objectKey := l.coder.EncodeObjectKey(key, revision)
	revisionBytes := uint64ToBytes(revision)

	err = l.create(ctx, revisionKey, objectKey, val, revisionBytes, ttl)
	if err != nil {
		if errors.Is(err, storage.ErrCASFailed) {
			var oldRev []byte
			if conflict, ok := err.(*storage.Conflict); ok && conflict.Idx == 0 {
				oldRev = conflict.Val
			} else {
				oldRev, err = l.store.Get(ctx, l.coder.EncodeRevisionKey(key))
				if err != nil {
					// if old revision can not be read
					// 1. it had deletion flag and was compacted, just create again
					// 2. storage is unavailable right now
					if errors.Is(err, storage.ErrKeyNotFound) {
						return l.create(ctx, revisionKey, objectKey, val, revisionBytes, ttl)
					}
					return storage.ErrUnavailable
				}
			}

			// concurrent create and delete
			prevRevision, isTombstone, parseErr := coder.ParseRevision(oldRev)
			if parseErr != nil {
				return parseErr
			}

			if isTombstone && prevRevision < revision {
				return l.update(ctx, revisionKey, objectKey, val, revisionBytes, oldRev, ttl)
			}
			return storage.ErrCASFailed
		}
		return err
	}
	return nil
}

func (l *naiveCreator) create(ctx context.Context, revisionKey []byte, objectKey []byte, value []byte, revision []byte, lease int64) (err error) {
	batch := l.store.BeginBatchWrite()
	batch.PutIfNotExist(revisionKey, revision, lease)
	batch.Put(objectKey, value, lease)
	return batch.Commit(ctx)
}

func (l *naiveCreator) update(ctx context.Context, revisionKey []byte, objectKey []byte, value []byte, newRevision []byte, oldRevision []byte, lease int64) (err error) {
	batch := l.store.BeginBatchWrite()
	batch.CAS(revisionKey, newRevision, oldRevision, lease)
	batch.Put(objectKey, value, lease)
	return batch.Commit(ctx)
}
