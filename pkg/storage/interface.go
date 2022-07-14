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

package storage

import (
	"context"
	"fmt"
)

// ExclusiveKvStorage defines the context individual KvStorage for the background job.
// * Leader runs background compaction periodically, which will may lead to high network io throughput on several tcp
// * conns of a single client. In this case, background compaction can make a notable impact on the latency of writing
// * requests which shares the same tcp conns with compact.
// * Thus, as an optional optimization, implement this interface besides KvStorage to get an exclusive one to reduce the
// * side effect of background compaction.
type ExclusiveKvStorage interface {
	// GetExclusiveKvStorage returns an exclusive KvStorage with exclusive client for operation of high io throughput in leader
	GetExclusiveKvStorage() KvStorage
}

// KvStorage defines the storage engine on kv database
type KvStorage interface {

	// GetTimestampOracle returns the logical timestamp if it could support
	// todo: deprecate it after KubeBrain native tso is implemented
	GetTimestampOracle(ctx context.Context) (timestamp uint64, err error)

	// GetPartitions returns the partitions that keys are spread over in
	// If it's not supported, just return [][]byte{start, end}, 0 , nil
	GetPartitions(ctx context.Context, start, end []byte) (partitions []Partition, err error)

	// Get returns value indexed by key
	// If it's not exist, return ErrKeyNotFound
	Get(ctx context.Context, key []byte) (val []byte, err error)

	// Iter get keys from `start` to `end` (`end` will be smaller than `start` if SupportIterForward is true)
	// todo: if refactor the format and procedure of write data, all iter may be from a smaller beginning to a lager ending.
	Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (Iter, error)

	// BeginBatchWrite returns a new BeginBatchWrite instance
	BeginBatchWrite() BatchWrite

	Writer

	FeatureSupport

	// Close the kv storage
	Close() error
}

// FeatureSupport indicates whether storage engine support some non-core feature
type FeatureSupport interface {
	SupportTTL() bool
}

// Writer defines some methods to modify kv in storage engine
// it aims to avoid multirow transaction if possible when write one key only
// it can be implemented by wrapping the BatchWrite
type Writer interface {
	// Del removes kv from kv storage
	Del(ctx context.Context, key []byte) (err error)

	// DelCurrent removes kv ref by Iter
	// Should be implemented as delete-if-value-equal or delete-if-version-equal
	DelCurrent(ctx context.Context, iter Iter) (err error)
}

// BatchWrite should support atomic batch pack with several operations
type BatchWrite interface {

	// PutIfNotExist creates kv if it doesn't exist.
	// If it exists, return ErrCASFailed while committed
	// *If it's available, return a Conflict instead of ErrCASFailed while committed to pass unexpected kv
	PutIfNotExist(key []byte, val []byte, ttl int64)

	// CAS compare and swap the value indexed by given key
	// todo: deprecate it if refactor the format and procedure of write data
	// If result of compare is false, return ErrCASFailed while committed
	// * If it's available, return a Conflict instead of ErrCASFailed committed to pass unexpected kv
	CAS(key []byte, newVal []byte, oldVal []byte, ttl int64)

	// Put kv to storage
	Put(key []byte, val []byte, ttl int64)

	// Del remove kv from storage
	Del(key []byte)

	// DelCurrent is an ugly design to unify the cas deleting in different storage implement
	DelCurrent(it Iter)

	// Commit commits batch atomically, return the first error in batch
	// * must return ErrUncertainResult if client can not know whether data is written
	Commit(ctx context.Context) error
}

// Iter is the iterator on a **snapshot** of kv storage with batch buffer
// Example:
//
//	iter := kvStorage.Iter(start, end, snapshotID, false)
//  defer iter.Close()
//  iterCtx := context.WithTimeout(ctx, timeout)
// 	for {
//      err := iter.Next(iterCtx)
//      if err != nil {
//          if err == io.EOF {
//             // come to the end
//          }
//      }
//      key, value := iter.Key(), iter.Val()
//      // processing ...
//   }
//
type Iter interface {
	// Key returns keys in buffer
	Key() []byte

	// Val returns values in buffer
	Val() []byte

	// Next get data from kv storage
	// err should be io.EOF if there is no more keys
	Next(ctx context.Context) (err error)

	// Close the iter
	Close() error
}

var (
	ErrUnsupported   = fmt.Errorf("unsupported")
	ErrKeyNotFound   = fmt.Errorf("not found")
	ErrKeyDuplicated = fmt.Errorf("key duplicated")
	ErrCASFailed     = fmt.Errorf("cas failed")
	ErrUnexpectedRet = fmt.Errorf("unexpected return")
	ErrUnavailable   = fmt.Errorf("unavailable")
)

// Partition indicates the boarder of an ordered key region `[Start, End)`
type Partition struct {
	// Start is the least key of the region
	Start []byte

	// End is the key only greater than keys in this partition, and it may be equal to `Start` of next partition
	End []byte
}
