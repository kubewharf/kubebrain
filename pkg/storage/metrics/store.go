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

package metrics

import (
	"context"
	"github.com/pkg/errors"
	"io"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// NewKvStorage wraps the storage to emit some metrics while calling
func NewKvStorage(store storage.KvStorage, m metrics.Metrics) storage.KvStorage {
	return &storeWrapper{
		KvStorage:  store,
		metricsCli: m,
	}
}

type storeWrapper struct {
	storage.KvStorage
	metricsCli metrics.Metrics
}

var (
	successTag       = metrics.Tag("state", "success")
	unexpectedErrTag = metrics.Tag("state", "error")
	notFoundTag      = metrics.Tag("state", "key_not_found")
	casFailedTag     = metrics.Tag("state", "cas_failed")
)

var (
	getTag       = metrics.Tag("op", "get")
	delTag       = metrics.Tag("op", "del")
	cmpAndDelTag = metrics.Tag("op", "cmp_and_del") // compare and delete tag
	batchTag     = metrics.Tag("op", "write_batch")
)

const (
	methodKey = "storage.op"
)

func (s *storeWrapper) time(f func() error, opTag metrics.T) {
	start := time.Now()
	err := f()
	stateTag := genStateTag(err)
	duSec := time.Now().Sub(start).Seconds()
	_ = s.metricsCli.EmitHistogram(methodKey, duSec, stateTag, opTag)
}

func genStateTag(err error) metrics.T {
	tag := unexpectedErrTag
	switch {
	case err == nil:
		tag = successTag
	case errors.Is(err, storage.ErrKeyNotFound):
		tag = notFoundTag
	case errors.Is(err, storage.ErrCASFailed):
		tag = casFailedTag
	}
	return tag
}

// Get implements storage.KvStorage
func (s *storeWrapper) Get(ctx context.Context, key []byte) (val []byte, err error) {
	f := func() error {
		val, err = s.KvStorage.Get(ctx, key)
		return err
	}
	s.time(f, getTag)
	return
}

// Iter implements storage.KvStorage
func (s *storeWrapper) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	iter, err := s.KvStorage.Iter(ctx, start, end, timestamp, limit)
	if err != nil {
		_ = s.metricsCli.EmitCounter("storage.iter.start", 1, unexpectedErrTag)
		return nil, err
	}
	_ = s.metricsCli.EmitCounter("storage.iter.start", 1, successTag)
	return newIterWrapper(iter, s.metricsCli, limit), nil
}

// BeginBatchWrite implements storage.KvStorage
func (s *storeWrapper) BeginBatchWrite() storage.BatchWrite {
	b := s.KvStorage.BeginBatchWrite()
	return newBatchWriteWrapper(b, s.metricsCli)
}

// Del implements storage.KvStorage
func (s *storeWrapper) Del(ctx context.Context, key []byte) (err error) {
	f := func() error {
		err = s.KvStorage.Del(ctx, key)
		return err
	}
	s.time(f, delTag)
	return
}

// DelCurrent implements storage.KvStorage
func (s *storeWrapper) DelCurrent(ctx context.Context, iter storage.Iter) (err error) {
	f := func() error {
		internalIter := iter.(*iterWrapper).Iter
		err = s.KvStorage.DelCurrent(ctx, internalIter)
		return err
	}
	s.time(f, cmpAndDelTag)
	return
}

type iterWrapper struct {
	storage.Iter
	m       metrics.Metrics
	start   time.Time
	counter int
	tags    []metrics.T
}

func newIterWrapper(it storage.Iter, m metrics.Metrics, limit uint64) *iterWrapper {
	iw := &iterWrapper{
		Iter:  it,
		m:     m,
		start: time.Now(),
		tags:  []metrics.T{metrics.Tag("limited", strconv.FormatBool(limit > 0))},
	}
	_ = m.EmitCounter("storage.iter.start.success", 1, iw.tags...)
	return iw
}

// Next implements storage.Iter
func (i *iterWrapper) Next(ctx context.Context) (err error) {
	err = i.Iter.Next(ctx)
	if err != nil && err != io.EOF && err != context.Canceled {
		// if there is an unexpected error, emit metric immediately
		_ = i.m.EmitCounter("storage.iter.fetch.error", 1, i.tags...)
	} else if err != nil {
		// do not emit metrics here to reduce cost
		i.counter++
	}
	return
}

// Close implements storage.Iter
func (i *iterWrapper) Close() (err error) {
	err = i.Iter.Close()

	// emit metrics
	duSumSec := time.Now().Sub(i.start).Seconds()
	duAvgSec := float64(0)
	if i.counter != 0 {
		duAvgSec = duSumSec / float64(i.counter)
	}
	_ = i.m.EmitCounter("storage.iter.fetch.success", i.counter, i.tags...)
	_ = i.m.EmitHistogram("storage.iter.duration.avg", duAvgSec, i.tags...)
	_ = i.m.EmitHistogram("storage.iter.duration.sum", duSumSec, i.tags...)
	return
}

type batchWriteWrapper struct {
	storage.BatchWrite
	start   time.Time
	m       metrics.Metrics
	counter int64
}

func newBatchWriteWrapper(batch storage.BatchWrite, m metrics.Metrics) *batchWriteWrapper {
	return &batchWriteWrapper{
		BatchWrite: batch,
		start:      time.Now(),
		m:          m,
	}
}

// PutIfNotExist implements storage.BatchWrite
func (b *batchWriteWrapper) PutIfNotExist(key []byte, val []byte, ttl int64) {
	b.counter++
	b.BatchWrite.PutIfNotExist(key, val, ttl)
}

// CAS implements storage.BatchWrite
func (b *batchWriteWrapper) CAS(key []byte, newVal []byte, oldVal []byte, ttl int64) {
	b.counter++
	b.BatchWrite.CAS(key, newVal, oldVal, ttl)
}

// Put implements storage.BatchWrite
func (b *batchWriteWrapper) Put(key []byte, val []byte, ttl int64) {
	b.counter++
	b.BatchWrite.Put(key, val, ttl)
}

// Del implements storage.BatchWrite
func (b *batchWriteWrapper) Del(key []byte) {
	b.counter++
	b.BatchWrite.Del(key)
}

// DelCurrent implements storage.BatchWrite
func (b *batchWriteWrapper) DelCurrent(it storage.Iter) {
	b.counter++
	internalIter := it.(*iterWrapper).Iter
	b.BatchWrite.DelCurrent(internalIter)
}

// Commit implements storage.BatchWrite
func (b *batchWriteWrapper) Commit(ctx context.Context) (err error) {
	err = b.BatchWrite.Commit(ctx)
	stateTag := genStateTag(err)
	duSumSec := time.Now().Sub(b.start).Milliseconds()
	_ = b.m.EmitHistogram("storage.batch.count", b.counter, batchTag, stateTag)
	_ = b.m.EmitHistogram("storage.batch.duration", duSumSec, batchTag, stateTag)
	return
}
