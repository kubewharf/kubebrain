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

package scanner

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

const (
	scanInitDelay     = 1 * time.Second
	scanBackoffFactor = 3
	scanBackoffSteps  = 3
	iterTimeout       = 1000 * time.Second
	rangeStreamBatch  = 300

	revisionValueLengthWithDeletionFlag = 9
)

// NewScanner create a Scanner
func NewScanner(store storage.KvStorage, coder coder.Coder, config Config, metricCli metrics.Metrics) Scanner {
	return &scanner{
		store:            store,
		coder:            coder,
		config:           config,
		metricCli:        metricCli,
		compactHistories: newCompactRecordQueue(),
	}
}

type scanner struct {
	store     storage.KvStorage
	coder     coder.Coder
	metricCli metrics.Metrics
	config    Config

	// component state
	compactHistories *compactRecordQueue
}

// Config is the configuration of scanner
type Config struct {
	// CompactKey is internal key to store the compact state
	CompactKey []byte

	// Tombstone is the value bytes used to mark delete
	Tombstone []byte

	// TTL is the time that key written with ttl will live
	TTL time.Duration
}

// Range implements Scanner interface
func (r *scanner) Range(ctx context.Context, start []byte, end []byte, revision uint64, limit int64) ([]*proto.KeyValue, error) {
	if limit > 0 {
		return r.rangeWithLimit(ctx, start, end, revision, limit)
	}

	receiver := &commonResultReceiver{}
	_, err := r.scan(ctx, start, end, revision, false, receiver)
	if err != nil {
		return nil, err
	}
	return receiver.result, nil
}

func (r *scanner) rangeWithLimit(ctx context.Context, start []byte, end []byte, revision uint64, limit int64) ([]*proto.KeyValue, error) {
	tso, err := r.store.GetTimestampOracle(ctx)
	if err != nil {
		return nil, err
	}
	err = r.checkCompactRace(ctx, revision, false)
	if err != nil {
		return nil, err
	}
	receiver := &commonResultReceiver{limit: int(limit)}
	w := newWorker(workerConfig{
		idx:       0,
		partition: storage.Partition{Start: start, End: end},
		tso:       tso,
		revision:  revision,
		tombstone: r.config.Tombstone,
		compact:   false,
	}, r.store, r.coder, r.metricCli)
	_, err = w.run(ctx, receiver)
	if err != nil {
		return nil, err
	}
	return receiver.result, nil
}

// Count implements Scanner interface
func (r *scanner) Count(ctx context.Context, start []byte, end []byte, revision uint64) (int, error) {
	// ban the calling of Count right now
	receiver := &emptyResultReceiver{}
	return r.scan(ctx, start, end, revision, false, receiver)
}

// RangeStream implements Scanner interface
func (r *scanner) RangeStream(ctx context.Context, start []byte, end []byte, revision uint64) chan *proto.StreamRangeResponse {
	// todo: set buffer size
	stream := make(chan *proto.StreamRangeResponse, 1000)
	receiver := newStreamReceiver(revision, stream)

	go func() {
		defer close(stream)
		_, err := r.scan(ctx, start, end, revision, false, receiver)
		stream <- getListStreamEnd(revision, err)
		if err != nil {
			klog.Errorf("backend list stream with revision %d failed %v, start key is %s, end key is %s", revision, err, start, end)
			r.metricCli.EmitCounter("backend.list.by.stream.failed", 1)
		}
	}()

	return stream
}

func (r *scanner) logCompactHistory(revision uint64) {
	klog.InfoS("log compact history", "rev", revision)
	cr := &compactRecord{
		revision: revision,
		time:     time.Now(),
	}
	r.compactHistories.push(cr)
}

func (r *scanner) getTimeoutRevision() uint64 {
	if r.store.SupportTTL() {
		return 0
	}

	// todo: if it's need to lock here to make it called concurrent-safely?
	prev := &compactRecord{}
	head := r.compactHistories.head()
	for head != nil {
		interval := time.Since(head.time)
		klog.InfoS("check compact history", "interval", interval.String(), "rev", head.revision)
		if interval < r.config.TTL {
			break
		}
		r.compactHistories.pop()
		prev = head
		head = r.compactHistories.head()
	}

	klog.InfoS("get timeout revision", "rev", prev.revision)
	return prev.revision
}

func getListStreamEnd(revision uint64, err error) *proto.StreamRangeResponse {
	response := &proto.StreamRangeResponse{
		// canceled means eof
		RangeResponse: &proto.RangeResponse{
			More:   false,
			Header: &proto.ResponseHeader{Revision: revision},
		},
	}
	if err != nil {
		// if err occurs, set it in CancelReason field
		response.Err = err.Error()
	}
	return response
}

// Compact implements Scanner interface
func (r *scanner) Compact(ctx context.Context, start []byte, end []byte, revision uint64) {
	r.logCompactHistory(revision)
	_, _ = r.scan(ctx, start, end, revision, true, &emptyResultReceiver{})
}

// adjustPartitionsBorders adjust the borders of partitions to avoid object keys generated from an internal key
// are scanned by multiple workers, which may cause error.
func (r *scanner) adjustPartitionsBorders(ps []storage.Partition) (ret []storage.Partition) {
	sort.Slice(ps, func(i, j int) bool {
		return bytes.Compare(ps[i].Start, ps[j].Start) < 0
	})

	for i := 0; i < len(ps); i++ {
		// ! if there is an error, it means border is not an interval key, just not modify it.
		// ! but if there is no error, it means border is an interval key and should be adjusted.
		// ! specially: ignore the start of first partition and the end of last partition.
		if i != 0 {
			// start border may be moved forward except the first partition
			ps[i].Start = ps[i-1].End
		}

		if i != len(ps)-1 {
			userKey, revision, err := r.coder.Decode(ps[i].End)
			if err == nil && revision != 0 {
				// end border may be moved forward except the last partition
				ps[i].End = r.coder.EncodeRevisionKey(userKey)
			}
		}
	}
	return ps
}

func (r *scanner) scan(ctx context.Context, start []byte, end []byte, revision uint64, compact bool, receiver resultReceiver) (int, error) {
	store := r.store
	if exclusiveKvStorage, ok := r.store.(storage.ExclusiveKvStorage); ok && compact {
		klog.InfoS("compact with exclusive kv storage", "start", string(start), "end", string(end), "rev", revision)
		store = exclusiveKvStorage.GetExclusiveKvStorage()
	}
	startTime := time.Now()

	tso, err := store.GetTimestampOracle(ctx)
	if err != nil {
		return 0, err
	}

	err = r.checkCompactRace(ctx, revision, compact)
	if err != nil {
		return 0, err
	}

	partitions, err := store.GetPartitions(ctx, start, end)
	if err != nil {
		return 0, err
	}

	partitions = r.adjustPartitionsBorders(partitions)

	timeoutRevision := uint64(0)
	if compact {
		timeoutRevision = r.getTimeoutRevision()
	}

	var wg sync.WaitGroup
	errList := make([]error, len(partitions))
	receiverList := make([]resultReceiver, len(partitions))
	globalCount := int64(0)
	wg.Add(len(partitions))

	// run worker concurrently
	for idx := range partitions {
		go func(idx int) {
			defer wg.Done()

			// create a worker
			receiverList[idx] = receiver.fork()
			w := newWorker(workerConfig{
				idx:             idx,
				partition:       partitions[idx],
				tso:             tso,
				revision:        revision,
				compact:         compact,
				tombstone:       r.config.Tombstone,
				timeoutRevision: timeoutRevision,
			}, store, r.coder, r.metricCli)

			// run worker
			localCount := 0
			localCount, errList[idx] = w.runWithBackoffRetry(ctx, receiverList[idx])
			atomic.AddInt64(&globalCount, int64(localCount))

		}(idx)
	}

	wg.Wait()
	// check error
	for _, e := range errList {
		if e != nil {
			return 0, e
		}
	}

	// merge result
	for _, forkedReceiver := range receiverList {
		receiver.merge(forkedReceiver)
		forkedReceiver.close()
	}

	klog.InfoS("scan", "start", start, "end", end, "count", globalCount, "latency", time.Since(startTime))
	return int(globalCount), nil
}

// worker fetches data from a partition
type worker struct {
	workerConfig

	store storage.KvStorage

	coder.Coder

	metricCli metrics.Metrics

	lastCompactFailedRawKey []byte
}

type workerConfig struct {
	// idx is the index of worker
	idx int

	// partition indicate the border of scan
	partition storage.Partition

	// tso indicate the version of snapshot which the group of workers iter on
	tso uint64

	// revision indicate the visible revision of object key
	revision uint64

	// tombstone indicate the deleted key's value
	tombstone []byte

	// compact is the switch of compaction
	compact bool

	// timeoutRevision indicate the revision that kvs with ttl were updated at is timeout
	timeoutRevision uint64
}

func newWorker(conf workerConfig, store storage.KvStorage, coder coder.Coder, metricCli metrics.Metrics) *worker {
	return &worker{
		workerConfig: conf,
		store:        store,
		Coder:        coder,
		metricCli:    metricCli,
	}
}

func (w *worker) runWithBackoffRetry(ctx context.Context, receiver resultReceiver) (int, error) {
	klog.InfoS("worker start processing", "worker", w.info())
	var scanErr error
	var count int

	backOff := wait.Backoff{
		Duration: scanInitDelay,
		Factor:   scanBackoffFactor,
		Steps:    scanBackoffSteps,
	}

	// retry scan with backoff, sleep duration increase after each fail
	err := wait.ExponentialBackoff(backOff, func() (bool, error) {
		select {
		case <-ctx.Done():
			klog.InfoS("context canceled when worker run",
				"partitionsIdx", w.idx, "start", string(w.partition.Start), "end", string(w.partition.End), "compact", w.compact)
			// return err to avoid useless retry
			return false, errors.New("context canceled")
		default:
		}

		if count, scanErr = w.run(ctx, receiver); scanErr == nil {
			return true, nil
		}
		return false, nil
	})

	if err != nil {
		// fail after retry, scanErr records the latest scan err
		if err == wait.ErrWaitTimeout {
			err = fmt.Errorf("partition %d reached the max retry time: %d, encounter latest error %v", w.idx, scanBackoffSteps, scanErr)
		}
	}

	return count, err
}

func (w *worker) run(ctx context.Context, receiver resultReceiver) (int, error) {
	// record start time
	startTime := time.Now()
	scanCtx, cancel := context.WithTimeout(ctx, iterTimeout)

	// create iter
	it, err := w.store.Iter(scanCtx, w.partition.Start, w.partition.End, w.tso, 0)
	defer cancel()

	// todo: check compact race here
	if err != nil {
		klog.ErrorS(err, "scan failed", "worker", w.info())
		return 0, err
	}
	defer it.Close()
	receiver.reset()
	count := 0
	valSize := int64(0)
	// iter cur
	var (
		curUserKey   []byte
		curRevision  uint64
		prevUserKey  []byte
		prevRevision uint64
		prevValue    []byte
	)

	for receiver.needMore() {
		select {
		case <-ctx.Done():
			klog.InfoS("context canceled when worker scan",
				"partitionsIdx", w.idx, "start", string(w.partition.Start), "end", string(w.partition.End), "compact", w.compact)
			return 0, fmt.Errorf("worker run context canceled")
		default:
		}

		// todo: set a shorter timeout?
		nextCtx, cancel := context.WithTimeout(ctx, iterTimeout)
		if err = it.Next(nextCtx); err != nil {
			cancel()
			break
		}
		cancel()

		// get key and value from iter
		key := it.Key()
		curUserKey, curRevision, err = w.Decode(key)
		if err != nil {
			klog.Errorf("unmarshal object key %s failed %v", key, err)
			continue
		}

		value := it.Val()
		valSize += int64(len(value))

		// check kvs with ttl and compact it if it's timeout
		if expired, _ := w.compactIfExpired(it, curUserKey, curRevision, value); expired {
			// if key is expired, just ignore other procession
			continue
		}

		// revision greater than leader mvcc commit index or compact index, ignore
		if curRevision > w.revision {
			continue
		}

		// todo: abstract compaction as a resultReceiver?
		// meet new raw key, should process old raw key
		if !bytes.Equal(curUserKey, prevUserKey) {
			// old raw key is not deleted with revision constraint
			if prevRevision > 0 && !bytes.Equal(prevValue, w.tombstone) {
				receiver.append(prevUserKey, prevValue, prevRevision)
				count++
			}
		} else {
			// raw key has multi versions, delete old versions which satisfies compact revision constraint
			if w.compact && prevRevision > 0 {
				prevKey := w.EncodeObjectKey(prevUserKey, prevRevision)
				klog.InfoS("compact expired object key", "key", prevUserKey, "rev", prevRevision)
				w.compactKey(prevKey, prevUserKey, prevRevision)
			}
		}
		// delete tombstone data
		if w.compact && bytes.Equal(value, w.tombstone) {
			klog.InfoS("compact object key with tombstone", "key", curUserKey, "rev", curRevision)
			w.compactKey(key, curUserKey, curRevision)
		}
		// delete revision with deletion flag
		if w.compact && curRevision == 0 && len(value) == revisionValueLengthWithDeletionFlag {
			klog.InfoS("compact index key", "key", curUserKey, "rev", curRevision, "val", binary.BigEndian.Uint64(value[0:8]), "len", len(value))
			// cas with value to prevent conflict
			w.compactCurrent(it, curUserKey, curRevision)
		}

		prevRevision = curRevision
		prevUserKey = curUserKey
		prevValue = it.Val()
	}

	endTime := time.Now()
	if err != io.EOF {
		klog.ErrorS(err, "worker error", "worker", w.info(), "count", count, "latency", endTime.Sub(startTime))
		return 0, err
	}
	// add last result
	if prevRevision > 0 && !bytes.Equal(prevValue, w.tombstone) && receiver.needMore() {
		receiver.append(prevUserKey, prevValue, prevRevision)
		count++
	}

	receiver.flush()
	scanLatency := endTime.Sub(startTime)
	klog.InfoS("worker done", "worker", w.info(), "latency", scanLatency, "count", count, "valSize", valSize)
	w.metricCli.EmitHistogram("storage.scan_worker.latency", scanLatency.Seconds())
	w.metricCli.EmitHistogram("storage.scan_worker.size", valSize)
	w.metricCli.EmitHistogram("storage.scan_worker.count", count)
	return count, nil
}

func (w *worker) info() string {
	return fmt.Sprintf("partition is %d start key is %s end key is %s, compact is %v", w.idx, string(w.partition.Start), string(w.partition.End), w.compact)
}

func (w *worker) isSkippedRawKey(rawKey []byte, rev uint64) bool {
	if len(w.lastCompactFailedRawKey) > 0 && bytes.Compare(w.lastCompactFailedRawKey, rawKey) == 0 {
		klog.InfoS("compact skip", "rawKey", string(rawKey), "rev", rev)
		w.metricCli.EmitCounter("compact.skip", 1)
		return true
	}
	return false
}

func (w *worker) updateSkippedRawKey(rawKey []byte, rev uint64, err error) {
	klog.ErrorS(err, "compact failed", "rawKey", string(rawKey), "rev", rev)
	if !errors.Is(err, storage.ErrCASFailed) {
		w.lastCompactFailedRawKey = rawKey
	}
}

func (w *worker) compactCurrent(iter storage.Iter, rawKey []byte, rev uint64) error {
	if w.isSkippedRawKey(rawKey, rev) {
		return nil
	}

	w.metricCli.EmitCounter("compact", 1)
	err := w.store.DelCurrent(context.Background(), iter)
	if err != nil {
		w.metricCli.EmitCounter("compact.err", 1)
		w.updateSkippedRawKey(rawKey, rev, err)
	}
	return err
}

func (w *worker) compactKey(key []byte, rawKey []byte, rev uint64) error {
	if w.isSkippedRawKey(rawKey, rev) {
		return nil
	}

	w.metricCli.EmitCounter("compact", 1)
	err := w.store.Del(context.Background(), key)
	if err != nil {
		w.metricCli.EmitCounter("compact.err", 1)
		w.updateSkippedRawKey(rawKey, rev, err)
	}
	return err
}

func (w *worker) compactIfExpired(iter storage.Iter, rawKey []byte, revision uint64, value []byte) (isExpired bool, err error) {
	// run compaction for object with ttl except
	// 1. storage engine support ttl
	// 2. start time is too late
	if w.store.SupportTTL() ||
		w.timeoutRevision == 0 {
		return false, nil
	}
	if bytes.Contains(rawKey, []byte("/events/")) {
		//? consider two type of compact now:
		//? 1. delete directly from storage engine (use this one right now)
		//? 2. set tombstone and delete util next compaction loop
		if revision == 0 { // revision key time out
			rev := binary.BigEndian.Uint64(value[:8])
			if rev <= w.timeoutRevision {
				klog.InfoS("compact expired revision key", "raw key", string(rawKey), "rev", rev)
				return true, w.compactCurrent(iter, rawKey, rev)
			}
		} else if revision <= w.timeoutRevision { // object key timeout
			klog.InfoS("compact expired object key", "raw key", string(rawKey), "rev", revision)
			return true, w.compactKey(iter.Key(), rawKey, revision)
		}
	}

	return false, nil
}

// checkCompactRace will guarantee range request and compact request don't conflict
func (r *scanner) checkCompactRace(ctx context.Context, revision uint64, compact bool) error {

	if compact {
		// compact operation, just try to set the compact revision
		// if it's error, try next time
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, revision)
		batch := r.store.BeginBatchWrite()
		batch.Put(r.config.CompactKey, bs, 0)
		return batch.Commit(ctx)
	}

	// if scan is triggered by range and range stream, check compact race
	// check is processed after get snapshot, so that even compact happens concurrently, it will not affect data for this range
	// get compact revision
	val, err := r.store.Get(ctx, r.config.CompactKey)
	if err != nil {
		// if compact_key is not initialized, return nil
		if err == storage.ErrKeyNotFound {
			return nil
		}
		klog.Errorf("get compact revision failed %v", err)
		return err
	}
	// compare compact revision and range revision
	compactRevision := binary.BigEndian.Uint64(val)
	if compactRevision > revision {
		// revision has already been compacted
		err := fmt.Errorf("range stream revision %d less than compact revision %d", revision, compactRevision)
		return err
	}
	return nil
}
