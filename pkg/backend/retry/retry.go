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

package retry

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/common"
	"github.com/kubewharf/kubebrain/pkg/backend/tso"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// retry state
const (
	retryState  = "state"
	retryMethod = "method"
)

var (
	retrySuccess     = metrics.Tag(retryState, "success")
	retryFailedGet   = metrics.Tag(retryState, "failed_get")
	retryFailedPut   = metrics.Tag(retryState, "failed_put")
	retryUnknownPut  = metrics.Tag(retryState, "unknown_put")
	retryUnnecessary = metrics.Tag(retryState, "unnecessary")

	retryUpdate = metrics.Tag(retryMethod, "update")
	retryDelete = metrics.Tag(retryMethod, "delete")
	retryCreate = metrics.Tag(retryMethod, "create")
)

func getRetryMethod(verb proto.Event_EventType) metrics.T {
	switch verb {
	case proto.Event_CREATE:
		return retryCreate
	case proto.Event_DELETE:
		return retryDelete
	default:
		return retryUpdate
	}
}

// NewAsyncFifoRetry build a fifo queue to automatically retry uncertain operations
func NewAsyncFifoRetry(coder coder.Coder,
	store storage.KvStorage,
	metrics metrics.Metrics,
	tso tso.TSO,
	getter getter,
	dispatcher dispatcher,
	config Config) AsyncFifoRetry {

	a := &asyncFifoRetryImpl{
		queue:      &eventQueue{},
		coder:      coder,
		store:      store,
		metrics:    metrics,
		tso:        tso,
		getter:     getter,
		dispatcher: dispatcher,
		config:     config,
	}
	return a
}

// Config is the config of retry queue
type Config struct {
	// UnaryTimeout is the max time to send unary request
	UnaryTimeout time.Duration

	// CheckInterval is the interval of checking
	CheckInterval time.Duration

	// RetryInterval is the interval of retrying
	RetryInterval time.Duration

	// Tombstone is the tombstone
	Tombstone []byte
}

type getter func(ctx context.Context, key []byte) (val []byte, modRevision uint64, err error)
type dispatcher func(ctx context.Context, key []byte, val []byte, revision, preRevision uint64, valid bool, eventType proto.Event_EventType, err error)

type asyncFifoRetryImpl struct {
	// internal components
	queue *eventQueue

	// external components
	coder   coder.Coder
	store   storage.KvStorage
	metrics metrics.Metrics
	tso     tso.TSO

	// callback func
	getter     getter
	dispatcher dispatcher

	// config
	config Config
}

// MinRevision implements AsyncFifoRetry interface
func (a *asyncFifoRetryImpl) MinRevision() uint64 {
	head := a.queue.getHead()
	if head != nil {
		return head.event.Revision
	}
	return 0
}

// Size implements AsyncFifoRetry interface
func (a *asyncFifoRetryImpl) Size() int {
	return a.queue.size()
}

// Append implements AsyncFifoRetry interface
func (a *asyncFifoRetryImpl) Append(event *common.WatchEvent) {
	a.queue.push(event)
}

// Run implements AsyncFifoRetry interface
func (a *asyncFifoRetryImpl) Run(ctx context.Context) {
	ticker := time.NewTicker(a.config.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		retryLoop:
			for {
				breakLoop := a.retry(ctx)
				if breakLoop {
					break retryLoop
				}
			}
		}
	}
}

func (a *asyncFifoRetryImpl) retry(ctx context.Context) (breakLoop bool) {

	a.metrics.EmitGauge("async_retry.queue_size", a.queue.size())
	node := a.queue.getHead()
	start := time.Now()
	if node == nil || start.Sub(node.time) < a.config.RetryInterval {
		return true
	}

	state := retrySuccess
	defer func() {
		// emit retry metrics
		a.metrics.EmitHistogram("async_retry.retry", time.Now().Sub(start).Seconds(),
			state, getRetryMethod(node.event.ResourceVerb))
	}()

	// retry
	klog.InfoS("async retry",
		"key", string(node.event.Key),
		"prevOpRevision", node.event.Revision,
		"op", node.event.ResourceVerb.String())

	retryCtx, cancel := context.WithTimeout(ctx, 2*a.config.UnaryTimeout)
	rev, err := a.overwrite(retryCtx, node.event.Key, node.event.Revision)
	cancel()
	if rev == 0 {
		// if rev is zero, it means that there is no new write batch
		if err != nil {
			//if there is no new write batch because of an error, just retry in next tick
			klog.ErrorS(err, "failed to get latest value",
				"key", string(node.event.Key))
			state = retryFailedGet
			return true
		}
		// if there is no error just process next event
		klog.InfoS("no need to fix", "key", string(node.event.Key))
		state = retryUnnecessary
	} else {
		// if rev is not zero, it means the latest value can be read and there is a new write batch
		val := node.event.Value
		verb := node.event.ResourceVerb
		prevRev := node.event.PrevRevision

		// if err is still uncertain, a new event will enqueue soon
		a.dispatcher(ctx, node.event.Key, val, rev, prevRev, err == nil, verb, err)
		if err != nil {
			klog.ErrorS(err, "failed to retry",
				"key", string(node.event.Key),
				"prevOpRevision", node.event.Revision,
				"op", node.event.ResourceVerb.String())
			state = retryFailedPut
			if errors.Is(err, storage.ErrUncertainResult) {
				state = retryUnknownPut
			}
		}
	}

	a.queue.pop()
	return false
}

func (a *asyncFifoRetryImpl) overwrite(ctx context.Context, key []byte, prevOpRev uint64) (rev uint64, err error) {
	val, modRev, err := a.getter(ctx, key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}

	if len(val) == 0 || modRev != prevOpRev {
		// not found or revision not match, just do nothing and return
		//! we assume:
		//!   if getting is successful, it should be the latest value.
		//!   and the result of prev uncertain operation can be certain now,
		//!   which means it's successful, failed or discarded.
		return 0, nil
	}
	klog.InfoS("internal retry", "key", string(key), "prevRev", prevOpRev)
	rev, err = a.tso.Deal()
	if err != nil {
		return rev, err
	}
	revKey := a.coder.EncodeRevisionKey(key)
	objKey := a.coder.EncodeObjectKey(key, rev)
	batch := a.store.BeginBatchWrite()

	prevRevBytes := make([]byte, 8, 9)
	binary.BigEndian.PutUint64(prevRevBytes, prevOpRev)
	revBytes := make([]byte, 8, 9)
	binary.BigEndian.PutUint64(revBytes, rev)

	if bytes.Compare(val, a.config.Tombstone) == 0 {
		// append delete flag after revision bytes
		revBytes = append(revBytes, 0)
		prevRevBytes = append(prevRevBytes, 0)
	}

	batch.CAS(revKey, revBytes, prevRevBytes, 0)
	batch.Put(objKey, val, 0) // rewrite it event if it's tombstone
	err = batch.Commit(ctx)

	return rev, err
}
