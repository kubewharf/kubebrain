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

package backend

import (
	"context"
	"errors"
	"sync/atomic"

	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/common"
	"github.com/kubewharf/kubebrain/pkg/backend/creator"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/backend/retry"
	"github.com/kubewharf/kubebrain/pkg/backend/scanner"
	"github.com/kubewharf/kubebrain/pkg/backend/tso"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

const (
	historyCapacity      = 200000 // TODO config from start option
	watchersChanCapacity = 100000
	eventBatchSize       = 300
)

type Backend interface {

	// Create inserts new key into storage
	Create(ctx context.Context, request *proto.CreateRequest) (*proto.CreateResponse, error)

	// Update set key into storage
	Update(ctx context.Context, request *proto.UpdateRequest) (*proto.UpdateResponse, error)

	// Delete removes key from storage
	Delete(ctx context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error)

	// Compact clears the kvs that are too old
	Compact(ctx context.Context, revision uint64) (*proto.CompactResponse, error)

	// Get read a kv from storage
	Get(ctx context.Context, r *proto.GetRequest) (*proto.GetResponse, error)

	// List read kvs in range
	List(ctx context.Context, r *proto.RangeRequest) (*proto.RangeResponse, error)

	// Count counts the number of kvs in range
	Count(ctx context.Context, r *proto.CountRequest) (*proto.CountResponse, error)

	// GetPartitions query the partition state of storage for ListByStream
	GetPartitions(ctx context.Context, r *proto.ListPartitionRequest) (*proto.ListPartitionResponse, error)

	// ListByStream reads kvs in range by stream
	ListByStream(ctx context.Context, startKey, endKey []byte, revision uint64) (<-chan *proto.StreamRangeResponse, error)

	// Watch subscribe the changes from revision on kvs with given prefix
	Watch(ctx context.Context, key string, revision uint64) (<-chan []*proto.Event, error)

	// GetResourceLock returns the resource lock for leader election
	GetResourceLock() resourcelock.Interface

	// GetCurrentRevision returns the read revision
	GetCurrentRevision() uint64

	// SetCurrentRevision is used for init tso for leader
	SetCurrentRevision(uint64)
}

var _ Backend = (*backend)(nil)

// watch data flow is as follows
// 1. write event triggered by storage write, then inserted into ring buffer
// 2. fetch write event with right sequence from ring buffer
//   2.1 insert into cache ring
//   2.2 insert into watch chan to broadcast to watchers

type backend struct {
	tso tso.TSO

	election election.ResourceLockManager

	kv storage.KvStorage

	coder coder.Coder

	creator creator.Creator

	scanner scanner.Scanner

	asyncFifoRetry retry.AsyncFifoRetry

	config Config

	watchEventsRingBuffer []atomic.Value

	// maximum size of history watch event window.
	capacity int
	// history watch event cache for watch request catch up
	watchCache *Ring

	// channel to pass etcd watch event to watchers
	watchChan chan []*proto.Event
	// hold watchers
	watcherHub *WatcherHub

	metricCli metrics.Metrics
}

// Config is the configuration for backend
type Config struct {
	// EnableEtcdCompatibility make backend compatible with etcd3
	EnableEtcdCompatibility bool

	// Prefix is the range that backend is in charge of
	Prefix string

	// Identity is the identity for a unique backend
	Identity string

	// SkippedPrefixes is the range that backend is not in charge of
	SkippedPrefixes []string
}

// NewBackend builds a new backend
func NewBackend(kv storage.KvStorage, config Config, metricCli metrics.Metrics) Backend {
	normalCoder := coder.NewNormalCoder()
	electionConfig := election.Config{Prefix: config.Prefix, Identity: config.Identity, Timeout: unaryRpcTimeout}
	b := &backend{
		kv:                    kv,
		tso:                   tso.NewTSO(),
		coder:                 normalCoder,
		creator:               creator.NewNaiveCreator(kv, normalCoder),
		election:              election.NewResourceLockManager(electionConfig, kv),
		scanner:               scanner.NewScanner(kv, normalCoder, config.getScannerConfig(), metricCli),
		config:                config,
		capacity:              historyCapacity,
		watchEventsRingBuffer: make([]atomic.Value, watchersChanCapacity, watchersChanCapacity),
		watchCache:            NewRing(historyCapacity),
		watchChan:             make(chan []*proto.Event, watchersChanCapacity),
		watcherHub: &WatcherHub{
			subs:      make(map[chan []*proto.Event]struct{}),
			metricCli: metricCli,
		},
		metricCli: metricCli,
	}

	asyncRetryConfig := retry.Config{
		UnaryTimeout:  unaryRpcTimeout,
		CheckInterval: checkInterval,
		RetryInterval: retryInterval,
		Tombstone:     tombStoneBytes,
	}
	b.asyncFifoRetry = retry.NewAsyncFifoRetry(b.coder, b.kv, b.metricCli, b.tso, b.getLatestInternalVal, b.notify, asyncRetryConfig)

	// TODO stop chan
	// write into watch chan, trigger by create/ update/ delete method in storage interface
	go b.collectStorageWriteEvents()

	// broadcast fan-out to all subscribed watchers
	go b.watcherHub.Stream(b.watchChan)

	go b.asyncFifoRetry.Run(context.Background())

	return b
}

var ErrRevisionDriftBack = errors.New("revision drift back")

func (b *backend) deal(prevRevision uint64) (uint64, error) {
	rev, err := b.tso.Deal()
	if err != nil {
		return 0, err
	}
	if prevRevision > 0 && rev < prevRevision {
		klog.ErrorS(ErrRevisionDriftBack, "deal", "generated", rev, "prev", prevRevision)
		b.metricCli.EmitCounter("revision.generator.invalid", 1)
		return rev, ErrRevisionDriftBack
	}

	if rev%100 == 0 {
		b.metricCli.EmitGauge("revision.generator", rev)
	}

	return rev, nil
}

func (b *backend) collectStorageWriteEvents() {
	// TODO(xiangchao.01):  config from config file or options
	events := make([]*proto.Event, eventBatchSize)
	// infinite loop
	for {
		cnt := 0
		for cnt < eventBatchSize {
			idx := (b.GetCurrentRevision() + 1) % watchersChanCapacity
			watchEvent, ok := b.watchEventsRingBuffer[idx].Load().(*common.WatchEvent)
			if !ok || watchEvent == nil {
				if cnt == 0 {
					// no event in inside loop, continue inside loop
					continue
				}
				// break inside loop for sending existing  events, then read events in a new loop
				break
			}
			b.metricCli.EmitGauge("watch.set.current.revision", watchEvent.Revision)
			b.watchEventsRingBuffer[watchEvent.Revision%watchersChanCapacity].Store((*common.WatchEvent)(nil))

			// invalid watch event, i.e. cas failed
			if !watchEvent.Valid {
				if errors.Is(watchEvent.Err, storage.ErrUncertainResult) {
					// must enqueue before update revision, otherwise it may be compact
					b.asyncFifoRetry.Append(watchEvent)
				}
				b.SetCurrentRevision(watchEvent.Revision)
				continue
			}

			b.SetCurrentRevision(watchEvent.Revision)

			e := &proto.Event{
				Type:     watchEvent.ResourceVerb,
				Revision: watchEvent.Revision,
			}
			if watchEvent.ResourceVerb == proto.Event_DELETE {
				e.Kv = &proto.KeyValue{
					Key:      watchEvent.Key,
					Value:    watchEvent.Value,
					Revision: watchEvent.PrevRevision,
				}
			} else {
				e.Kv = &proto.KeyValue{
					Key:      watchEvent.Key,
					Value:    watchEvent.Value,
					Revision: watchEvent.Revision,
				}
			}
			// push to watchers
			events[cnt] = e
			cnt++
			// set watch cache
			b.watchCache.Add(e)
		}

		if cnt > 0 {
			evs := make([]*proto.Event, cnt)
			copy(evs, events[:cnt])
			b.watchChan <- evs
		}
	}
}

// GetResourceLock implements Backend interface
func (b *backend) GetResourceLock() resourcelock.Interface {
	return b.election.GetResourceLock()
}

// GetCurrentRevision implements Backend interface
func (b *backend) GetCurrentRevision() uint64 {
	return b.tso.GetRevision()
}

// SetCurrentRevision implements Backend interface
func (b *backend) SetCurrentRevision(revision uint64) {
	b.tso.Commit(revision)
}

func responseHeader(rev uint64) *proto.ResponseHeader {
	return &proto.ResponseHeader{
		Revision: rev,
	}
}
