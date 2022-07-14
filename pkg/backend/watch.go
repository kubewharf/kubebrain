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
	"bytes"
	"context"
	"fmt"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	resultChanLength = 100
)

// Watch return a channel, every event‘s ModRevision >= revision
// and has specify prefix will be read from channel
// if revision < 0, invalid revision
// if revision == 0, start reading events from read channel
func (b *backend) Watch(ctx context.Context, prefix string, revision uint64) (<-chan []*proto.Event, error) {

	klog.InfoS("WATCH", "prefix", prefix, "revision", revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan, err := b.watcherHub.AddWatcher(ctx)
	if err != nil {
		cancel()
		klog.ErrorS(err, "add watcher failed", "chan", readChan)
		return nil, err
	}

	result := make(chan []*proto.Event, resultChanLength)

	// include the current revision in list
	if revision < 0 {
		cancel()
		klog.Errorf("revision < 0, close chan %v", readChan)
		return nil, fmt.Errorf("invalid revision, revision is %d", revision)
	}

	if revision == 0 {
		go b.processEvents(cancel, result, readChan, prefix, revision)
		return result, nil
	}

	ret := b.watchCache.FindEvents(revision)

	if ret.empty {
		if uint64(revision) > b.tso.GetRevision() {
			// watch revision is latest, no need to fetch history
			go b.processEvents(cancel, result, readChan, prefix, revision)
			return result, nil
		}
		currentRevision := b.tso.GetRevision()
		// event cache is empty
		cancel()
		klog.Errorf("empty cache event, close chan %v", readChan)
		return nil, fmt.Errorf(" empty cache event, current revision is %d", currentRevision)
	}

	if ret.high {
		go b.processEvents(cancel, result, readChan, prefix, revision)
		return result, nil
	}

	if ret.low {
		// need read oldest event
		oldestRevision := ret.oldest.Revision
		cancel()
		klog.Errorf("ret low, close chan, %v", readChan)
		return nil, fmt.Errorf("cache event oldest revision is %d newer than requested revision %d", oldestRevision, revision+1)
	}

	rev := ret.newest.Revision
	events := filterByPrefix(ret.events, []byte(prefix))

	klog.InfoS("watch list", "prefix", prefix, "revision", revision, "latestRev", rev, "cachedEvents", len(events))

	lastRevision := revision
	if len(events) > 0 {
		lastRevision = rev + 1
		b.catchUpEvents(result, events)
	}
	go b.processEvents(cancel, result, readChan, prefix, lastRevision)

	return result, nil
}

// catchUpEvents send stacked events to channel
func (b *backend) catchUpEvents(out chan<- []*proto.Event, events []*proto.Event) {
	batchSize := eventBatchSize
	// to avoid result chan full and hang
	if len(events) > resultChanLength*eventBatchSize {
		batchSize = len(events) / (resultChanLength - 1)
	}
	for {
		if len(events) > batchSize {
			out <- events[0:batchSize]
			events = events[batchSize:]
		} else {
			out <- events
			break
		}
	}
}

func (b *backend) processEvents(cancel context.CancelFunc, out chan<- []*proto.Event, in <-chan []*proto.Event,
	prefix string, revision uint64) {
	prefixBytes := []byte(prefix)
	klog.InfoS("start process events chan", "prefix", prefix, "revision", revision)

	// always ensure we fully read the channel
	for events := range in {
		evs := filterByPrefix(filterByRevision(events, revision), prefixBytes)
		if len(evs) > 0 {
			out <- evs
		}
	}
	// channel closed by watcher hub due to slow process or ctx done
	klog.InfoS("events chan closed", "chan", in, "prefix", prefix)
	b.metricCli.EmitCounter("watcherhub.events_chan.closed", 1, metrics.Tag("prefix", prefix))

	close(out)
	klog.InfoS("watch channel closed", "prefix", prefix)
	cancel()
}

func filterByPrefix(events []*proto.Event, prefix []byte) []*proto.Event {
	filteredEventList := make([]*proto.Event, 0, len(events))

	for _, event := range events {
		if bytes.HasPrefix(event.Kv.Key, prefix) {
			filteredEventList = append(filteredEventList, event)
		}
	}

	return filteredEventList
}

// filter event's ModRevision start from(inclusive) rev
func filterByRevision(events []*proto.Event, rev uint64) []*proto.Event {
	for len(events) > 0 && events[0].Revision < rev {
		events = events[1:]
	}

	return events
}
