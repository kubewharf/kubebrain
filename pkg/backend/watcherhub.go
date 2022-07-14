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
	"sync"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	// TODO: from start options or self-adaptive
	watchBuffer = 10000
)

// WatcherHub maintain registry of Watcher
type WatcherHub struct {
	sync.RWMutex
	subs      map[chan []*proto.Event]struct{}
	metricCli metrics.Metrics
}

// AddWatcher add watcher, filter by prefix and revision is processed in upper server layer
func (w *WatcherHub) AddWatcher(ctx context.Context) (<-chan []*proto.Event, error) {
	w.metricCli.EmitCounter("watcher_hub.add_watcher", 1)
	w.Lock()
	defer w.Unlock()

	// set watch buffer
	sub := make(chan []*proto.Event, watchBuffer)
	if w.subs == nil {
		w.subs = map[chan []*proto.Event]struct{}{}
	}
	w.subs[sub] = struct{}{}
	go func() {
		<-ctx.Done()
		klog.InfoS("ctx done, delete watcher %v", "chan", sub)
		w.DeleteWatcher(sub, true)
	}()

	return sub, nil
}

// DeleteWatcher delete watcher
func (w *WatcherHub) DeleteWatcher(sub chan []*proto.Event, lock bool) {
	w.metricCli.EmitCounter("watcher_hub.delete_watcher", 1)
	if lock {
		w.Lock()
	}
	if _, ok := w.subs[sub]; ok {
		klog.InfoS("close event chan in watcher hub", "chan", sub)
		close(sub)
		delete(w.subs, sub)
	}
	if lock {
		w.Unlock()
	}
}

// Stream push events to watchers.
func (w *WatcherHub) Stream(input chan []*proto.Event) {
	for item := range input {
		w.RLock()
		for sub := range w.subs {
			select {
			case sub <- item:
			default:
				// drop slow consumer
				klog.InfoS("drop slow consumer", "chan", sub, "bufSize", watchBuffer)
				w.metricCli.EmitCounter("drop.slow.watcher", 1)
				go w.DeleteWatcher(sub, true)
			}
		}
		w.RUnlock()
	}

	w.Lock()
	klog.Info("[watcher hub] input channel from heap closed, delete all watchers")
	for sub := range w.subs {
		w.DeleteWatcher(sub, false)
	}
	w.Unlock()
}
