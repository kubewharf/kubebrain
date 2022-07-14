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

package etcd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

var (
	// one watcher is one grpc stream
	watcherID int64
)

// watcher correspond to one stream, one watcher has many watches
type watcher struct {
	sync.Mutex

	wg      sync.WaitGroup
	backend BackendShim
	// stream server
	watchServer etcdserverpb.Watch_WatchServer
	// gRPC server
	grpcServer *RPCServer

	// hold watch request info in this stream
	watches map[int64]*watch
	id      int64

	metricCli metrics.Metrics
}

var (
	// one watch is one watch request
	watchID int64
)

// watch correspond to one watch request
type watch struct {
	cancel     func()
	start, end string
}

func (s *RPCServer) Watch(ws etcdserverpb.Watch_WatchServer) error {
	w := &watcher{
		watchServer: ws,
		grpcServer:  s,
		backend:     s.backend,
		watches:     make(map[int64]*watch),
		metricCli:   s.metricCli,
	}
	w.id = atomic.AddInt64(&watcherID, 1)
	klog.InfoS("new watcher", "id", w.id)
	defer func() {
		w.Close()
	}()

	for {
		msg, err := ws.Recv()
		if err != nil {
			// log watch infos on stream when stream err occurs
			w.Lock()
			watchInfo := make([]string, 0, len(w.watches))
			for watchID, i := range w.watches {
				watchInfo = append(watchInfo, fmt.Sprintf("watch id: %d start: %s end: %s", watchID, i.start, i.end))
			}
			w.Unlock()
			s.metricCli.EmitCounter("watcher.receive.cancel", 1)
			klog.ErrorS(err, "watcher receive err", "id", w.id, "info", strings.Join(watchInfo, "\n"))
			return err
		}

		if r := msg.GetCreateRequest(); r != nil {
			// normal watch request can only be handled by leader
			// magic logic: when StartRevision < 0, the request is a RangeStream Request.
			if r.StartRevision >= 0 && isPureWatchRequest(r) && !s.peers.IsLeader() && !s.peers.EtcdProxyEnabled() {
				s.metricCli.EmitCounter("watch.follower", 1)
				leaderInfo := s.peers.GetLeaderInfo()
				klog.InfoS("watch follower", "revision", r.StartRevision, "addr", s.backend.GetResourceLock().Identity(), "leader", leaderInfo)
				return status.Errorf(codes.Unavailable, "watch error addr is %s leader %s", s.backend.GetResourceLock().Identity(), leaderInfo)
			}

			w.Start(ws.Context(), r)
		} else if cancelRequest := msg.GetCancelRequest(); cancelRequest != nil {
			s.metricCli.EmitCounter("watch.client.cancel", 1)
			klog.InfoS("receive watch cancel request", "id", w.id, "watchID", cancelRequest.GetWatchId())
			w.Cancel(msg.GetCancelRequest().WatchId, nil, false)
		} else {
			s.metricCli.EmitCounter("watch.request.unsupported", 1)
			klog.Info("watch receive message unsupported type")
		}
	}
}

func (w *watcher) Start(c context.Context, r *etcdserverpb.WatchCreateRequest) {
	w.Lock()
	ctx, cancel := context.WithCancel(c)
	id := atomic.AddInt64(&watchID, 1)

	w.watches[id] = &watch{
		cancel: cancel,
		start:  string(r.Key),
		end:    string(r.RangeEnd),
	}
	if len(w.watches) > 1 {
		klog.InfoS("watcher reuse", "id", w.id, "size", len(w.watches))
	}
	w.Unlock()
	w.metricCli.EmitGauge("watch.watch_id", id)

	if err := w.watchServer.Send(&etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{},
		Created: true,
		WatchId: id,
	}); err != nil {
		klog.ErrorS(err, "watch send create watch response err", "wacher", w.id, "watch", id)
		w.Cancel(id, err, false)
		return
	}

	w.wg.Add(1)
	key := string(r.Key)

	// use watch to simulate list
	if r.StartRevision < 0 {
		w.metricCli.EmitCounter("watch.range", 1)
		go w.List(ctx, id, r)
	} else {
		w.metricCli.EmitCounter("watch.watch", 1)
		go w.Watch(ctx, id, r)
		klog.InfoS("watch start", "id", id, "count", len(w.watches), "key", key, "revision", r.StartRevision)
	}
}

func (w *watcher) Cancel(id int64, err error, compact bool) {
	klog.InfoS("watch cancel", "watcher", w.id, "watch", id, "err", err, "compact", compact)
	var tags []metrics.T
	if compact {
		tags = append(tags, metrics.Tag("compact", "true"))
	}
	w.metricCli.EmitCounter("watch.cancel", 1, tags...)
	w.Lock()
	if c, ok := w.watches[id]; ok {
		klog.InfoS("cancel context", "watcher", w.id, "watch", id, "start", c.start, "end", c.end)
		if c.cancel != nil {
			c.cancel()
		}
		delete(w.watches, id)
	}
	w.Unlock()
	// if compact is true, apiserver reflector watch will return with err, which will trigger re-list & re-watch (detail in etcd/clientv3/watch.go watchGrpcStream.run)
	// else, apiserver reflector watch will return nil, which will trigger re-watch
	var compactRevision int64
	if compact {
		compactRevision = 1
	}
	serr := w.watchServer.Send(&etcdserverpb.WatchResponse{
		Header:          &etcdserverpb.ResponseHeader{},
		Canceled:        true,
		CancelReason:    "watch closed",
		WatchId:         id,
		CompactRevision: compactRevision,
	})
	if serr != nil {
		klog.ErrorS(serr, "failed to send cancel response", "watcher", w.id, "watch", id)
	}
}

func (w *watcher) Close() {
	w.metricCli.EmitCounter("watch.close", 1)
	w.Lock()
	for _, v := range w.watches {
		if v.cancel != nil {
			v.cancel()
		}
	}
	w.Unlock()
	w.wg.Wait()
}

func (w *watcher) List(ctx context.Context, id int64, r *etcdserverpb.WatchCreateRequest) {
	defer w.wg.Done()
	if err := w.grpcServer.peers.SyncReadRevision(); err != nil {
		w.Cancel(id, err, true)
		return
	}
	startTime := time.Now()
	klog.InfoS("RANGE STREAM", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "rev", r.StartRevision)
	ch, err := w.backend.ListByStream(ctx, r.Key, r.RangeEnd, uint64(r.StartRevision*-1))
	if err != nil {
		klog.ErrorS(err, "list by stream failed", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "rev", int64(r.StartRevision*-1))
		w.metricCli.EmitCounter("watch.backend.list_stream.err", 1)
		w.Cancel(id, err, true)
		return
	}

	for watchResponse := range ch {
		revision := int64(r.StartRevision * -1)
		// range stream eof, tell client range ends
		// if has err, CancelReason is not nil
		if watchResponse.Canceled == true {
			klog.InfoS("receive cancel message", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, int64(r.StartRevision*-1))
			// indicates eof
			revision = -1
			// set err info in events
			watchResponse.Events = []*mvccpb.Event{
				{
					Kv: &mvccpb.KeyValue{
						Key:         []byte("eof"),
						Value:       []byte(watchResponse.CancelReason),
						ModRevision: GetPartitionMagic,
					},
				},
			}
			w.metricCli.EmitCounter("watch.list_stream.eof", 1)
			w.metricCli.EmitHistogram("watch.list_stream.latency", time.Now().Sub(startTime).Seconds())
		}

		response := &etcdserverpb.WatchResponse{
			Header: &etcdserverpb.ResponseHeader{
				Revision: revision,
			},
			WatchId: id,
			Events:  watchResponse.Events,
		}
		w.metricCli.EmitCounter("watch.list_stream.push", len(response.Events))
		w.metricCli.EmitHistogram("watch.list_stream.push.size", response.Size())
		if err := w.watchServer.Send(response); err != nil {
			klog.ErrorS(err, "[range stream] send response with header failed", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "rev", int64(r.StartRevision*-1), "respRev", revision)
			w.metricCli.EmitCounter("watch.list_stream.push.err", 1)
			// send failed, should break
			// TODO retry refer to etcd victims
			w.Cancel(id, err, true)
			continue
		}
	}
	// for range stream, don't send cancel in watchServer, but wait client to cancel. to prevent message receive order confusion
	// delete watch info in watches map to avoid resource leak
	w.Lock()
	if c, ok := w.watches[id]; ok {
		klog.InfoS("[range stream] begin to cancel context", "watcher", w.id, "watch", id)
		if c.cancel != nil {
			c.cancel()
		}
		delete(w.watches, id)
	}
	w.Unlock()
	klog.InfoS("[range stream] range closed", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd)
}

func (w *watcher) Watch(ctx context.Context, id int64, r *etcdserverpb.WatchCreateRequest) {
	defer w.wg.Done()
	// when connection error, in etcd client v3 newWatchClient function,
	// range stream retry with newest revision may fall into this logic, which results
	// range stream hang

	// watchServer send message(including normal message and eof) has err transport closing, so the client can't receive cancel message
	// it will get connection err and trigger newWatchClient, newWatchClient will retry all not completed substreams with latest event revision.
	// once come into watch, it either hangs or come up with resource version too old err
	if !isPureWatchRequest(r) {
		w.Cancel(id, fmt.Errorf("invliad watch %s revision %d", r.Key, r.StartRevision), true)
		w.metricCli.EmitCounter("invalid.watch.key", 1)
		return
	}

	var ch <-chan []*mvccpb.Event
	var err error

	if w.grpcServer.peers.IsLeader() {
		ch, err = w.backend.Watch(ctx, string(r.Key), uint64(r.StartRevision))
	} else {
		ch, err = w.grpcServer.peers.Watch(ctx, string(r.Key), uint64(r.StartRevision))
	}
	klog.InfoS("[watch stream] watch", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "rev", r.StartRevision)
	if err != nil {
		w.metricCli.EmitCounter("watch.backend.err", 1)
		klog.ErrorS(err, "[watch stream] cancel due to backend watch err", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "rev", r.StartRevision)
		w.Cancel(id, err, true)
		return
	}
	for events := range ch {
		if len(events) == 0 {
			continue
		}
		watchResponse := &etcdserverpb.WatchResponse{
			Header: &etcdserverpb.ResponseHeader{
				Revision: events[len(events)-1].Kv.ModRevision,
			},
			WatchId: id,
			Events:  events,
		}
		w.metricCli.EmitGauge("watch.watch_stream.push", watchResponse.Header.Revision)
		w.metricCli.EmitHistogram("watch.watch_stream.push.size", watchResponse.Size())
		if err := w.watchServer.Send(watchResponse); err != nil {
			w.metricCli.EmitCounter("watch.watch_stream.push.err", 1)
			klog.ErrorS(err, "[watch stream] watch send err, cancel", "watcher", w.id, "watch", id)
			w.Cancel(id, err, false)
			continue
		}
	}
	klog.ErrorS(err, "[watch stream] watch to be canceled", "watcher", w.id, "watch", id, "key", string(r.Key))
	w.Cancel(id, nil, false)
	klog.InfoS("[watch stream] watch canceled", "watcher", w.id, "watch", id, "key", string(r.Key))
}

func isPureWatchRequest(r *etcdserverpb.WatchCreateRequest) bool {
	// if starts with /, it is a normal watch request, not a range stream request
	if strings.HasPrefix(string(r.Key), "/") {
		return true
	}
	return false
}
