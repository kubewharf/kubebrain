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

package brain

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

func (s *Server) Watch(r *proto.WatchRequest, server proto.Watch_WatchServer) error {
	if len(r.Key) == 0 {
		return fmt.Errorf("invalid empty key in watch request ")
	}
	if !s.peers.IsLeader() {
		s.metricCli.EmitCounter("brain.watch.follower", 1)
		return status.Errorf(codes.Unavailable, "txn error addr is %s leader %s", s.backend.GetResourceLock().Identity(), s.backend.GetResourceLock().Describe())
	}
	s.emitMethodMetric(watchMetric, "watch-start", nil, 0)

	start := time.Now()

	ctx, cancel := context.WithCancel(server.Context())
	defer cancel()

	ch, err := s.backend.Watch(ctx, string(r.Key), r.Revision)
	if err != nil {
		s.emitMethodMetric(watchMetric, "watch-end", err, time.Since(start))
		klog.ErrorS(err, "brain server watch backend failed", "key", string(r.Key), "revision", r.Revision)
		return err
	}
	responseSize := 0
	var sendErr error
	for events := range ch {
		if sendErr != nil {
			// drain the channel to ensure producer could exit
			continue
		}

		s.metricCli.EmitCounter("brain.watch.event", len(events))
		watchResponse := &proto.WatchResponse{
			Header: &proto.ResponseHeader{
				Revision: r.Revision,
			},
			Events: events,
		}
		responseSize += watchResponse.Size()
		sendErr = server.Send(watchResponse)
		if sendErr != nil {
			s.emitMethodMetric(watchMetric, "watch-send", sendErr, 0)
			klog.ErrorS(sendErr, "send watch response to client", "key", string(r.Key), "end", string(r.End), "revision", r.Revision)
			cancel()
		}
	}
	s.emitMethodMetric(watchMetric, "watch-end", nil, time.Since(start))
	s.emitResponseDetailMetric(watchMetric, "watch", true, responseSize)
	klog.InfoS("brain server watch finished", "key", string(r.Key), "end", string(r.End), "revision", r.Revision)
	return nil
}
