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

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

func (s *Server) Get(ctx context.Context, r *proto.GetRequest) (*proto.GetResponse, error) {
	if len(r.Key) == 0 {
		return nil, fmt.Errorf("invailid empty key in get request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(); err != nil {
		return &proto.GetResponse{}, err
	}
	response, err := s.backend.Get(ctx, r)
	if err != nil {
		klog.ErrorS(err, "brain server get failed", "key", string(r.Key), "revision", r.Revision)
	}
	// emit metrics
	s.emitMethodMetric(readMetric, "get", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(readMetric, "get", response.Kv != nil, response.Size())
	}
	return response, err
}

func (s *Server) Range(ctx context.Context, r *proto.RangeRequest) (*proto.RangeResponse, error) {
	if len(r.Key) == 0 || len(r.End) == 0 {
		return nil, fmt.Errorf("empty key or end in range request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(); err != nil {
		return &proto.RangeResponse{}, err
	}
	response, err := s.backend.List(ctx, r)
	if err != nil {
		klog.ErrorS(err, "brain server range failed", "key", string(r.Key), "end", string(r.End), "limit", r.Limit, "revision", r.Revision)
	}
	// emit metrics
	s.emitMethodMetric(readMetric, "range", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(readMetric, "range", true, response.Size())
	}
	return response, err
}

func (s *Server) Count(ctx context.Context, r *proto.CountRequest) (*proto.CountResponse, error) {
	if len(r.Key) == 0 || len(r.End) == 0 {
		return nil, fmt.Errorf("empty key or end in count request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(); err != nil {
		return &proto.CountResponse{}, err
	}
	response, err := s.backend.Count(ctx, r)
	if err != nil {
		klog.ErrorS(err, "brain server count failed", "key", string(r.Key), "end", string(r.End))
	}
	// emit metrics
	s.emitMethodMetric(readMetric, "count", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(readMetric, "count", true, response.Size())
	}
	return response, err
}

func (s *Server) ListPartition(ctx context.Context, r *proto.ListPartitionRequest) (*proto.ListPartitionResponse, error) {
	if len(r.Key) == 0 || len(r.End) == 0 {
		return nil, fmt.Errorf("empty key or end in list partition request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(); err != nil {
		return &proto.ListPartitionResponse{}, err
	}
	response, err := s.backend.GetPartitions(ctx, r)
	if err != nil {
		klog.ErrorS(err, "brain server list-partition failed", "key", string(r.Key), "end", string(r.End))
	}
	s.emitMethodMetric(readMetric, "list-partition", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(readMetric, "list-partition", true, response.Size())
	}
	return response, err
}

func (s *Server) RangeStream(r *proto.RangeRequest, server proto.Read_RangeStreamServer) error {
	if len(r.Key) == 0 || len(r.End) == 0 {
		return fmt.Errorf("empty key or end in range stream request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(); err != nil {
		return err
	}
	ch, err := s.backend.ListByStream(server.Context(), r.Key, r.End, r.Revision)
	if err != nil {
		s.emitMethodMetric(readMetric, "range-stream", err, time.Since(start))
		klog.ErrorS(err, "backend list by stream failed", "key", r.Key, "end", r.End, "revision", r.Revision)
		return err
	}
	responseSize := 0
	for response := range ch {
		responseSize += response.Size()
		err = server.Send(response)
		if err != nil {
			s.emitMethodMetric(readMetric, "range-stream-send", err, time.Since(start))
			klog.ErrorS(err, "send range stream response to client failed", "key", r.Key, "end", r.End, "revision", r.Revision)
			return err
		}
	}
	s.emitMethodMetric(readMetric, "range-stream", nil, time.Since(start))
	s.emitResponseDetailMetric(readMetric, "range-stream", true, responseSize)
	klog.InfoS("brain server range stream finished", "error", err, "key", string(r.Key), "end", string(r.End))
	return nil
}
