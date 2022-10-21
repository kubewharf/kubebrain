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

const unaryRpcTimeout = time.Second

func (s *Server) Create(ctx context.Context, createRequest *proto.CreateRequest) (*proto.CreateResponse, error) {
	if len(createRequest.Key) == 0 || len(createRequest.Value) == 0 {
		return nil, fmt.Errorf("empty key or value in create request")
	}
	start := time.Now()
	deadline, ok := ctx.Deadline()
	if ok && start.Sub(deadline) >= 0 {
		return nil, context.DeadlineExceeded
	}
	ctx, cancel := context.WithTimeout(context.Background(), unaryRpcTimeout)
	defer cancel()

	if err := s.checkLeaderWrite(); err != nil {
		s.metricCli.EmitCounter("brain.write.follower", 1)
		return nil, err
	}
	response, err := s.backend.Create(ctx, createRequest)
	if err != nil {
		klog.ErrorS(err, "brain server create failed", "key", string(createRequest.Key), "lease", createRequest.Lease)
	}
	s.emitMethodMetric(writeMetric, "create", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(writeMetric, "create", response.Succeeded, response.Size())
	}
	return response, err
}

func (s *Server) Update(ctx context.Context, updateRequest *proto.UpdateRequest) (*proto.UpdateResponse, error) {
	if updateRequest.Kv == nil || len(updateRequest.Kv.Key) == 0 || len(updateRequest.Kv.Value) == 0 {
		return nil, fmt.Errorf("kv in updateRequest is nil, or empty key or value in update request kv")
	}
	start := time.Now()
	deadline, ok := ctx.Deadline()
	if ok && start.Sub(deadline) >= 0 {
		return nil, context.DeadlineExceeded
	}
	ctx, cancel := context.WithTimeout(context.Background(), unaryRpcTimeout)
	defer cancel()

	if err := s.checkLeaderWrite(); err != nil {
		s.metricCli.EmitCounter("brain.write.follower", 1)
		return nil, err
	}
	response, err := s.backend.Update(ctx, updateRequest)
	if err != nil {
		klog.ErrorS(err, "brain server update failed", "key", string(updateRequest.Kv.Key), "revision", updateRequest.Kv.Revision)
	}
	s.emitMethodMetric(writeMetric, "update", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(writeMetric, "update", response.Succeeded, response.Size())
	}
	return response, err
}

func (s *Server) Delete(ctx context.Context, deleteRequest *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	if len(deleteRequest.Key) == 0 {
		return nil, fmt.Errorf("empty key in deleteRequest")
	}
	start := time.Now()
	deadline, ok := ctx.Deadline()
	if ok && start.Sub(deadline) >= 0 {
		return nil, context.DeadlineExceeded
	}
	ctx, cancel := context.WithTimeout(context.Background(), unaryRpcTimeout)
	defer cancel()

	if err := s.checkLeaderWrite(); err != nil {
		s.metricCli.EmitCounter("brain.write.follower", 1)
		return nil, err
	}
	response, err := s.backend.Delete(ctx, deleteRequest)
	if err != nil {
		klog.ErrorS(err, "brain server delete failed", "key", string(deleteRequest.Key), "revision", deleteRequest.Revision)
	}
	s.emitMethodMetric(writeMetric, "delete", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(writeMetric, "delete", response.Succeeded, response.Size())
	}
	return response, err
}

func (s *Server) Compact(ctx context.Context, compactRequest *proto.CompactRequest) (*proto.CompactResponse, error) {
	if compactRequest.Revision <= 0 {
		return nil, fmt.Errorf("invalid revision in compact request")
	}
	start := time.Now()
	if err := s.checkLeaderWrite(); err != nil {
		s.metricCli.EmitCounter("brain.write.follower", 1)
		return nil, err
	}
	response, err := s.backend.Compact(ctx, compactRequest.Revision)
	if err != nil {
		klog.ErrorS(err, "brain server compact failed", "revision", compactRequest.Revision)
	}
	s.emitMethodMetric(writeMetric, "compact", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(writeMetric, "compact", true, response.Size())
	}
	return response, err
}

// validate whether this instance should serve write request
func (s *Server) checkLeaderWrite() error {
	// only leader can accept and handle write request
	// return error includes current leader, help etcd client send request to right instance
	if !s.peers.IsLeader() {
		s.metricCli.EmitCounter("brain.server.write.follower", 1)
		klog.Infof("brain server write to follower")
		return status.Errorf(codes.Unavailable, "txn error addr is %s leader %s", s.backend.GetResourceLock().Identity(), s.backend.GetResourceLock().Describe())
	}
	return nil
}
