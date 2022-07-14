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

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// MemberList lists the current cluster membership.
func (s *RPCServer) MemberList(context.Context, *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	s.metricCli.EmitCounter("member.list", 1)
	return &etcdserverpb.MemberListResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: 0,
		},
	}, nil
}

// MemberAdd adds a member into the cluster.
func (s *RPCServer) MemberAdd(context.Context, *etcdserverpb.MemberAddRequest) (*etcdserverpb.MemberAddResponse, error) {
	return nil, fmt.Errorf("member Add is not supported")
}

// MemberRemove removes an existing member from the cluster.
func (s *RPCServer) MemberRemove(context.Context, *etcdserverpb.MemberRemoveRequest) (*etcdserverpb.MemberRemoveResponse, error) {
	return nil, fmt.Errorf("member remove is not supported")
}

// MemberUpdate updates the peer addresses of the member.
func (s *RPCServer) MemberUpdate(context.Context, *etcdserverpb.MemberUpdateRequest) (*etcdserverpb.MemberUpdateResponse, error) {
	return nil, fmt.Errorf("member update is not supported")
}

// MemberPromote promotes a member from raft learner (non-voting) to raft voting member.
func (s *RPCServer) MemberPromote(context.Context, *etcdserverpb.MemberPromoteRequest) (*etcdserverpb.MemberPromoteResponse, error) {
	return nil, fmt.Errorf("member promote is not supported")
}
