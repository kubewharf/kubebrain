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

func (s *RPCServer) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	s.metricCli.EmitCounter("lease.grant", 1)
	return &etcdserverpb.LeaseGrantResponse{
		Header: &etcdserverpb.ResponseHeader{},
		ID:     req.TTL,
		TTL:    req.TTL,
	}, nil
}

func (s *RPCServer) LeaseRevoke(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	s.metricCli.EmitCounter("lease.revoke", 1)
	return nil, fmt.Errorf("lease revoke is not supported")
}

func (s *RPCServer) LeaseKeepAlive(etcdserverpb.Lease_LeaseKeepAliveServer) error {
	s.metricCli.EmitCounter("lease.keepalive", 1)
	return fmt.Errorf("lease keep alive is not supported")
}

func (s *RPCServer) LeaseTimeToLive(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	s.metricCli.EmitCounter("lease.ttl", 1)
	return nil, fmt.Errorf("lease time to live is not supported")
}

func (s *RPCServer) LeaseLeases(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	s.metricCli.EmitCounter("lease.leases", 1)
	return nil, fmt.Errorf("lease leases is not supported")
}
