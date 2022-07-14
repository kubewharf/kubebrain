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
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"

	b "github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service"
)

var (
	_ etcdserverpb.KVServer    = (*RPCServer)(nil)
	_ etcdserverpb.WatchServer = (*RPCServer)(nil)
)

// RPCServer only support limited method of etcd grpc server
type RPCServer struct {
	backend BackendShim

	// watcher map mutes
	sync.Mutex

	metricCli metrics.Metrics
	peers     service.PeerService
}

// New returns the etcd rpc server
func New(backend b.Backend, metricCli metrics.Metrics, peers service.PeerService) *RPCServer {
	server := &RPCServer{
		backend:   NewBackendShim(backend, metricCli),
		metricCli: metricCli,
		peers:     peers,
	}
	return server
}

// Register register etcd grpc service
func (s *RPCServer) Register(server *grpc.Server) {
	etcdserverpb.RegisterLeaseServer(server, s)
	etcdserverpb.RegisterWatchServer(server, s)
	etcdserverpb.RegisterKVServer(server, s)
	etcdserverpb.RegisterClusterServer(server, s)
}
