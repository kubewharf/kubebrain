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

package server

import (
	"context"
	"encoding/json"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/brain"
	"github.com/kubewharf/kubebrain/pkg/server/etcd"
	"github.com/kubewharf/kubebrain/pkg/server/service"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/server/service/revision"
)

// Server is the application layer server providing services for clients and peers
type Server interface {
	// RegisterClient registers grpc service for clients
	RegisterClient(server *grpc.Server)

	// RegisterPeer registers grpc service for peer
	RegisterPeer(server *grpc.Server)

	// GetClientHttpHandlers returns http handlers for client
	GetClientHttpHandlers() map[string]http.Handler

	// GetPeerHttpHandlers returns http handlers for peer
	GetPeerHttpHandlers() map[string]http.Handler

	// GetInfoHttpHandlers returns http handlers for node info
	GetInfoHttpHandlers() map[string]http.Handler
}

type server struct {
	// etcd protocol grpc server
	etcdServer *etcd.RPCServer
	// kube brain protocol grpc server
	brainServer *brain.Server
	// health server to tell client whether this instance is leader
	healthServer *health.Server

	leaderElection leader.LeaderElection
	metricCli      metrics.Metrics
	backend        backend.Backend

	config Config
}

// NewServer returns the server
func NewServer(backend backend.Backend, metricCli metrics.Metrics, config Config) Server {
	// health server to tell client whether this instance is leader
	healthServer := health.NewServer()
	// leader election call back
	election := leader.NewLeaderElection(backend, metricCli, func(context.Context) {
		healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	}, func() {
		healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	})
	// revisionSyncer sync revision from leader to follower
	peerService := service.NewPeerService(election, metricCli, backend, config.getPeerServiceConfig())
	// construct etcd & brian grpc server
	etcdServer := etcd.New(backend, metricCli, peerService)
	brainServer := brain.New(backend, metricCli, peerService)
	return &server{
		etcdServer:     etcdServer,
		brainServer:    brainServer,
		healthServer:   healthServer,
		leaderElection: election,
		metricCli:      metricCli,
		backend:        backend,
		config:         config,
	}
}

// RegisterClient implements Server interface
func (s *server) RegisterClient(server *grpc.Server) {
	s.register(server)
}

// RegisterPeer implement Server interface
func (s *server) RegisterPeer(server *grpc.Server) {
	s.register(server)
}

func (s *server) register(server *grpc.Server) {
	// register grpc method to grpc server
	s.etcdServer.Register(server)
	s.brainServer.Register(server)
	// set NOT_SERVEING when initialized
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(server, s.healthServer)
}

// GetClientHttpHandlers implements Server interface
func (s *server) GetClientHttpHandlers() map[string]http.Handler {
	return map[string]http.Handler{
		"/health": http.HandlerFunc(s.httpHealthHandler),
	}
}

// GetPeerHttpHandlers implements Server interface
func (s *server) GetPeerHttpHandlers() map[string]http.Handler {
	return map[string]http.Handler{
		"/status": http.HandlerFunc(s.revisionHandler),
	}
}

// GetInfoHttpHandlers implements Server interface
func (s *server) GetInfoHttpHandlers() map[string]http.Handler {
	return map[string]http.Handler{
		"/health":   http.HandlerFunc(s.httpHealthHandler),
		"/status":   http.HandlerFunc(s.revisionHandler),
		"/election": http.HandlerFunc(s.electionHandler),
	}
}

func (s *server) electionHandler(w http.ResponseWriter, req *http.Request) {
	info, err := s.leaderElection.GetElectionInfo()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	respBytes, _ := json.Marshal(info)
	w.Write(respBytes)
	return
}

func (s *server) revisionHandler(w http.ResponseWriter, req *http.Request) {
	if !s.leaderElection.IsLeader() {
		s.metricCli.EmitCounter("leader.invalid", 1)
		w.WriteHeader(400)
		w.Write([]byte("i'm not leader, so can't tell you revision"))
		return
	}
	rev := s.backend.GetCurrentRevision()
	w.WriteHeader(200)
	responseBody, _ := json.Marshal(&revision.LeaderRevision{
		Revision: rev,
	})
	s.metricCli.EmitGauge("leader.revision", rev)
	w.Write(responseBody)
}

const (
	HealthResponse = `{"health":"true"}`
)

func (s *server) httpHealthHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		klog.Warningf("/health error (status code %d)", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(HealthResponse))
}
