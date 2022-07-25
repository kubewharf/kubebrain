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
	"strconv"
	"time"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	b "github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service"
)

const (
	writeMetric = "brain.server.write"
	readMetric  = "brain.server.read"
	watchMetric = "brain.server.watch"
)

// Server is the KubeWharf grpc api server
type Server struct {
	backend   b.Backend
	metricCli metrics.Metrics
	peers     service.PeerService
}

func New(backend b.Backend, metricCli metrics.Metrics, peerServer service.PeerService) *Server {
	server := &Server{
		backend:   backend,
		metricCli: metricCli,
		peers:     peerServer,
	}
	go server.peers.Campaign()
	go server.compactLoop()
	return server
}

// Register register kube-wharf rpc protocol server
func (s *Server) Register(server *grpc.Server) {
	proto.RegisterReadServer(server, s)
	proto.RegisterWriteServer(server, s)
	proto.RegisterWatchServer(server, s)
}

// compactLoop compacts background
func (s *Server) compactLoop() {
	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()
	for {
		<-ticker.C
		klog.Info("begin to check compact")
		if s.peers.IsLeader() {
			klog.Info("leader start to compact")
			compactRevision := s.backend.GetCurrentRevision() - 1000
			startTime := time.Now()
			s.backend.Compact(context.Background(), compactRevision)
			s.metricCli.EmitGauge("leader.compact", compactRevision)
			s.metricCli.EmitHistogram("leader.compact.latency", time.Since(startTime).Seconds())
		}
	}
}

// emit metric and latency
func (s *Server) emitMethodMetric(metricName string, method string, err error, latency time.Duration) {
	methodTag := metrics.Tag("method", method)
	successTag := metrics.Tag("err", strconv.FormatBool(err != nil))
	s.metricCli.EmitCounter(metricName, 1, methodTag, successTag)
	s.metricCli.EmitHistogram(metricName+".latency", latency.Milliseconds(), methodTag, successTag)
}

// emit response metric including response size/ success flag
func (s *Server) emitResponseDetailMetric(metricName string, method string, success bool, responseSize int) {
	methodTag := metrics.Tag("method", method)
	if !success {
		s.metricCli.EmitCounter(metricName+".fail", 1, methodTag)
	}
	s.metricCli.EmitHistogram(metricName+".responsesize", responseSize, methodTag)
}
