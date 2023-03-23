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

package endpoint

import (
	"context"
	"net/http"
	_ "net/http/pprof"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server"
)

// Endpoint is the endpoint for serving
type Endpoint struct {
	metrics metrics.Metrics
	backend backend.Backend
	server  server.Server
	config  *Config
}

// NewEndpoint returns an Endpoint for serving
func NewEndpoint(b backend.Backend, m metrics.Metrics, config *Config) *Endpoint {
	klog.InfoS("new endpoints", "config", config)
	return &Endpoint{
		metrics: m,
		backend: b,
		config:  config,
	}
}

// Run start grpc and http server, and block until there is any error
//
//	each port may start secure server and insecure server
//	ClientPort─┬─ Secure(TLS) ─┬─ GRPC
//	           │               └─ HTTP
//	           │
//	           └─  Insecure   ─┬─ GRPC
//	                           └─ HTTP
//
//	PeerPort  ─┬─ Secure(TLS) ─┬─ GRPC
//	           │               └─ HTTP
//	           │
//	           └─  Insecure   ─┬─ GRPC
//	                           └─ HTTP
//	InfoPort  ───  Insecure   ─── HTTP
func (e *Endpoint) Run(ctx context.Context) (err error) {

	e.server = server.NewServer(e.backend, e.metrics, e.config.getServerConfig())

	// start exposed server
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		klog.ErrorS(err, "shutdown")
	}()

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		defer cancel()
		return e.runClientServer(ctx)
	})

	group.Go(func() error {
		defer cancel()
		return e.runPeerServer(ctx)
	})

	if e.config.InfoPort != 0 {
		group.Go(func() error {
			defer cancel()
			return e.runMetricsServer(ctx)
		})

	}

	// block until all server exit
	return group.Wait()
}

func (e *Endpoint) buildExposedServers(sc *SecurityConfig, servers ...exposedServer) (ret []exposedServer) {
	mode := sc.mode()
	klog.InfoS("build exposed servers", "mode", mode)
	switch mode {
	case modeOnlyInsecure:
		return servers
	case modeOnlySecure:
		return []exposedServer{newSecureServer(sc, servers...)}
	case modeBothInsecureAndSecure:
		return append([]exposedServer{newSecureServer(sc, servers...)}, servers...)
	}
	return nil
}

func (e *Endpoint) runClientServer(ctx context.Context) error {
	clientHttp := e.buildClientHttpServer()
	clientGrpc := e.buildClientGrpcServer()
	exposedServers := e.buildExposedServers(e.config.ClientSecurityConfig, clientHttp, clientGrpc)
	clientServiceGroup := newRootServer(e.config.Port, exposedServers...)
	return clientServiceGroup.run(ctx)
}

func (e *Endpoint) runPeerServer(ctx context.Context) error {
	peerHttp := e.buildPeerHttpServer()
	peerGrpc := e.buildPeerGrpcServer()
	exposedServers := e.buildExposedServers(e.config.PeerSecurityConfig, peerHttp, peerGrpc)
	peerServiceGroup := newRootServer(e.config.PeerPort, exposedServers...)
	return peerServiceGroup.run(ctx)
}

func (e *Endpoint) runMetricsServer(ctx context.Context) error {
	metricsHttp := e.buildMetricsHttpServer()
	peerServiceGroup := newRootServer(e.config.InfoPort, metricsHttp)
	return peerServiceGroup.run(ctx)
}

func (e *Endpoint) buildClientHttpServer() exposedServer {
	handlersMaps := []map[string]http.Handler{
		e.metrics.GetHttpHandlers(),
		getPProfHandlers(),
		e.server.GetClientHttpHandlers(),
	}

	return newHttpServerWithHandlers(handlersMaps...)
}

func (e *Endpoint) buildPeerHttpServer() exposedServer {
	handlersMaps := []map[string]http.Handler{
		e.server.GetPeerHttpHandlers(),
	}

	return newHttpServerWithHandlers(handlersMaps...)
}

func (e *Endpoint) buildClientGrpcServer() exposedServer {
	grpcServer := grpc.NewServer(e.metrics.GetGrpcServerOption()...)
	e.server.RegisterClient(grpcServer)
	return newGrpcServer(grpcServer)
}

func (e *Endpoint) buildPeerGrpcServer() exposedServer {
	grpcServer := grpc.NewServer(e.metrics.GetGrpcServerOption()...)
	e.server.RegisterPeer(grpcServer)
	return newGrpcServer(grpcServer)
}

func (e *Endpoint) buildMetricsHttpServer() exposedServer {
	handlersMaps := []map[string]http.Handler{
		e.metrics.GetHttpHandlers(),
		getPProfHandlers(),
		e.server.GetInfoHttpHandlers(),
	}
	return newHttpServerWithHandlers(handlersMaps...)
}
