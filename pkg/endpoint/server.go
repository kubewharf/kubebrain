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
	"fmt"
	"net"
	"net/http"

	"github.com/soheilhy/cmux"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/util"
)

type exposedServer interface {
	name() string
	matcher() cmux.Matcher
	serve(listener net.Listener) error
	close() error
}

func newHttpServerWithHandlers(handlersMaps ...map[string]http.Handler) exposedServer {
	mux := http.NewServeMux()
	for _, handlersMap := range handlersMaps {
		for pattern, handler := range handlersMap {
			mux.Handle(pattern, handler)
		}
	}
	return newHttpServer(mux)
}

func newHttpServer(handler http.Handler) exposedServer {
	svr := &http.Server{
		Handler: handler,
	}
	return &httpServer{
		svr: svr,
	}
}

type httpServer struct {
	svr *http.Server
}

func (h *httpServer) name() string {
	return "http"
}

func (h *httpServer) matcher() cmux.Matcher {
	return cmux.HTTP1()
}

func (h *httpServer) serve(listener net.Listener) error {
	return h.svr.Serve(listener)
}

func (h *httpServer) close() error {
	return h.svr.Close()
}

func newGrpcServer(svr *grpc.Server) exposedServer {
	return &grpcServer{Server: svr}
}

type grpcServer struct {
	*grpc.Server
}

func (g *grpcServer) name() string {
	return "grpc"
}

func (g *grpcServer) matcher() cmux.Matcher {
	return cmux.HTTP2()
}

func (g *grpcServer) serve(listener net.Listener) error {
	return g.Serve(listener)
}

func (g *grpcServer) close() error {
	g.Stop()
	return nil
}

type rootServer struct {
	port     int
	services []exposedServer
}

func newRootServer(port int, ss ...exposedServer) *rootServer {
	return &rootServer{
		port:     port,
		services: ss,
	}
}

func (gs *rootServer) run(ctx context.Context) (err error) {
	defer func() {
		klog.ErrorS(err, "root servers shutdown", "port", gs.port)
	}()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", gs.port))
	if err != nil {
		return err
	}
	defer listener.Close()

	mux := cmux.New(listener)
	go func() {
		defer util.Recover()
		klog.InfoS("root server start to listen", "port", gs.port)
		err = mux.Serve()
		klog.ErrorS(err, "root server shutdown cause by temporary network error", "port", gs.port)
	}()

	return runServers(ctx, mux, gs.services)
}

// runServers run servers concurrently and shutdown all servers if anyone is error
func runServers(ctx context.Context, mux cmux.CMux, servers []exposedServer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	for _, server := range servers {

		// ! NOTICE: `mux.Match` is not thread-safe, so it should be called serially.
		lsn := mux.Match(server.matcher())

		runner := runSubServer(ctx, lsn, server)
		group.Go(func() (err error) {
			// if any sub exposedServer exit, call `cancel` to make other sub exposedServer exit
			defer cancel()
			return runner()
		})
	}

	// block until all sub exposedServer exit and return the first error if exist
	return group.Wait()
}

func runSubServer(ctx context.Context, lsn net.Listener, server exposedServer) func() error {
	return func() (err error) {

		closed := make(chan error, 1)
		defer func() {
			_ = server.close()
			_ = lsn.Close()
			// wait until closed
			<-closed
		}()

		// run server in a new goroutine
		go func() {
			defer util.Recover()

			// run until server is closed or has an internal error
			klog.InfoS("run server", "name", server.name(), "addr", lsn.Addr())
			if err = server.serve(lsn); err != nil {
				klog.ErrorS(err, "exposed server stop", "name", server.name(), "addr", lsn.Addr())
				closed <- err
			}
			close(closed)
		}()

		// block until exposedServer stop or context done
		return waitFor(ctx, closed)
	}

}

func waitFor(ctx context.Context, closed chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-closed:
		return err
	}
}
