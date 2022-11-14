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
	"crypto/tls"
	"fmt"
	"net"

	"github.com/soheilhy/cmux"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

type secureExposedServer struct {
	exposedServer
}

func (s *secureExposedServer) name() string {
	return fmt.Sprintf("secure %v", s.exposedServer.name())
}

func newSecureExposedServers(ss []exposedServer) []exposedServer {
	ret := make([]exposedServer, 0, len(ss))
	for _, s := range ss {
		ret = append(ret, &secureExposedServer{exposedServer: s})
	}
	return ret
}

type secureServer struct {
	internalServers []exposedServer
	conf            *SecurityConfig
}

func newSecureServer(conf *SecurityConfig, ss ...exposedServer) exposedServer {
	return &secureServer{
		conf:            conf,
		internalServers: ss,
	}
}

func (t *secureServer) name() string {
	return "tls"
}

func (t *secureServer) matcher() cmux.Matcher {
	return cmux.TLS()
}

func (t *secureServer) serve(listener net.Listener) (err error) {

	tlsConf := t.conf.getServerTLSConfig()
	tlsListener := tls.NewListener(listener, tlsConf)
	mux := cmux.New(tlsListener)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		klog.ErrorS(err, "tls server shutdown")
	}()

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer cancel()
		return mux.Serve()
	})

	group.Go(func() error {
		defer cancel()
		return runServers(ctx, mux, newSecureExposedServers(t.internalServers))
	})

	return group.Wait()
}

func (t *secureServer) close() error {
	for _, server := range t.internalServers {
		err := server.close()
		if err != nil {
			klog.ErrorS(err, "tls internal server close err", "server", server.name())
		}
	}
	return nil
}
