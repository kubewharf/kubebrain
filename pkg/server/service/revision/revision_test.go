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

package revision

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/assert"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/storage"
	ibadger "github.com/kubewharf/kubebrain/pkg/storage/badger"
	"github.com/kubewharf/kubebrain/pkg/util/auth"
)

func newBadgerStorage(t *testing.T, ast *assert.Assertions) storage.KvStorage {
	p := path.Join(t.TempDir(), "badger")
	st, err := ibadger.NewKvStorage(ibadger.Config{Dir: p})
	if err != nil {
		ast.FailNow(err.Error())
	}
	return st
}

type testcase struct {
	name            string
	retryTimes      int
	expectError     error
	respWaitTime    time.Duration
	enableCmux      bool
	clientTlsConfig *tls.Config
	serverTlsConfig *tls.Config
}

type backendStub struct {
	currentRev uint64
}

func (b *backendStub) SetCurrentRevision(u uint64) {
	b.currentRev = u
}

func (tc *testcase) run(t *testing.T) {
	ast := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMetrics := mock.NewMinimalMetrics(ctrl)

	wg := sync.WaitGroup{}
	ts := &testRevisionServer{
		t:            t,
		ast:          ast,
		wg:           &wg,
		respWaitTime: tc.respWaitTime,
		tlsConfig:    tc.serverTlsConfig,
		enableCmux:   tc.enableCmux,
	}
	stopServer := ts.run()

	leaderElectionStub := &leader.Stub{
		ElectionInfo: leader.ElectionInfo{
			IsLeader:      false,
			LeaderAddress: "127.0.0.1:3380",
			//LeaderAddress: "10.248.146.34:3380",
		},
	}

	bs := &backendStub{}
	rs := NewRevisionSyncer(bs, mockMetrics, leaderElectionStub, tc.clientTlsConfig)
	defer rs.Close()

	times := 0
	var err error
	for times < tc.retryTimes {
		err = rs.SyncReadRevision()
		if err != nil {

			// since the server is run in a goroutine, there may be error if the server is not ready
			if errors.Is(err, syscall.ECONNREFUSED) {
				times++
				t.Logf("retry times %d", times)
				time.Sleep(time.Millisecond * 100)
				continue
			}

			if tc.expectError == nil {
				t.Error("unexpected failed of sync revision", err)
			} else {
				ast.True(errors.Is(err, tc.expectError) || strings.Contains(err.Error(), tc.expectError.Error()))
			}
			break
		}

		if tc.expectError != nil {
			ast.Error(err, "unexpected success")
		}
		break
	}

	stopServer()
	wg.Wait()
}

func TestHttpRevisionSyncer(t *testing.T) {

	serverTlsConfig, clientTlsConfig := mustLoadCert(t)
	testcases := []testcase{
		{
			name:         "success",
			retryTimes:   10,
			respWaitTime: 0,
		},
		{
			name:         "timeout",
			retryTimes:   10,
			respWaitTime: 2 * time.Second,
			expectError:  context.DeadlineExceeded,
		},
		{
			name:            "http->std https",
			retryTimes:      10,
			expectError:     errors.New("no suitable schema to leader"),
			clientTlsConfig: nil,
			serverTlsConfig: serverTlsConfig,
		},
		{
			name:            "https->std http(expect success through retrying)",
			retryTimes:      10,
			clientTlsConfig: clientTlsConfig,
			serverTlsConfig: nil,
		},
		{
			name:            "https->std https",
			retryTimes:      10,
			clientTlsConfig: clientTlsConfig,
			serverTlsConfig: serverTlsConfig,
		},
		{
			name:            "http->cmux https",
			retryTimes:      10,
			expectError:     errors.New("no suitable schema to leader"),
			clientTlsConfig: nil,
			serverTlsConfig: serverTlsConfig,
			enableCmux:      true,
		},
		{
			name:            "https->cmux http(expect success through retrying)",
			retryTimes:      10,
			clientTlsConfig: clientTlsConfig,
			serverTlsConfig: nil,
			enableCmux:      true,
		},
		{
			name:            "https->cmux https",
			retryTimes:      10,
			clientTlsConfig: clientTlsConfig,
			serverTlsConfig: serverTlsConfig,
			enableCmux:      true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t)
		})
	}
}

func getAuthPath(filename string) string {
	return filepath.Join("../../../util/auth/testdata", filename)
}
func mustLoadCert(t *testing.T) (serverTlsConfig, clientTlsConfig *tls.Config) {
	var err error
	serverTlsConfig, err = auth.GetTLSConfig(getAuthPath("server.crt"), getAuthPath("server.key"), getAuthPath("ca.crt"))

	if err != nil {
		t.Fatalf("failed to get server tls config err:%v", err)
		return nil, nil
	}

	clientTlsConfig, err = auth.GetTLSConfig(getAuthPath("server.crt"), getAuthPath("server.key"), getAuthPath("ca.crt"))

	if err != nil {
		t.Fatalf("failed to get client tls config err:%v", err)
		return nil, nil
	}
	return
}

func TestNewRevisionSyncer(t *testing.T) {
	ast := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetrics := mock.NewMinimalMetrics(ctrl)
	t.Run("conn refused", func(t *testing.T) {
		leaderElectionStub := &leader.Stub{
			ElectionInfo: leader.ElectionInfo{
				IsLeader:      false,
				LeaderAddress: "127.0.0.1:3380",
			},
		}

		backendConf := backend.Config{
			Prefix:   "/test",
			Identity: "127.0.0.1:2380",
		}
		testBackend := backend.NewBackend(newBadgerStorage(t, ast), backendConf, mockMetrics)
		rs := NewRevisionSyncer(testBackend, mockMetrics, leaderElectionStub, nil)
		defer rs.Close()

		err := rs.SyncReadRevision()
		ast.True(strings.Contains(err.Error(), "connection refused"))
	})
}

type testRevisionServer struct {
	t            *testing.T
	ast          *assert.Assertions
	wg           *sync.WaitGroup
	enableCmux   bool
	respWaitTime time.Duration
	tlsConfig    *tls.Config
}

func (t *testRevisionServer) run() (cancel func()) {
	mux := http.NewServeMux()

	var statusHandler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		if t.respWaitTime != 0 {
			time.Sleep(t.respWaitTime)
		}
		lr := LeaderRevision{Revision: 1000}
		data, err := json.Marshal(lr)
		t.ast.NoError(err)
		_, err = w.Write(data)
		t.ast.NoError(err)
	}

	mux.Handle("/status", statusHandler)
	server := http.Server{Addr: "127.0.0.1:3380", Handler: mux, TLSConfig: t.tlsConfig}
	rawListener, err := net.Listen("tcp", "127.0.0.1:3380")
	if err != nil {
		t.t.Fatalf("failed to listen err: %v", err)
		return
	}

	listener := rawListener
	if t.enableCmux {
		c := cmux.New(listener)

		if t.tlsConfig == nil {
			listener = c.Match(cmux.HTTP1())
		} else {
			listener = c.Match(cmux.TLS())
		}
		go func() {
			err := c.Serve()
			t.t.Logf("cmux exit err:%v", err)
		}()
	}

	cancel = func() {
		t.t.Logf("try to stop server addr:%s", server.Addr)
		//time.Sleep(time.Minute)
		err := server.Close()
		t.ast.NoError(err)
		if t.enableCmux {
			_ = listener.Close()
		}
		_ = rawListener.Close()
		t.t.Logf("server closed addr:%s", server.Addr)
	}

	t.wg.Add(1)
	go func() {
		defer func() {
			t.wg.Done()
			t.t.Logf("server existed addr:%s", server.Addr)
		}()

		var err error
		if server.TLSConfig != nil {
			t.t.Logf("https server run addr:%s", server.Addr)
			err = server.ServeTLS(listener, "", "")
		} else {
			t.t.Logf("http server run addr:%s", server.Addr)
			err = server.Serve(listener)
		}

		if !t.ast.True(strings.Contains(err.Error(), "http: Server closed")) {
			t.ast.Failf(err.Error(), "unexpected err")
		}
	}()

	return cancel
}
