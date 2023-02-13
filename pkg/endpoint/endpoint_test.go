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
	"io/ioutil"
	"net/http"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/kubewharf/kubebrain/pkg/backend"
	mockmetrics "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server"
	"github.com/kubewharf/kubebrain/pkg/storage"
	ibadger "github.com/kubewharf/kubebrain/pkg/storage/badger"
)

func newBadgerStorage(t *testing.T, ast *assert.Assertions) storage.KvStorage {
	p := path.Join(t.TempDir(), "badger")
	st, err := ibadger.NewKvStorage(ibadger.Config{Dir: p})
	if err != nil {
		ast.FailNow(err.Error())
	}
	return st
}

func getAuthPath(filename string) string {
	return filepath.Join("../util/auth/testdata", filename)
}

func TestRunEndpoint(t *testing.T) {
	ast := assert.New(t)
	conf := Config{
		Port:     2379,
		PeerPort: 2380,
		ClientSecurityConfig: &SecurityConfig{
			CertFile:      getAuthPath("server.crt"),
			KeyFile:       getAuthPath("server.key"),
			CA:            getAuthPath("ca.crt"),
			ClientAuth:    true,
			AllowInsecure: true,
		},
		PeerSecurityConfig: &SecurityConfig{
			CertFile:      getAuthPath("server.crt"),
			KeyFile:       getAuthPath("server.key"),
			CA:            getAuthPath("ca.crt"),
			ClientAuth:    true,
			AllowInsecure: true,
		},
	}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockMetrics := mockmetrics.NewMinimalMetrics(mockCtrl)
	backendConf := backend.Config{
		Prefix:   "/test",
		Identity: "127.0.0.1:2380",
	}
	testBackend := backend.NewBackend(newBadgerStorage(t, ast), backendConf, mockMetrics)
	ep := NewEndpoint(testBackend, mockMetrics, &conf)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, _ := errgroup.WithContext(context.Background())
	eg.Go(func() error {
		return ep.Run(ctx)
	})

	// wait for endpoint running
	time.Sleep(3 * time.Second)
	http.DefaultTransport.(*http.Transport).TLSClientConfig = conf.PeerSecurityConfig.getClientTLSConfig()
	urls := []string{
		"http://127.0.0.1:2379/health",
		"https://127.0.0.1:2379/health",
	}
	for _, url := range urls {
		t.Logf("testing url %s", url)
		resp, err := http.Get(url)
		ast.NoError(err)
		bs, err := ioutil.ReadAll(resp.Body)
		ast.NoError(err)
		ast.Equal(server.HealthResponse, string(bs))
		resp.Body.Close()
	}

	cancel()
	eg.Wait()
}
