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

package util

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetHost(t *testing.T) {

	klogLevel = 0
	defer func() {
		klogLevel = 4
	}()
	t.Run("ipv4", func(t *testing.T) {
		testGetHost(t)
	})

	forceInIPv6Mode = true
	defer func() {
		forceInIPv6Mode = false
	}()

	t.Run("ipv6", func(t *testing.T) {
		testGetHost(t)
	})

}

func getIpFromHost(host string) string {
	if strings.HasPrefix(host, "[") &&
		strings.HasSuffix(host, "]") {
		// ipv6 host
		return host[1 : len(host)-1]
	}
	return host
}

func testGetHost(t *testing.T) {
	host := GetHost()

	ast := assert.New(t)
	t.Log(host)
	i := net.ParseIP(getIpFromHost(host))

	ast.Equal(isPrivate(i), !skipPrivateAddr)
}

func TestGetHostAndTestConn(t *testing.T) {
	t.Skip()
	klogLevel = 0
	defer func() {
		klogLevel = 4
	}()
	t.Run("ipv4", func(t *testing.T) {
		testGetHostAndTestConn(t)
	})

	forceInIPv6Mode = true
	defer func() {
		forceInIPv6Mode = false
	}()

	t.Run("ipv6", func(t *testing.T) {
		testGetHostAndTestConn(t)
	})

}

func testGetHostAndTestConn(t *testing.T) {
	host := GetHost()
	if host == "" {
		t.Skip("no valid host")
		return
	}
	msg := []byte("hello")
	ast := assert.New(t)
	var wg sync.WaitGroup
	mux := http.NewServeMux()
	mux.HandleFunc("/test", func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write(msg)
	})

	svr := http.Server{Addr: ":20200", Handler: mux}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = svr.ListenAndServe()
	}()

	time.Sleep(1 * time.Second)
	t.Log(host)
	ast.NotEqual("[]", host)
	// if
	resp, err := http.Get(fmt.Sprintf("http://%v:20200/test", host))
	if !ast.NoError(err) {
		return
	}
	defer resp.Body.Close()

	got, _ := ioutil.ReadAll(resp.Body)
	t.Log("got", string(got))
	ast.Equal(msg, got)
	_ = svr.Close()
	wg.Wait()
}
