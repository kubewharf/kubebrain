// Copyright 2023 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
)

type RevisionSyncer interface {
	// SyncReadRevision  fetch the latest revision from leader and set it in backend
	// if this instance is follower. otherwise, do nothing.
	SyncReadRevision() error

	// Close closes syncer
	Close() error
}

type Backend interface {
	// SetCurrentRevision sets the revision of backend
	SetCurrentRevision(uint64)
}

var (
	syncRevTimeout = time.Second
)

type revisionSyncer struct {
	// inject
	leaderElection leader.LeaderElection
	metricCli      metrics.Metrics
	backend        Backend
	enableTLS      bool

	// internal
	flight     singleflight.Group
	schema     string
	httpClient *http.Client
}

func defaultTransportDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return dialer.DialContext
}

func NewRevisionSyncer(backend Backend, metricCli metrics.Metrics, l leader.LeaderElection, tlsConfig *tls.Config) RevisionSyncer {
	r := &revisionSyncer{
		leaderElection: l,
		flight:         singleflight.Group{},
		metricCli:      metricCli,
		backend:        backend,
		schema:         "http",
		enableTLS:      false,
	}

	// transport is a copy of http.DefaultTransport
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: defaultTransportDialContext(&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if tlsConfig != nil {
		r.schema = "https"
		r.enableTLS = true
		transport.TLSClientConfig = tlsConfig
	}

	r.httpClient = &http.Client{
		Transport: transport,
		Timeout:   syncRevTimeout,
	}

	return r
}

// SyncReadRevision implements RevisionSyncer
func (r *revisionSyncer) SyncReadRevision() error {
	if r.leaderElection.IsLeader() {
		return nil
	}
	// only sync when not leader
	r.metricCli.EmitCounter("read.follower", 1)
	currentRevision, err := r.singleFlightGetRevisionFromLeader()
	if err != nil {
		r.metricCli.EmitCounter("read.follower.revision_err", 1)
		klog.Errorf("sync read revision failed %v", err)
		return fmt.Errorf("get revision from leader failed %v", err)
	}
	r.backend.SetCurrentRevision(currentRevision)
	return nil
}

// Close implements RevisionSyncer
func (r *revisionSyncer) Close() error {
	r.httpClient.CloseIdleConnections()
	return nil
}

// LeaderRevision is the data return by leader
type LeaderRevision struct {
	// Revision is the revision of leader
	Revision uint64
}

func (r *revisionSyncer) singleFlightGetRevisionFromLeader() (uint64, error) {
	v, err, _ := r.flight.Do("get_revision", func() (interface{}, error) {
		// there is no guarantee about the schema of leader, so we just try one by one
		for _, schema := range r.getRetrySchemas() {
			r.schema = schema
			rev, err := r.getRevisionFromLeader()
			if err != nil {
				if possibleSchemaMismatch(err) {
					// switch schema and retry in next loop if possible
					continue
				}

				// for others error, just return (maybe timeout)
				return uint64(0), err
			}

			return rev, nil
		}

		// maybe leader can be access by https only but current node is running without cert
		err := status.Errorf(codes.Unavailable, "no suitable schema to leader")
		klog.ErrorS(err, "can not get revision from leader", "leader", r.leaderElection.GetLeaderInfo())
		return uint64(0), err
	})
	return v.(uint64), err
}

func possibleSchemaMismatch(err error) bool {
	if err == nil {
		// success
		return false
	}
	// schema mismatching will rise an error:
	// ┌──────────┬──────────┬───────────────────────────────────────────────┐
	// │  client  │  server  │                     error                     │
	// ├──────────┼──────────┼───────────────────────────────────────────────┤
	// │   http   │cmux https│ connection reset by peer (syscall.ECONNRESET) │
	// ├──────────┼──────────┼───────────────────────────────────────────────┤
	// │  https   │cmux http │                 EOF (io.EOF)                  │
	// ├──────────┼──────────┼───────────────────────────────────────────────┤
	// │   http   │std https │Client sent an HTTP request to an HTTPS server.│
	// ├──────────┼──────────┼───────────────────────────────────────────────┤
	// │  https   │ std http │http: server gave HTTP response to HTTPS client│
	// └──────────┴──────────┴───────────────────────────────────────────────┘
	// switch schema if possible
	if errors.Is(err, syscall.ECONNRESET) {
		klog.InfoS("conn reset", "err", err)
		return true
	}

	if errors.Is(err, io.EOF) {
		klog.InfoS("EOF", "err", err)
		return true
	}

	if isSendHttpReqToHttpsServerErr(err) {
		klog.InfoS("send http request to https server", "err", err)
		return true
	}

	if isSendHttpsReqToHttpServerErr(err) {
		klog.InfoS("send https request to http server", "err", err)
		return true
	}

	// for other error, do not retry
	return false
}

func isSendHttpReqToHttpsServerErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Client sent an HTTP request to an HTTPS server.")
}

func isSendHttpsReqToHttpServerErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "http: server gave HTTP response to HTTPS client")
}

func (r *revisionSyncer) getRevisionFromLeader() (uint64, error) {
	leaderAddress := r.leaderElection.GetLeaderInfo()
	r.metricCli.EmitGauge("follower.getleader", 1, metrics.Tag("leader", leaderAddress))
	startTime := time.Now()

	// todo: implement it based on grpc API
	url := fmt.Sprintf("%s://%s/status", r.schema, leaderAddress)
	klog.V(10).InfoS("get revision", "from", url)
	response, err := r.httpClient.Get(url)
	r.metricCli.EmitHistogram("member.round_trip",
		time.Now().Sub(startTime).Milliseconds(),
		metrics.Tag("leader", leaderAddress))
	if err != nil {
		r.metricCli.EmitCounter("follower.get.revision.err", 1, metrics.Tag("leader", leaderAddress))
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		r.metricCli.EmitCounter("follower.get.revision.failed", 1, metrics.Tag("leader", leaderAddress))
		//return 0, errors.Wrapf(err, "status code from leader %s is %d", leaderAddress, response.StatusCode)

		msg, _ := io.ReadAll(response.Body)
		return 0, fmt.Errorf("status code from leader %s is %d, msg is %s", leaderAddress, response.StatusCode, msg)
	}

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}

	revision := &LeaderRevision{}
	json.Unmarshal(responseBody, revision)
	r.metricCli.EmitGauge("follower.get.revision", revision.Revision, metrics.Tag("leader", leaderAddress))
	return revision.Revision, nil
}

var (
	schemasHttpOnly  = []string{"http"}
	schemasHttpHttps = []string{"http", "https"}
	schemasHttpsHttp = []string{"https", "http"}
)

func (r *revisionSyncer) getRetrySchemas() []string {
	if !r.enableTLS {
		return schemasHttpOnly
	}
	// prefer prev connectable schema
	if r.schema == "http" {
		return schemasHttpHttps
	}
	return schemasHttpsHttp
}
