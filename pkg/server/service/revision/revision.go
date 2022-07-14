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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	b "github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
)

type RevisionSyncer interface {
	// SyncReadRevision  fetch the latest revision from leader and set it in backend
	// if this instance is follower. otherwise, do nothing.
	SyncReadRevision() error
}

type revisionSyncer struct {
	// inject
	leaderElection leader.LeaderElection
	metricCli      metrics.Metrics
	backend        b.Backend
	enableTLS      bool

	// internal
	flight singleflight.Group
	schema string
}

func NewRevisionSyncer(backend b.Backend, metricCli metrics.Metrics, l leader.LeaderElection, tlsConfig *tls.Config) RevisionSyncer {
	r := &revisionSyncer{
		leaderElection: l,
		flight:         singleflight.Group{},
		metricCli:      metricCli,
		backend:        backend,
		schema:         "http",
		enableTLS:      false,
	}

	if tlsConfig != nil {
		r.schema = "https"
		r.enableTLS = true
		http.DefaultTransport.(*http.Transport).TLSClientConfig = tlsConfig
	}

	return r
}

// SyncReadRevision implements PeerService
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
			if errors.Is(err, syscall.ECONNRESET) {
				// schema mismatching will rise an error of resetting conn,
				// just retry with another schema
				continue
			}
			// for others error, just return
			return rev, err
		}

		// maybe leader can be access by https only but current node is running without cert
		err := status.Errorf(codes.Unavailable, "no suitable schema to leader")
		klog.ErrorS(err, "can not get revision from leader", "leader", r.leaderElection.GetLeaderInfo())
		return uint64(0), err
	})
	return v.(uint64), err
}

func (r *revisionSyncer) getRevisionFromLeader() (uint64, error) {
	leaderAddress := r.leaderElection.GetLeaderInfo()
	r.metricCli.EmitGauge("follower.getleader", 1, metrics.Tag("leader", leaderAddress))
	startTime := time.Now()

	// todo: implement it based on grpc API
	response, err := http.Get(fmt.Sprintf("%s://%s/status", r.schema, leaderAddress))
	r.metricCli.EmitHistogram("member.round_trip", time.Now().Sub(startTime).Milliseconds(), metrics.Tag("leader", leaderAddress))
	if err != nil {
		r.metricCli.EmitCounter("follower.get.revision.err", 1, metrics.Tag("leader", leaderAddress))
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		r.metricCli.EmitCounter("follower.get.revision.failed", 1, metrics.Tag("leader", leaderAddress))
		return 0, fmt.Errorf("status code from leader %s is %d", leaderAddress, response.StatusCode)
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
