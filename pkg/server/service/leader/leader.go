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

package leader

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	b "github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// LeaderElection is responsible for run leader election
// and handle state when leader status changed
type LeaderElection interface {
	// Campaign run leader election loop
	Campaign()

	// GetLeaderInfo get leader info, return peer address
	GetLeaderInfo() string

	// IsLeader return true when this instance is leader
	IsLeader() bool

	// GetElectionInfo get info of election
	GetElectionInfo() (ElectionInfo, error)
}

// ElectionInfo is the response for http election service
type ElectionInfo struct {
	// LeaderAddress is the ip address of leader
	LeaderAddress string

	// IsLeader returns true if current node is leader
	IsLeader bool
}

type leaderElection struct {
	backend b.Backend
	// resource lock
	resourceLock resourcelock.Interface
	metricCli    metrics.Metrics
	// onStartedLeading is called when a LeaderElector client starts leading
	onStartedLeading func(context.Context)
	// onStoppedLeading is called when a LeaderElector client stops leading
	onStoppedLeading func()
	// indicates whether this instance is leader
	leader bool
}

// NewLeaderElection returns a LeaderElection based on resourcelock of backend.Backend
func NewLeaderElection(backend b.Backend, metricCli metrics.Metrics, onStartedLeading func(context.Context), onStoppedLeading func()) LeaderElection {
	return &leaderElection{
		backend:          backend,
		resourceLock:     backend.GetResourceLock(),
		metricCli:        metricCli,
		onStartedLeading: onStartedLeading,
		onStoppedLeading: onStoppedLeading,
	}
}

// Campaign implements LeaderElection interface
func (l *leaderElection) Campaign() {
	leaderelection.RunOrDie(context.Background(), leaderelection.LeaderElectionConfig{
		Lock:            l.resourceLock,
		ReleaseOnCancel: true,
		// lease timout deadline
		LeaseDuration: 8 * time.Second,
		// renew deadline
		RenewDeadline: 5 * time.Second,
		// renew lease period
		RetryPeriod: 1 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// we're notified when we start - this is where you would
				// usually put your code
				klog.Info("start leading")
				l.metricCli.EmitCounter("leader.election.success", 1)
				leaderAddr, version, err := l.getLeaderAndVersion()
				if err != nil {
					klog.Fatal("leader lost")
					panic("invalid leader info")
				}
				l.metricCli.EmitGauge("leader.election.initial.version", version, metrics.Tag("addr", leaderAddr))
				// TODO push this logic to on start leading call back
				l.backend.SetCurrentRevision(version)
				l.leader = true
				l.onStartedLeading(ctx)
			},
			OnStoppedLeading: func() {
				// we can do cleanup here, or after the RunOrDie method
				// returns
				l.leader = false
				l.onStoppedLeading()
				leaderAddr := l.GetLeaderInfo()
				l.metricCli.EmitCounter("leader.election.lost", 1, metrics.Tag("addr", leaderAddr))
				klog.Fatal("leader lost")
				// panic to avoid watchCache field in backend dirty, simple and rude
				panic("leader lost")
			},
		},
	})
}

// IsLeader implements LeaderElection interface
func (l *leaderElection) IsLeader() bool {
	return l.leader
}

// GetLeaderInfo implements LeaderElection interface
func (l *leaderElection) GetLeaderInfo() string {
	leaderAddr, _, _ := l.getLeaderAndVersion()
	return leaderAddr
}

func (l *leaderElection) GetElectionInfo() (ElectionInfo, error) {
	leaderAddr, _, err := l.getLeaderAndVersion()
	if err != nil {
		return ElectionInfo{}, err
	}
	return ElectionInfo{
		LeaderAddress: leaderAddr,
		IsLeader:      l.IsLeader(),
	}, err
}

func (l *leaderElection) getLeaderAndVersion() (string, uint64, error) {
	lockInfo := l.resourceLock.Describe()
	infos := strings.Split(lockInfo, ",")
	if len(infos) != 2 {
		return "", 0, fmt.Errorf("lock info invalid %s", lockInfo)
	}
	version, err := strconv.ParseUint(infos[1], 10, 64)
	if err != nil {
		return "", 0, err
	}

	return infos[0], version, nil
}
