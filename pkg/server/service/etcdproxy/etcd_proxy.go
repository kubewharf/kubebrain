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

package etcdproxy

import (
	"context"
	"crypto/tls"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/util"
)

type etcdProxy struct {
	election  leader.LeaderElection
	tlsConfig *tls.Config

	closed    chan struct{}
	client    *clientv3.Client
	err       error
	curLeader string
	lock      sync.RWMutex
}

func (e *etcdProxy) EtcdProxyEnabled() bool {
	return true
}

// defaultCallOption is a copy of etcd client default call option
var defaultCallOption = []grpc.CallOption{
	grpc.FailFast(false),
	grpc.MaxCallSendMsgSize(2 * 1024 * 1024),
	grpc.MaxCallRecvMsgSize(math.MaxInt32),
}

// NewEtcdProxy return an ETCD proxy for forward request to leader
func NewEtcdProxy(leaderElection leader.LeaderElection, tlsConfig *tls.Config) EtcdProxy {
	proxy := &etcdProxy{election: leaderElection, tlsConfig: tlsConfig}
	proxy.updateClient()
	go func() {
		defer util.Recover()
		proxy.checkLeaderLoop()
	}()

	return proxy
}

func (e *etcdProxy) checkLeaderLoop() {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		e.updateClient()
		<-timer.C
		timer.Reset(time.Second)
	}
}

func (e *etcdProxy) resetClient() (reset bool) {
	reset = e.client != nil

	if e.client != nil {
		_ = e.client.Close()
		e.client = nil
	}

	if e.closed != nil {
		close(e.closed)
	}
	e.closed = make(chan struct{})
	return
}

func (e *etcdProxy) updateClient() {

	if err := e.checkConn(); err != nil {
		e.curLeader = ""
		e.lock.Lock()
		defer e.lock.Unlock()
		if e.resetClient() {
			klog.ErrorS(e.err, "reset client caused by checking conn")
		}
		return
	}

	if e.election.IsLeader() {
		e.lock.Lock()
		defer e.lock.Unlock()
		if e.resetClient() {
			klog.InfoS("reset client caused by becoming leader")
		}
		return
	}

	curLeader := e.election.GetLeaderInfo()
	if curLeader == e.curLeader || curLeader == "empty" || curLeader == "" {
		return
	}
	oldLeader := e.curLeader
	e.curLeader = curLeader

	e.lock.Lock()
	defer e.lock.Unlock()
	// close prev client
	if e.resetClient() {
		klog.InfoS("reset client caused by changing leader")
	}

	klog.InfoS("try to conn to new leader", "newLeader", curLeader)
	tlsConfigs := []*tls.Config{e.tlsConfig}
	if e.tlsConfig != nil {
		tlsConfigs = []*tls.Config{e.tlsConfig, nil}
	}

	for _, tlsConfig := range tlsConfigs {
		e.client, e.err = clientv3.New(clientv3.Config{
			Endpoints: []string{e.curLeader},
			TLS:       tlsConfig,
		})
		if e.err != nil {
			klog.ErrorS(e.err, "failed to create new client")
			e.err = status.Error(codes.Internal, e.err.Error())
			return
		}

		klog.InfoS("check conn to new leader", "newLeader", curLeader, "secure", tlsConfig == nil)

		e.err = e.checkConn()
		if e.err != nil {
			klog.ErrorS(e.err, "failed to conn to new leader", "newLeader", e.curLeader, "secure", tlsConfig != nil)
			if errors.Is(e.err, context.DeadlineExceeded) {
				// maybe tls config error, just change config and retry
				e.err = nil
				continue
			}
			break
		}

		klog.InfoS("conn to new leader", "oldLeader", oldLeader, "curLeader", e.curLeader)
		return
	}

	klog.ErrorS(e.err, "failed to conn to new leader", "newLeader", e.curLeader)
	e.curLeader = ""
	e.client = nil
}

func (e *etcdProxy) checkConn() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if e.client == nil {
		return e.err
	}

	_, err := e.client.MemberList(ctx)
	if err != nil {
		return err
	}
	return nil
}

func getKeyFromTxn(txn *etcdserverpb.TxnRequest) (string, int64) {
	if len(txn.GetCompare()) <= 0 {
		return "", 0
	}
	return string(txn.GetCompare()[0].GetKey()), txn.GetCompare()[0].GetModRevision()
}

func (e *etcdProxy) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	e.lock.RLock()
	defer e.lock.RUnlock()

	err := e.Ready()
	if err != nil {
		return nil, err
	}

	key, rev := getKeyFromTxn(txn)
	klog.InfoS("forward txn",
		"leader", e.curLeader,
		"key", key,
		"rev", rev)
	resp, err := etcdserverpb.NewKVClient(e.client.ActiveConnection()).Txn(ctx, txn, defaultCallOption...)
	if err != nil {
		klog.InfoS("forward txn failed", "key", key, "err", err.Error())
		return nil, err
	}
	if !resp.GetSucceeded() {
		var respRev int64

		if resp.Responses != nil && len(resp.Responses) == 1 {
			getResp := (*clientv3.GetResponse)(resp.Responses[0].GetResponseRange())
			if getResp != nil && getResp.Kvs != nil && len(getResp.Kvs) == 1 {
				respRev = getResp.Kvs[0].ModRevision
			}
		}

		klog.InfoS("forward txn cas failed",
			"key", key,
			"result", resp.GetSucceeded(),
			"rev", rev,
			"respRev", resp.GetHeader().GetRevision(),
			"kvRev", respRev,
		)
	}

	return resp, err
}

func (e *etcdProxy) Ready() error {
	if e.client == nil {
		return status.Errorf(codes.Unavailable, "no ready right now")
	}
	return nil
}

func (e *etcdProxy) Watch(ctx context.Context, key string, revision uint64) (<-chan []*mvccpb.Event, error) {
	e.lock.RLock()
	defer e.lock.RUnlock()

	err := e.Ready()
	if err != nil {
		return nil, err
	}

	outputCh := make(chan []*mvccpb.Event, 100)
	closed := e.closed
	go func() {
		defer util.Recover()
		defer close(outputCh)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		klog.InfoS("etcd proxy start watching")
		inputCh := e.client.Watch(ctx, key, clientv3.WithRev(int64(revision)), clientv3.WithPrefix())
		for {
			select {
			case <-closed:
				klog.InfoS("leader change")
				return
			case <-ctx.Done():
				klog.InfoS("etcd proxy watch ctx done")
				return
			case wresp, ok := <-inputCh:
				if !ok {
					klog.InfoS("etcd proxy watch closed", "key", key, "rev", revision, "channel", outputCh)
					return
				}
				err := wresp.Err()
				if err != nil {
					klog.InfoS("etcd proxy watch error", "key", key, "rev", revision, "channel", outputCh, "error", err)
					return
				}
				outputCh <- convertEvents(wresp.Events)
			}
		}
	}()
	return outputCh, nil
}

func convertEvents(events []*clientv3.Event) []*mvccpb.Event {
	ret := make([]*mvccpb.Event, len(events))
	for i, event := range events {
		ret[i] = (*mvccpb.Event)(event)
	}
	return ret
}
