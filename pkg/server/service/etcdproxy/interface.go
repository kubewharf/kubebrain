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

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// EtcdProxy forward etcd-style rpc request to leader for compatibility of k8s apiserver community edition
type EtcdProxy interface {
	// EtcdProxyEnabled returns if the etcd proxy is enabled
	EtcdProxyEnabled() bool

	// Txn forward txn unary request to leader
	Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error)

	// Watch forward watch stream request to leader
	Watch(ctx context.Context, key string, revision uint64) (<-chan []*mvccpb.Event, error)
}
