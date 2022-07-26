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

package etcd

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	// GetPartitionMagic is the Magic Number for get partition through range request
	GetPartitionMagic int64 = 1888
	unaryRpcTimeout         = time.Second
)

func (s *RPCServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	startTime := time.Now()
	klog.InfoS("RANGE", "key", r.Key, "rangeEnd", r.RangeEnd, "countOnly", r.CountOnly)
	if err := s.peers.SyncReadRevision(); err != nil {
		return &etcdserverpb.RangeResponse{}, err
	}
	var (
		response              *etcdserverpb.RangeResponse
		err                   error
		methodTag, successTag metrics.T
	)
	if len(r.RangeEnd) == 0 {
		// get method
		response, err = s.backend.Get(ctx, r)
		methodTag = metrics.Tag("method", "get")
	} else {
		// tricky code
		// when range end is not nil and revision is GetPartitionMagic, we tell it is a request for partition infos
		if r.Revision == GetPartitionMagic {
			methodTag = metrics.Tag("method", "list-partition")
			response, err = s.backend.GetPartitions(ctx, r)
		} else if r.CountOnly {
			// count only
			methodTag = metrics.Tag("method", "count")
			// range method
			response, err = s.backend.Count(ctx, r)
		} else {
			methodTag = metrics.Tag("method", "range")
			// range method
			response, err = s.backend.List(ctx, r)
		}
	}
	successTag = getSuccessMetricTagByErr(err)
	s.metricCli.EmitCounter("read", 1, methodTag, successTag)
	s.metricCli.EmitHistogram("read.latency", time.Since(startTime).Seconds(), methodTag, successTag)
	if response != nil {
		s.metricCli.EmitHistogram("read.responsesize", response.Size(), methodTag, successTag)
	}
	return response, err
}

func (s *RPCServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	startTime := time.Now()

	deadline, ok := ctx.Deadline()
	if ok && startTime.Sub(deadline) >= 0 {
		return nil, context.DeadlineExceeded
	}
	ctx, cancel := context.WithTimeout(ctx, unaryRpcTimeout)
	defer cancel()

	// only leader can accept and handle write request
	// return error includes current leader, help etcd client send request to right instance
	if !s.peers.IsLeader() {
		s.metricCli.EmitCounter("write.follower", 1)
		if s.peers.EtcdProxyEnabled() {
			return s.peers.Txn(ctx, txn)
		}
		return nil, status.Errorf(codes.Unavailable, "txn error addr is %s leader %s", s.backend.GetResourceLock().Identity(), s.backend.GetResourceLock().Describe())
	}
	var (
		err                   error
		response              *etcdserverpb.TxnResponse
		methodTag, successTag metrics.T
		failedKey             string
	)
	if put := isCreate(txn); put != nil {
		response, err = s.backend.Create(ctx, put)
		methodTag = metrics.Tag("method", "create")
		if err != nil || !response.Succeeded {
			failedKey = string(put.Key)
		}
	} else if rev, key, ok := isDelete(txn); ok {
		response, err = s.backend.Delete(ctx, key, rev)
		methodTag = metrics.Tag("method", "delete")
		if err != nil || !response.Succeeded {
			failedKey = string(key)
		}
	} else if rev, key, value, lease, ok := isUpdate(txn); ok {
		response, err = s.backend.Update(ctx, rev, key, value, lease)
		methodTag = metrics.Tag("method", "update")
		if err != nil || !response.Succeeded {
			failedKey = string(key)
		}
	} else if ok := isCompact(txn); ok {
		response, err = s.compact()
		methodTag = metrics.Tag("method", "compact")
	} else {
		response, err = nil, fmt.Errorf("unsupported transaction: %v", txn)
		methodTag = metrics.Tag("method", "invalid")
	}
	// emit metric
	successTag = getSuccessMetricTagByErr(err)
	s.metricCli.EmitCounter("write", 1, methodTag, successTag)
	s.metricCli.EmitHistogram("write.latency", time.Since(startTime).Seconds(), methodTag, successTag)
	if response != nil {
		s.metricCli.EmitHistogram("write.responsesize", response.Size(), methodTag, successTag)
		if !response.Succeeded {
			s.metricCli.EmitCounter("write.fail", 1, methodTag)
		}
	}
	if len(failedKey) > 0 {
		klog.ErrorS(err, "txn failed", "op", methodTag.Value, "key", failedKey)
	}
	return response, err
}

func (s *RPCServer) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	return &etcdserverpb.CompactionResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: r.Revision,
		},
	}, nil
}

func (s *RPCServer) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return nil, fmt.Errorf("put is not supported")
}

func (s *RPCServer) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return nil, fmt.Errorf("delete is not supported")
}

func isCreate(txn *etcdserverpb.TxnRequest) *etcdserverpb.PutRequest {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		txn.Compare[0].GetModRevision() == 0 &&
		len(txn.Failure) == 0 &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil {
		return txn.Success[0].GetRequestPut()
	}
	return nil
}

func isDelete(txn *etcdserverpb.TxnRequest) (int64, []byte, bool) {
	if len(txn.Compare) == 0 &&
		len(txn.Failure) == 0 &&
		len(txn.Success) == 2 &&
		txn.Success[0].GetRequestRange() != nil &&
		txn.Success[1].GetRequestDeleteRange() != nil {
		rng := txn.Success[1].GetRequestDeleteRange()
		return 0, rng.Key, true
	}
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestDeleteRange() != nil {
		return txn.Compare[0].GetModRevision(), txn.Success[0].GetRequestDeleteRange().Key, true
	}
	return 0, nil, false
}

func isUpdate(txn *etcdserverpb.TxnRequest) (int64, []byte, []byte, int64, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil {
		return txn.Compare[0].GetModRevision(),
			txn.Compare[0].Key,
			txn.Success[0].GetRequestPut().Value,
			txn.Success[0].GetRequestPut().Lease,
			true
	}
	return 0, nil, nil, 0, false
}

func isCompact(txn *etcdserverpb.TxnRequest) bool {
	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go#L72
	return len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_VERSION &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil &&
		string(txn.Compare[0].Key) == "compact_rev_key"
}

// just return false, so that apiserver will not call compact method
// compact logic is maintained in kubebrain
func (s *RPCServer) compact() (*etcdserverpb.TxnResponse, error) {
	return &etcdserverpb.TxnResponse{
		Header:    &etcdserverpb.ResponseHeader{},
		Succeeded: false,
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: &etcdserverpb.ResponseHeader{},
						Kvs: []*mvccpb.KeyValue{
							{},
						},
						Count: 1,
					},
				},
			},
		},
	}, nil
}

func getSuccessMetricTagByErr(err error) metrics.T {
	if err != nil {
		return metrics.Tag("success", "false")
	}

	return metrics.Tag("success", "true")
}

func txnHeader(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: rev,
	}
}
