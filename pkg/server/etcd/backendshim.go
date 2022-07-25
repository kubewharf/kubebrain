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

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
)

var (
	_ BackendShim = (*backendShim)(nil)
)

// BackendShim wrapper Backend interface to adapt with Etcd grpc protobuf
type BackendShim interface {
	// Create inserts new key into storage
	Create(ctx context.Context, put *etcdserverpb.PutRequest) (*etcdserverpb.TxnResponse, error)

	// Delete removes key from storage
	Delete(ctx context.Context, key []byte, revision int64) (*etcdserverpb.TxnResponse, error)

	// Update set key into storage
	Update(ctx context.Context, rev int64, key []byte, value []byte, lease int64) (*etcdserverpb.TxnResponse, error)

	// Compact clears the kvs that are too old
	Compact(ctx context.Context, revision uint64) (*etcdserverpb.TxnResponse, error)

	// Get read a kv from storage
	Get(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// List read kvs in range
	List(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// Count counts the number of kvs in range
	Count(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// GetPartitions query the partition state of storage for ListByStream
	GetPartitions(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// ListByStream reads kvs in range by stream
	ListByStream(ctx context.Context, startKey, endKey []byte, revision uint64) (<-chan *etcdserverpb.WatchResponse, error)

	// Watch subscribe the changes from revision on kvs with given prefix
	Watch(ctx context.Context, key string, revision uint64) (<-chan []*mvccpb.Event, error)

	// GetResourceLock returns the resource lock for leader election
	GetResourceLock() resourcelock.Interface

	// GetCurrentRevision returns the read revision
	GetCurrentRevision() uint64

	// SetCurrentRevision is used for init tso for leader
	SetCurrentRevision(uint64)
}

// implement backendShim interface
type backendShim struct {
	// raw backend
	backend backend.Backend
	// emit metrics
	metricCli metrics.Metrics
}

func NewBackendShim(backend backend.Backend, metricCli metrics.Metrics) BackendShim {
	return &backendShim{
		backend:   backend,
		metricCli: metricCli,
	}
}

func (b *backendShim) Create(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.TxnResponse, error) {
	if r.IgnoreLease {
		return nil, unsupported("ignoreLease")
	} else if r.IgnoreValue {
		return nil, unsupported("ignoreValue")
	} else if r.PrevKv {
		return nil, unsupported("prevKv")
	}

	request := &proto.CreateRequest{
		Key:   r.Key,
		Value: r.Value,
		Lease: r.Lease,
	}
	response, err := b.backend.Create(ctx, request)
	if err != nil {
		return nil, err
	}
	// transform response from etcd protobuf to kube-brain protobuf
	createResponse := &etcdserverpb.TxnResponse{
		Header: txnHeader(int64(response.Header.Revision)),
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{
						Header: txnHeader(int64(response.Header.Revision)),
					},
				},
			},
		},
		Succeeded: response.Succeeded,
	}

	return createResponse, nil
}

func (b *backendShim) Delete(ctx context.Context, key []byte, revision int64) (*etcdserverpb.TxnResponse, error) {
	request := &proto.DeleteRequest{
		Key:      key,
		Revision: uint64(revision),
	}
	response, err := b.backend.Delete(ctx, request)
	if err != nil {
		return nil, err
	}
	var kvs []*mvccpb.KeyValue

	if response.Kv != nil {
		kvs = append(kvs, kvToEtcdKv(response.Kv))
	}
	deleteResponse := &etcdserverpb.TxnResponse{
		Header:    txnHeader(int64(response.Header.Revision)),
		Succeeded: response.Succeeded,
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: txnHeader(int64(response.Header.Revision)),
						Kvs:    kvs,
					},
				},
			},
		},
	}
	return deleteResponse, nil
}

func (b *backendShim) Update(ctx context.Context, rev int64, key []byte, value []byte, lease int64) (*etcdserverpb.TxnResponse, error) {
	request := &proto.UpdateRequest{
		Kv: &proto.KeyValue{
			Key:      key,
			Value:    value,
			Revision: uint64(rev),
		},
		Lease: lease,
	}
	// paas through update method
	response, err := b.backend.Update(ctx, request)
	if err != nil {
		return nil, err
	}
	headerRevision := int64(response.Header.Revision)
	resp := &etcdserverpb.TxnResponse{
		Header:    txnHeader(headerRevision),
		Succeeded: response.Succeeded,
	}
	if response.Succeeded {
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{
						Header: txnHeader(headerRevision),
					},
				},
			},
		}
	} else {
		var kvs []*mvccpb.KeyValue
		if response.Kv != nil {
			kvs = append(kvs, kvToEtcdKv(response.Kv))
		}
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: txnHeader(headerRevision),
						Kvs:    kvs,
					},
				},
			},
		}
	}

	return resp, nil
}

// TODO: compact is unnecessary for kube-brain ?
func (b *backendShim) Compact(ctx context.Context, revision uint64) (*etcdserverpb.TxnResponse, error) {
	_, err := b.backend.Compact(ctx, revision)
	if err != nil {
		return nil, err
	}

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

func (b *backendShim) Get(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// transform request from etcd protobuf to kube-brain protobuf
	request := &proto.GetRequest{
		Key:      r.Key,
		Revision: uint64(r.Revision),
	}
	// pass through get method
	response, err := b.backend.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	resp := &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
	}
	if response.Kv != nil {
		resp.Kvs = append(resp.Kvs, kvToEtcdKv(response.Kv))
		resp.Count = 1
	}
	return resp, nil
}

func (b *backendShim) List(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	request := &proto.RangeRequest{
		Key:      r.Key,
		End:      r.RangeEnd,
		Limit:    r.Limit,
		Revision: uint64(r.Revision),
	}
	// pass through list method
	response, err := b.backend.List(ctx, request)
	if err != nil {
		return nil, err
	}

	resp := &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
		Count:  int64(len(response.Kvs)),
		More:   response.More,
		Kvs:    make([]*mvccpb.KeyValue, 0, len(response.Kvs)),
	}
	if resp.More {
		resp.Count = resp.Count + 1
	}

	for _, kv := range response.Kvs {
		resp.Kvs = append(resp.Kvs, kvToEtcdKv(kv))
	}
	return resp, nil
}

func (b *backendShim) Count(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// transform count request from etcd protobuf to kube-brain protobuf
	request := &proto.CountRequest{
		Key: r.Key,
		End: r.RangeEnd,
	}

	// pass through count method
	response, err := b.backend.Count(ctx, request)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
		Count:  int64(response.Count),
	}, nil
}

func (b *backendShim) GetPartitions(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// transform list partition request from etcd protobuf to kube-brain protobuf
	request := &proto.ListPartitionRequest{
		Key: r.Key,
		End: r.RangeEnd,
	}
	// pass through get partition method
	response, err := b.backend.GetPartitions(ctx, request)
	if err != nil {
		return nil, err
	}
	resp := &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
		Count:  response.PartitionNum + 1,
		Kvs:    make([]*mvccpb.KeyValue, 0, response.PartitionNum+1),
	}
	for _, kv := range response.PartitionKeys {
		resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{
			Key: kv,
		})

	}
	return resp, nil
}

// todo deprecate range stream in etcd
func (b *backendShim) ListByStream(ctx context.Context, startKey, endKey []byte, revision uint64) (<-chan *etcdserverpb.WatchResponse, error) {
	ch, err := b.backend.ListByStream(context.Background(), startKey, endKey, revision)
	if err != nil {
		return nil, err
	}
	responseCh := make(chan *etcdserverpb.WatchResponse)
	transformResponseFunc := func(ctx context.Context, in <-chan *proto.StreamRangeResponse, out chan *etcdserverpb.WatchResponse) {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				klog.Info("[backend-shim] range stream ctx done")
				return
			case response, ok := <-in:
				if !ok {
					return
				}
				if response == nil || response.RangeResponse == nil || response.RangeResponse.Header == nil {
					klog.Fatalf("invalid response start %s end %s from backend list stream, range response or range response header is nil", startKey, endKey)
				}
				etcdWatchResponse := &etcdserverpb.WatchResponse{
					Header: txnHeader(int64(response.RangeResponse.Header.Revision)),
					Events: make([]*mvccpb.Event, 0, len(response.RangeResponse.Kvs)),
				}
				if response.RangeResponse.More == false {
					etcdWatchResponse.Canceled = true
					etcdWatchResponse.CancelReason = response.Err
				} else {
					for _, kv := range response.RangeResponse.Kvs {
						etcdWatchResponse.Events = append(etcdWatchResponse.Events, &mvccpb.Event{
							Kv: kvToEtcdKv(kv),
						})
					}
				}
				// send to server layer
				out <- etcdWatchResponse
			}
		}
	}
	go transformResponseFunc(ctx, ch, responseCh)
	return responseCh, nil
}

func (b *backendShim) Watch(ctx context.Context, key string, revision uint64) (<-chan []*mvccpb.Event, error) {
	ch, err := b.backend.Watch(ctx, key, revision)
	if err != nil {
		return nil, err
	}
	watchResponseCh := make(chan []*mvccpb.Event)
	transformResponseFunc := func(ctx context.Context, in <-chan []*proto.Event, out chan []*mvccpb.Event) {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				klog.Info("[backend-shim] watch ctx done")
				return
			case events, ok := <-in:
				if !ok {
					return
				}
				etcdEvents := make([]*mvccpb.Event, 0, len(events))
				for _, e := range events {
					etcdEvent := &mvccpb.Event{}
					if e.Type == proto.Event_CREATE || e.Type == proto.Event_PUT {
						etcdEvent.Type = mvccpb.PUT
						etcdEvent.Kv = kvToEtcdKv(e.Kv)
						etcdEvent.Kv.CreateRevision = etcdEvent.Kv.ModRevision
					} else {
						etcdEvent.Type = mvccpb.DELETE
						etcdEvent.PrevKv = kvToEtcdKv(e.Kv)
						etcdEvent.Kv = &mvccpb.KeyValue{
							ModRevision: int64(e.Revision),
							Key:         e.Kv.Key,
						}
					}
					etcdEvents = append(etcdEvents, etcdEvent)
				}
				out <- etcdEvents
			}
		}
	}
	go transformResponseFunc(ctx, ch, watchResponseCh)
	return watchResponseCh, nil
}

// leader election and revision related method, just pass through
func (b *backendShim) GetResourceLock() resourcelock.Interface {
	return b.backend.GetResourceLock()
}

func (b *backendShim) GetCurrentRevision() uint64 {
	return b.backend.GetCurrentRevision()
}

func (b *backendShim) SetCurrentRevision(revision uint64) {
	b.backend.SetCurrentRevision(revision)
}

func kvToEtcdKv(kv *proto.KeyValue) *mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	return &mvccpb.KeyValue{
		Key:         kv.Key,
		Value:       kv.Value,
		ModRevision: int64(kv.Revision),
	}
}

func unsupported(field string) error {
	return fmt.Errorf("%s is unsupported", field)
}
