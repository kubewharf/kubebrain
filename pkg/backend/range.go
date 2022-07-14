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

package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// Get implements Backend interface
func (b *backend) Get(ctx context.Context, r *proto.GetRequest) (resp *proto.GetResponse, err error) {
	ts := time.Now()
	defer func() {
		klog.V(klogLevel).InfoS("get",
			"key", Key(r.GetKey()),
			"rev", r.GetRevision(),
			"respRev", resp.GetHeader().GetRevision(),
			"respSize", resp.Size(),
			"latency", time.Now().Sub(ts))
	}()

	curRev := b.tso.GetRevision()
	requireRev := uint64(r.GetRevision())

	val, modRev, err := b.get(ctx, r.Key, requireRev)
	if err == storage.ErrKeyNotFound {
		return &proto.GetResponse{
			Header: responseHeader(curRev),
		}, nil
	} else if err != nil {
		klog.ErrorS(err, "backend get err", "key", string(r.GetKey()), "revision", r.GetRevision())
		return nil, err
	}

	if modRev > curRev {
		curRev = modRev
	}

	resp = &proto.GetResponse{
		Header: responseHeader(curRev),
	}
	if val != nil {
		resp.Kv = &proto.KeyValue{
			Key:      r.Key,
			Value:    val,
			Revision: uint64(modRev),
		}
	}

	return resp, nil
}

func (b *backend) getLatestInternalVal(ctx context.Context, key []byte) (val []byte, modRevision uint64, err error) {
	return b.getInternalVal(ctx, key, 0)
}

func (b *backend) get(ctx context.Context, key []byte, revision uint64) (val []byte, modRevision uint64, err error) {
	val, modRevision, err = b.getInternalVal(ctx, key, revision)
	if bytes.Compare(val, tombStoneBytes) == 0 {
		return nil, modRevision, storage.ErrKeyNotFound
	}
	return val, modRevision, err
}

func (b *backend) getInternalVal(ctx context.Context, key []byte, revision uint64) (val []byte, modRevision uint64, err error) {
	if revision == 0 {
		revision = math.MaxUint64
	}

	startKey := b.coder.EncodeObjectKey(key, revision)
	endKey := b.coder.EncodeObjectKey(key, 0)
	iter, err := b.kv.Iter(ctx, startKey, endKey, 0, 1)
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()
	err = iter.Next(ctx)
	if err != nil {
		if err == io.EOF {
			// it's compacted after deleted or it doesn't exist
			return nil, 0, storage.ErrKeyNotFound
		}
		return nil, 0, err
	}

	userKey, modRev, err := b.coder.Decode(iter.Key())
	if modRev == 0 || bytes.Compare(userKey, key) != 0 {
		// it's marked by deleting
		return nil, modRev, storage.ErrKeyNotFound
	}
	return iter.Val(), modRev, nil
}

// List implements Backend interface
func (b *backend) List(ctx context.Context, r *proto.RangeRequest) (resp *proto.RangeResponse, err error) {
	ts := time.Now()
	defer func() {
		klog.V(klogLevel).InfoS("list",
			"start", Key(r.GetKey()),
			"end", Key(r.GetEnd()),
			"rev", r.GetRevision(),
			"limit", r.GetLimit(),
			"respCount", len(resp.GetKvs()),
			"respSize", resp.Size(),
			"latency", time.Now().Sub(ts))
	}()

	if len(r.End) == 0 {
		return nil, fmt.Errorf("invalid nil end field in RangeRequest")
	}

	reqRevision := uint64(r.Revision)
	curRevision := b.tso.GetRevision()
	if reqRevision == 0 {
		reqRevision = curRevision
	}

	if bytes.Compare(r.Key, r.End) >= 0 {
		return nil, errors.New("invalid range end")
	}

	key, rangeEnd := b.coder.EncodeObjectKey(r.Key, 0), b.coder.EncodeObjectKey(r.End, 0)

	// add limit to check if there is more value
	limit := r.Limit
	if limit > 0 {
		limit++
	}

	kvs, err := b.scanner.Range(ctx, key, rangeEnd, reqRevision, limit)
	if err != nil {
		klog.ErrorS(err, "backend range err", "key", string(r.GetKey()), "end", string(r.GetEnd()), "revision", r.GetRevision())
		return nil, err
	}
	resp = &proto.RangeResponse{
		Header: responseHeader(curRevision),
	}

	if limit > 0 && len(kvs) > int(r.Limit) {
		resp.More = true
		kvs = kvs[0:r.Limit]
	}
	resp.Kvs = kvs
	return resp, nil
}

// Count implements Backend interface
func (b *backend) Count(ctx context.Context, r *proto.CountRequest) (resp *proto.CountResponse, err error) {
	ts := time.Now()
	defer func() {
		klog.V(klogLevel).InfoS("count",
			"start", Key(r.GetKey()),
			"end", Key(r.GetEnd()),
			"respCount", resp.GetCount(),
			"latency", time.Now().Sub(ts))
	}()

	rev := b.tso.GetRevision()
	if !b.config.EnableEtcdCompatibility {
		return &proto.CountResponse{
			Header: responseHeader(rev),
			Count:  uint64(0),
		}, nil
	}

	key, rangeEnd := b.coder.EncodeObjectKey(r.Key, 0), b.coder.EncodeObjectKey(r.End, 0)
	count, err := b.scanner.Count(ctx, key, rangeEnd, rev)
	if err != nil {
		klog.Errorf("backend count %v return err %v", r, err)
		return nil, err
	}
	return &proto.CountResponse{
		Header: responseHeader(rev),
		Count:  uint64(count),
	}, nil
}

// GetPartitions implements Backend interface
func (b *backend) GetPartitions(ctx context.Context, r *proto.ListPartitionRequest) (resp *proto.ListPartitionResponse, err error) {
	ts := time.Now()
	defer func() {
		klog.V(klogLevel).InfoS("get partitions",
			"start", Key(r.GetKey()),
			"end", Key(r.GetEnd()),
			"respCount", resp.GetPartitionNum(),
			"latency", time.Now().Sub(ts))
	}()

	rev := b.tso.GetRevision()
	start, end := b.coder.EncodeObjectKey(r.Key, 0), b.coder.EncodeObjectKey(r.End, 0)

	partitions, err := b.kv.GetPartitions(ctx, start, end)

	if err != nil {
		klog.Errorf("backend getPartitions %v return err %v", r, err)
		return nil, err
	}
	resp = &proto.ListPartitionResponse{
		Header:       responseHeader(rev),
		PartitionNum: int64(len(partitions)),
	}
	// kvs length = partition number + 1
	resp.PartitionKeys = make([][]byte, 0, len(partitions)+1)

	for idx, p := range partitions {
		// append range start of partition only
		resp.PartitionKeys = append(resp.PartitionKeys, p.Start)

		// append last end of partition
		if idx == len(partitions)-1 {
			resp.PartitionKeys = append(resp.PartitionKeys, p.End)
		}
	}
	return resp, nil
}

// ListByStream implements Backend interface
func (b *backend) ListByStream(ctx context.Context, startKey, endKey []byte, rev uint64) (<-chan *proto.StreamRangeResponse, error) {

	curRev := b.tso.GetRevision()
	if rev == 0 {
		rev = curRev
	}
	klog.V(klogLevel).InfoS("list by stream", "start", Key(startKey), "end", Key(endKey), "rev", rev)
	stream := b.scanner.RangeStream(ctx, startKey, endKey, rev)
	return stream, nil
}
