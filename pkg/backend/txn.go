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
	"time"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/common"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// Create implements Backend interface
func (b *backend) Create(ctx context.Context, put *proto.CreateRequest) (resp *proto.CreateResponse, err error) {
	ts := time.Now()
	defer func() {
		txnLog("create",
			put.GetKey(),
			len(put.GetValue()),
			0,
			resp.GetHeader().GetRevision(),
			resp.GetSucceeded(),
			time.Since(ts),
			err)
	}()

	revision, err := b.create(ctx, put.Key, put.Value)
	b.notify(ctx, put.Key, put.Value, revision, 0, err == nil, proto.Event_CREATE, err)
	if errors.Is(err, storage.ErrCASFailed) {
		return &proto.CreateResponse{
			Header:    responseHeader(revision),
			Succeeded: false,
		}, nil
	} else if err != nil {
		klog.ErrorS(err, "backend create err", "key", string(put.GetKey()), "revision", revision)
		return nil, err
	}

	return &proto.CreateResponse{
		Header:    responseHeader(revision),
		Succeeded: true,
	}, nil
}

func (b *backend) create(ctx context.Context, key []byte, value []byte) (revision uint64, error error) {
	revision, err := b.deal(0)
	if err != nil {
		return 0, err
	}

	if bytes.Contains(key, events) {
		err = b.creator.CreateWithTTL(ctx, key, value, revision, eventsTTL)
	} else {
		err = b.creator.Create(ctx, key, value, revision)
	}
	return revision, err
}

// Delete implements Backend interface
func (b *backend) Delete(ctx context.Context, r *proto.DeleteRequest) (resp *proto.DeleteResponse, err error) {
	ts := time.Now()
	defer func() {
		txnLog("delete",
			r.GetKey(),
			0,
			r.GetRevision(),
			resp.GetHeader().GetRevision(),
			resp.GetSucceeded(),
			time.Since(ts),
			err)

	}()

	rev, old, err := b.delete(ctx, r.Revision, r.Key)

	b.notify(ctx, r.Key, old.Val, rev, old.Revision, err == nil, proto.Event_DELETE, err)

	resp = &proto.DeleteResponse{
		Header:    responseHeader(rev),
		Succeeded: err == nil,
	}

	if err == storage.ErrKeyNotFound {
		// key is not exist
		return resp, nil
	} else if errors.Is(err, storage.ErrCASFailed) {
		// possible case:
		// 1. expect revision is too old
		// 2. concurrent modification
		val, modRevision, getErr := b.get(ctx, r.Key, 0)
		if getErr != nil {
			resp.Kv = &proto.KeyValue{
				Key:      r.Key,
				Value:    old.Val,
				Revision: old.Revision,
			}
			return resp, nil
		}

		resp.Header.Revision = maxUint64(resp.Header.Revision, modRevision)
		resp.Kv = &proto.KeyValue{
			Key:      r.Key,
			Value:    val,
			Revision: modRevision,
		}
		return resp, nil
	} else if err != nil {
		klog.ErrorS(err, "backend delete err", "key", string(r.GetKey()), "old revision", r.GetRevision(), "new revision", rev)
		return nil, err
	}

	resp.Succeeded = true
	resp.Kv = &proto.KeyValue{
		Key:      old.Key,
		Value:    old.Val,
		Revision: old.Revision,
	}
	return resp, nil
}

func (b *backend) mustDeal(prevRev uint64) (rev uint64) {
	rev, _ = b.deal(prevRev)
	return rev
}

func (b *backend) delete(ctx context.Context, oldRevision uint64, key []byte) (newRevision uint64, old KeyVal, err error) {
	expectedRevision := oldRevision

	// get the latest value
	oldVal, modRevision, err := b.get(ctx, key, 0)
	if err != nil {
		getRev := b.mustDeal(oldRevision)
		return getRev, KeyVal{}, err
	}

	newRevision, err = b.deal(oldRevision)
	if err != nil {
		return 0, KeyVal{}, err
	}

	old = KeyVal{Key: key, Revision: modRevision, Val: oldVal}

	if expectedRevision > 0 && expectedRevision != modRevision {
		// expect revision is too old
		klog.InfoS("delete cas failed", "expectRev", expectedRevision, "actualRev", modRevision)
		return newRevision, old, storage.ErrCASFailed
	} else if expectedRevision <= 0 {
		// delete the latest
		expectedRevision = modRevision
	}

	if newRevision <= modRevision {
		// there maybe a concurrent txn which is dealt later but is done more quickly
		klog.InfoS("delete cas failed", "newRev", newRevision, "modRev", expectedRevision)
		return newRevision, old, fmt.Errorf("cas failed, new revision is %d existing revision is %d", newRevision, modRevision)
	}

	objectKey := b.coder.EncodeObjectKey(key, newRevision)
	revisionKey := b.coder.EncodeRevisionKey(key)
	expectedRevisionBytes := uint64ToBytes(expectedRevision)
	newRevisionBytes := append(uint64ToBytes(newRevision), 0) // delete revision
	// delete revision, key is {raw_key}:{0}, value is {revision}{deletion_flag}

	batch := b.kv.BeginBatchWrite()
	batch.CAS(revisionKey, newRevisionBytes, expectedRevisionBytes, 0)
	batch.Put(objectKey, tombStoneBytes, 0)
	err = batch.Commit(ctx)

	// todo: need an internal retry if there is any conflict error?
	return newRevision, old, err
}

// Update implements Backend interface
func (b *backend) Update(ctx context.Context, r *proto.UpdateRequest) (resp *proto.UpdateResponse, err error) {
	ts := time.Now()
	defer func() {
		txnLog("update",
			r.GetKv().GetKey(),
			len(r.GetKv().GetValue()),
			r.GetKv().Revision,
			resp.GetHeader().GetRevision(),
			resp.GetSucceeded(),
			time.Since(ts),
			err)
	}()

	var (
		key     = r.Kv.Key
		value   = r.Kv.Value
		prevRev = r.Kv.Revision
		lease   = r.Lease
	)
	var curRev uint64
	if prevRev == 0 {
		curRev, err = b.create(ctx, key, value)
		b.notify(ctx, key, value, curRev, prevRev, err == nil, proto.Event_CREATE, err)
	} else {
		curRev, err = b.update(ctx, prevRev, key, value, lease)
		b.notify(ctx, key, value, curRev, prevRev, err == nil, proto.Event_PUT, err)
	}

	resp = &proto.UpdateResponse{
		Header:    responseHeader(curRev),
		Succeeded: err == nil,
	}
	if errors.Is(err, storage.ErrCASFailed) {
		// cas failed, just return the latest value
		val, modRevision, err := b.get(ctx, key, 0)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				resp.Kv = nil
				return resp, nil
			}
			return nil, err
		}
		resp.Header.Revision = maxUint64(resp.Header.Revision, modRevision)
		resp.Kv = &proto.KeyValue{
			Key:      key,
			Value:    val,
			Revision: modRevision,
		}
		return resp, nil
	} else if err != nil {
		klog.ErrorS(err, "backend update err", "key", string(r.GetKv().GetKey()), "old revision", r.GetKv().GetRevision(), "new revision", curRev)
		return nil, err
	}
	return resp, nil
}

func (b *backend) update(ctx context.Context, oldRevision uint64, key []byte, value []byte, lease int64) (revision uint64, err error) {
	var newRevision uint64
	newRevision, err = b.deal(oldRevision)
	if err != nil {
		return 0, err
	}

	objectKey := b.coder.EncodeObjectKey(key, newRevision)
	revisionKey := b.coder.EncodeRevisionKey(key)
	oldRevisionBytes := uint64ToBytes(oldRevision)
	newRevisionBytes := uint64ToBytes(newRevision)

	batch := b.kv.BeginBatchWrite()
	batch.CAS(revisionKey, newRevisionBytes, oldRevisionBytes, 0)
	batch.Put(objectKey, value, 0)
	return newRevision, batch.Commit(ctx)
}

func (b *backend) notify(ctx context.Context,
	key []byte, val []byte, revision, preRevision uint64, valid bool, eventType proto.Event_EventType, err error) {
	if revision == 0 {
		b.metricCli.EmitCounter("watch.event.buffer.invalid", 1)
		// todo: panic or not ?
		return
	}

	// todo: abstract as an individual component
	watchEvent := &common.WatchEvent{
		Revision:     revision,
		PrevRevision: preRevision,
		Valid:        valid,
		ResourceVerb: eventType,
		Key:          key,
		Value:        val,
		Err:          err,
	}
	// buffer full
	// TODO dynamic size
	if revision-b.GetCurrentRevision() >= watchersChanCapacity {
		b.metricCli.EmitCounter("watch.event.buffer.full", 1)
		panic("watch push buffer full")
	}
	b.watchEventsRingBuffer[int64(revision)%watchersChanCapacity].Store(watchEvent)
	b.metricCli.EmitGauge("watch.revision.lag", watchEvent.Revision-b.GetCurrentRevision())
}
