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

package scanner

import (
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

type resultReceiver interface {
	append(key, value []byte, revision uint64)
	flush()
	close()
	reset()
	needMore() bool

	// MapReduce
	fork() resultReceiver
	merge(receiver resultReceiver)
}

type emptyResultReceiver struct{}

func (e *emptyResultReceiver) needMore() bool {
	return true
}

func (e *emptyResultReceiver) append(key, value []byte, revision uint64) {
	// do nothing
}

func (e *emptyResultReceiver) flush() {
	// do nothing
}

func (e *emptyResultReceiver) close() {
	// do nothing
}

func (e *emptyResultReceiver) reset() {
	// do nothing
}

func (e *emptyResultReceiver) fork() resultReceiver {
	return &emptyResultReceiver{}
}
func (e *emptyResultReceiver) merge(receiver resultReceiver) {
	// do nothing
}

type commonResultReceiver struct {
	emptyResultReceiver
	result []*proto.KeyValue
	limit  int
}

func (c *commonResultReceiver) fork() resultReceiver {
	return &commonResultReceiver{limit: c.limit, result: make([]*proto.KeyValue, 0, c.limit)}
}

func (c *commonResultReceiver) merge(receiver resultReceiver) {
	subReceiver := receiver.(*commonResultReceiver)

	if !c.isLimited() || len(c.result)+len(subReceiver.result) <= c.limit {
		c.result = append(c.result, subReceiver.result...)
	} else {
		c.result = append(c.result, subReceiver.result[:c.limit-len(c.result)]...)
	}
}

func (c *commonResultReceiver) needMore() bool {
	if c.isLimited() {
		return len(c.result) < c.limit
	}
	return true
}

func (c *commonResultReceiver) isLimited() bool {
	return c.limit > 0
}

func (c *commonResultReceiver) append(key, value []byte, revision uint64) {
	c.result = append(c.result, &proto.KeyValue{
		Key:      key,
		Value:    value,
		Revision: revision,
	})
}

func (c *commonResultReceiver) reset() {
	c.result = make([]*proto.KeyValue, 0, len(c.result))
}

type streamResultReceiver struct {
	emptyResultReceiver
	readRev uint64
	stream  chan *proto.StreamRangeResponse
	batch   []*proto.KeyValue
}

func newStreamReceiver(readRev uint64, stream chan *proto.StreamRangeResponse) *streamResultReceiver {
	return &streamResultReceiver{
		readRev: readRev,
		stream:  stream,
	}
}

func (e *streamResultReceiver) append(key, value []byte, revision uint64) {
	e.batch = append(e.batch, &proto.KeyValue{
		Key:      key,
		Value:    value,
		Revision: revision,
	})
	if len(e.batch) >= rangeStreamBatch {
		// todo: use object pool
		batch := e.batch
		e.batch = make([]*proto.KeyValue, 0, rangeStreamBatch)
		resp := &proto.StreamRangeResponse{
			RangeResponse: &proto.RangeResponse{
				Header: &proto.ResponseHeader{Revision: e.readRev},
				Kvs:    batch,
				More:   true,
			},
		}
		e.stream <- resp
	}
}

func (e *streamResultReceiver) flush() {
	if len(e.batch) != 0 {
		resp := &proto.StreamRangeResponse{
			RangeResponse: &proto.RangeResponse{
				Header: &proto.ResponseHeader{Revision: e.readRev},
				Kvs:    e.batch,
				More:   true,
			},
		}
		e.stream <- resp
		e.reset()
	}
}

func (e *streamResultReceiver) close() {
	e.flush()
}

func (e *streamResultReceiver) reset() {
	e.batch = make([]*proto.KeyValue, 0, rangeStreamBatch)
}

func (e *streamResultReceiver) fork() resultReceiver {
	return &streamResultReceiver{
		stream: e.stream,
	}
}
