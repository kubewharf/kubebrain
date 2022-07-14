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
	"path"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

func TestConstructCompactBordersWithSkippedPrefixOption(t *testing.T) {

	var tests = []struct {
		in       Config   // input
		expected [][]byte // expected result
	}{
		{
			Config{
				Prefix: "/registry/test",
				SkippedPrefixes: []string{
					"/registry/test/pods",
					"/registry/test/events",
				},
			},
			[][]byte{
				encodeRevisionKey([]byte("/registry/test/")),
				encodeRevisionKey([]byte("/registry/test/events/")),
				encodeRevisionKey([]byte("/registry/test/events0")),
				encodeRevisionKey([]byte("/registry/test/pods/")),
				encodeRevisionKey([]byte("/registry/test/pods0")),
				encodeRevisionKey([]byte("/registry/test0")),
			},
		},
		{
			Config{
				Prefix: "/registry/test",
			},
			[][]byte{
				encodeRevisionKey([]byte("/registry/test/")),
				encodeRevisionKey([]byte("/registry/test0")),
			},
		},
	}
	for _, suit := range tests {
		b := &backend{config: suit.in, coder: coder.NewNormalCoder()}
		actual := b.getCompactBorders()
		if len(actual) != len(suit.expected) {
			t.Errorf("byte kv get compact borders return length %d is not equal to %d", len(actual), len(suit.expected))
		}
		for i := range actual {
			if !bytes.Equal(actual[i], suit.expected[i]) {
				t.Errorf("index %d actual %s expected %s ", i, actual[i], suit.expected[i])
			}
		}
	}
}

type storageWrapper struct {
	deleteUnavailableCallIdxes []int64
	deleteCalledTimes          int64
	storage.KvStorage
}

// Delete rewrite method of sdk to return error when the called times is matched
func (b *storageWrapper) Del(ctx context.Context, key []byte) (err error) {
	err = b.checkDelete()
	if err != nil {
		userKey, rev, _ := coder.NewNormalCoder().Decode(key)
		klog.InfoS("mock delete failed", "key", string(userKey), "rev", rev)
		return err
	}

	return b.KvStorage.Del(ctx, key)
}

func (b *storageWrapper) DelCurrent(ctx context.Context, iter storage.Iter) (err error) {
	err = b.checkDelete()
	if err != nil {
		userKey, rev, _ := coder.NewNormalCoder().Decode(iter.Key())
		klog.InfoS("mock delete failed", "key", string(userKey), "rev", rev)
		return err
	}

	return b.KvStorage.DelCurrent(ctx, iter)
}

func (b *storageWrapper) resetDeleteUnavailableCallIdxes(indexes []int64) {
	atomic.StoreInt64(&b.deleteCalledTimes, 0)
	b.deleteUnavailableCallIdxes = indexes
}

func (b *storageWrapper) checkDelete() error {
	times := atomic.AddInt64(&b.deleteCalledTimes, 1)
	klog.InfoS("check delete times", "curTimes", times)
	for _, t := range b.deleteUnavailableCallIdxes {
		if t == times {
			return context.DeadlineExceeded
		}
	}
	return nil
}

func newKVStorageForCompactTest(ast *assert.Assertions) (storage.KvStorage, *storageWrapper) {
	st := newTestRefactorTiKVStorage(ast)
	bcw := &storageWrapper{KvStorage: st}
	return bcw, bcw
}

func TestCompactConsistence(t *testing.T) {

	compactErrorCases := []struct {
		description  string
		errorIndexes []int64
	}{
		{
			description:  "err while compact revision key",
			errorIndexes: []int64{1},
		},
		{
			description:  "err while oldest object key revision key",
			errorIndexes: []int64{2},
		},
		{
			description:  "err while latest object key revision key",
			errorIndexes: []int64{5},
		},
	}

	for _, compactErrorCase := range compactErrorCases {
		t.Run(compactErrorCase.description, func(t *testing.T) {
			testCompactConsistence(t, compactErrorCase.errorIndexes)
		})
	}

}

func testCompactConsistence(t *testing.T, deleteErrorIndexes []int64) {
	ast := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metricsClient := mock.NewMinimalMetrics(ctrl)
	store, bcw := newKVStorageForCompactTest(ast)
	config := Config{
		Prefix:   prefix,
		Identity: getStorageIdentity(),
	}
	b := NewBackend(store, config, metricsClient)
	defer clear(ast, store, prefix)

	initRevision := b.GetCurrentRevision()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output, err := b.Watch(ctx, prefix+"/", 0) // must end with slash as prefix
	if !ast.NoError(err) {
		ast.FailNow(setWatcherError)
	}

	st := suite{
		kv:      store,
		backend: b,
		ctrl:    ctrl,
		ast:     ast,
		ctx:     ctx,
	}

	prefix := path.Join(prefix, "compact-consistence", t.Name())
	s := func(i int) string { return path.Join(prefix, strconv.Itoa(i)) }
	prepareCases := []testcase{
		&createTestcase{
			description:   "create1",
			putReq:        newCreateRequest(s(1), s(1)),
			expectedResp:  newCreateResponse(initRevision+1, true),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+1, newKeyValue(s(1), s(1), initRevision+1)),
		},
		&updateTestcase{
			description:   "update1-1",
			key:           s(1),
			value:         s(1),
			revision:      initRevision + 1,
			expectedResp:  newUpdateResponse(initRevision+2, true, nil),
			expectedEvent: newEvent(proto.Event_PUT, initRevision+2, newKeyValue(s(1), s(1), initRevision+2)),
		},
		&updateTestcase{
			description:   "update1-2",
			key:           s(1),
			value:         s(1),
			revision:      initRevision + 2,
			expectedResp:  newUpdateResponse(initRevision+3, true, nil),
			expectedEvent: newEvent(proto.Event_PUT, initRevision+3, newKeyValue(s(1), s(1), initRevision+3)),
		},
		&updateTestcase{
			description:   "update1-3",
			key:           s(1),
			value:         s(1),
			revision:      initRevision + 3,
			expectedResp:  newUpdateResponse(initRevision+4, true, nil),
			expectedEvent: newEvent(proto.Event_PUT, initRevision+4, newKeyValue(s(1), s(1), initRevision+4)),
		},
		&deleteTestcase{
			description:   "delete1",
			key:           s(1),
			expectedResp:  newDelResponse(initRevision+5, true, newKeyValue(s(1), s(1), initRevision+4)),
			expectedEvent: newEvent(proto.Event_DELETE, initRevision+5, newKeyValue(s(1), s(1), initRevision+4)),
		},
		&createTestcase{
			description:   "create2",
			putReq:        newCreateRequest(s(2), s(2)),
			expectedResp:  newCreateResponse(initRevision+6, true),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+6, newKeyValue(s(2), s(2), initRevision+6)),
		},
		&deleteTestcase{
			description:   "delete2-1",
			key:           s(2),
			expectedResp:  newDelResponse(initRevision+7, true, newKeyValue(s(2), s(2), initRevision+6)),
			expectedEvent: newEvent(proto.Event_DELETE, initRevision+7, newKeyValue(s(2), s(2), initRevision+6)),
		},
		&deleteTestcase{
			description:   "delete2-2",
			key:           s(2),
			expectedResp:  newDelResponse(initRevision+8, false, nil),
			expectedEvent: nil,
		},
	}

	count := func(ch <-chan *proto.StreamRangeResponse) int {
		c := 0
		for resp := range ch {
			c += len(resp.RangeResponse.Kvs)
			for _, kv := range resp.RangeResponse.Kvs {
				klog.InfoS("got", "key", string(kv.Key), "rev", kv.Revision)
			}
		}
		return c
	}

	// error while compact
	t.Run("prepare", func(t *testing.T) {
		for _, prepareCase := range prepareCases {
			prepareCase.run(t, st, output)
		}
	})

	t.Run("compact and check", func(t *testing.T) {
		ast := assert.New(t)
		bcw.resetDeleteUnavailableCallIdxes(deleteErrorIndexes)
		b.Compact(ctx, b.GetCurrentRevision()-1)

		ch, err := b.ListByStream(ctx, []byte(prefix), getEnd([]byte(prefix)), 0)
		ast.NoError(err)
		ast.Equal(0, count(ch))

		bcw.resetDeleteUnavailableCallIdxes([]int64{})
		b.Compact(ctx, b.GetCurrentRevision()-1)
		ch, err = b.ListByStream(ctx, []byte(prefix), getEnd([]byte(prefix)), 0)
		ast.NoError(err)
		ast.Equal(0, count(ch))
	})

}
