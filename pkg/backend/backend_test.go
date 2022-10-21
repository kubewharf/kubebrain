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
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	ibadger "github.com/kubewharf/kubebrain/pkg/storage/badger"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
	imetrics "github.com/kubewharf/kubebrain/pkg/storage/metrics"
	imysql "github.com/kubewharf/kubebrain/pkg/storage/mysql"
	itikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
)

type storageType int

const (
	memKvStorage storageType = iota
	tiKvStorage
	badgerStorage
	metricsWrapper
	mysqlStorage
)

// test config
const (
	interval = 100 * time.Millisecond
	timeout  = 5 * time.Second
)

// msg
const (
	expectAEvent       = "expect a Event"
	expectAMvccPBEvent = "expect a mvccpb.Event"
	expectNoEvent      = "expect no Event"

	noExpectedEvent   = "no expected event"
	setWatcherError   = "set watcher error"
	backendIsNotReady = "backend is not ready"
)

var (
	skippedTests = []string{
		".*/range/native/count.*", // todo: release Count later
	}
	storages = map[string]storageType{
		//"memKv": memKvStorage,
		//"tiKv":  tiKvStorage,
		//"badger":          badgerStorage,
		//"metrics-wrapper": metricsWrapper,
		"mysql":           mysqlStorage,
	}
)

func checkSkip(t *testing.T) {
	for _, exp := range skippedTests {
		fmt.Println(t.Name())
		if b, _ := regexp.MatchString(exp, t.Name()); b {
			t.SkipNow()
		}
	}
}

type suite struct {
	backend Backend
	kv      storage.KvStorage
	metrics metrics.Metrics
	ctrl    *gomock.Controller
	ast     *assert.Assertions
	ctx     context.Context
}

func newTestSuites(t *testing.T, st storageType) (s suite, close func()) {

	ast := assert.New(t)
	ctrl := gomock.NewController(t)

	m := mock.NewMinimalMetrics(ctrl)
	kv := newTestKvStorage(t, ast, st, m)
	config := Config{
		Prefix:   prefix,
		Identity: getStorageIdentity(),
	}
	backend := NewBackend(kv, config, m)
	initRevision := uint64(time.Now().UnixNano())
	klog.InfoS("init", "revision", initRevision)
	backend.SetCurrentRevision(initRevision)
	ctx, cancel := context.WithCancel(context.Background())
	s = suite{
		backend: backend,
		kv:      kv,
		metrics: m,
		ctrl:    ctrl,
		ast:     ast,
		ctx:     ctx,
	}
	close = func() {
		ctrl.Finish()
		clear(ast, s.kv, prefix)
		err := kv.Close()
		ast.NoError(err)
		cancel()
	}
	return s, close
}

func getStorageIdentity() string {
	return path.Join(prefix, "identities", strconv.Itoa(int(atomic.AddInt64(&identities, 1))))
}

func newTestKvStorage(t *testing.T, ast *assert.Assertions, st storageType, m metrics.Metrics) storage.KvStorage {
	switch st {
	case memKvStorage:
		return imemkv.NewKvStorage()
	case tiKvStorage:
		return newTestRefactorTiKVStorage(ast)
	case badgerStorage:
		return newBadgerStorage(t, ast)
	case metricsWrapper:
		return imetrics.NewKvStorage(newBadgerStorage(t, ast), m)
	case mysqlStorage:
		return newTestMysqlStorage(ast)
	default:
		ast.FailNow("invalid storage")
		return nil
	}
}

func newBadgerStorage(t *testing.T, ast *assert.Assertions) storage.KvStorage {
	p := path.Join(t.TempDir(), "badger")
	st, err := ibadger.NewKvStorage(ibadger.Config{Dir: p})
	if err != nil {
		ast.FailNow(err.Error())
	}
	return st
}

func newTestRefactorTiKVStorage(ast *assert.Assertions) storage.KvStorage {
	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	ast.NoError(err)
	testutils.BootstrapWithMultiRegions(cluster)
	store, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	ast.NoError(err)
	return itikv.NewKvStoreWithStorage([]*tikv.KVStore{store})
}

func newTestMysqlStorage(ast *assert.Assertions) storage.KvStorage {
	store, err := imysql.NewKvStorage(imysql.Config{
		UserName: "root",
		Password: "",
		URL:      "127.0.0.1:4000",
		DBName:   "test",
		Debug:    true,
	})
	ast.NoError(err)
	return store
}

func newEvent(eventType proto.Event_EventType, rev uint64, cur *proto.KeyValue) *proto.Event {
	return &proto.Event{
		Type:     eventType,
		Revision: rev,
		Kv:       cur,
	}
}

func newKeyValue(key, value string, modRevision uint64) *proto.KeyValue {
	valueBytes := []byte(value)
	if value == "" {
		valueBytes = nil
	}
	return &proto.KeyValue{
		Key:      []byte(key),
		Revision: modRevision,
		Value:    valueBytes,
	}
}

func newCreateRequest(key, val string) *proto.CreateRequest {
	return &proto.CreateRequest{
		Key:   []byte(key),
		Value: []byte(val),
		Lease: 0,
	}
}

func newCreateResponse(revision uint64, succeeded bool) *proto.CreateResponse {
	return &proto.CreateResponse{
		Header:    responseHeader(revision),
		Succeeded: succeeded,
	}
}

func newUpdateResponse(revision uint64, succeeded bool, kv *proto.KeyValue) *proto.UpdateResponse {
	return &proto.UpdateResponse{
		Header:    responseHeader(revision),
		Succeeded: succeeded,
		Kv:        kv,
	}
}

func newGetResponse(revision uint64, kv *proto.KeyValue) *proto.GetResponse {
	return &proto.GetResponse{
		Header: responseHeader(revision),
		Kv:     kv,
	}
}
func newRangeResponse(revision uint64, kvs ...*proto.KeyValue) *proto.RangeResponse {
	return &proto.RangeResponse{
		Header: responseHeader(revision),
		Kvs:    kvs,
	}
}

func newLimitedRangeResponse(revision uint64, count int64, kvs ...*proto.KeyValue) *proto.RangeResponse {
	return &proto.RangeResponse{
		Header: responseHeader(revision),
		Kvs:    kvs,
		More:   int(count) > len(kvs),
	}
}

func newCountResponse(revision uint64, count int64) *proto.CountResponse {
	return &proto.CountResponse{
		Header: responseHeader(revision),
		Count:  uint64(count),
	}
}

func newDelResponse(revision uint64, succeeded bool, kv *proto.KeyValue) *proto.DeleteResponse {
	return &proto.DeleteResponse{
		Header:    responseHeader(revision),
		Kv:        kv,
		Succeeded: succeeded,
	}
}

func newRangeRequest(revision uint64, key string, rangeEnd string, limit int64) *proto.RangeRequest {
	var rangeEndBytes []byte
	if rangeEnd != "" {
		rangeEndBytes = []byte(rangeEnd)
	}
	return &proto.RangeRequest{
		Key:      []byte(key),
		End:      rangeEndBytes,
		Limit:    limit,
		Revision: revision,
	}
}

func newCountRequest(revision int64, key string) *proto.CountRequest {
	return &proto.CountRequest{
		Key: []byte(key),
		End: PrefixEnd([]byte(key)),
	}
}

func newGetRequest(revision uint64, key string) *proto.GetRequest {
	return &proto.GetRequest{
		Key:      []byte(key),
		Revision: revision,
	}
}

func newDelRequest(revision uint64, key string) *proto.DeleteRequest {
	return &proto.DeleteRequest{
		Key:      []byte(key),
		Revision: revision,
	}
}

func encodeRevisionKey(userKey []byte) (internalKey []byte) {
	cdr := coder.NewNormalCoder()
	return cdr.EncodeObjectKey(userKey, 0)
}

// clear list all key-value pairs with given prefix and delete them to reset data set in storage
func clear(ast *assert.Assertions, st storage.KvStorage, prefix string) {
	// remove key with given prefix in storage
	iter, err := st.Iter(context.Background(), encodeRevisionKey([]byte(prefix)), encodeRevisionKey(PrefixEnd([]byte(prefix))), 0, 0)
	ast.NoError(err)
	ctx := context.Background()
	for {
		err = iter.Next(ctx)
		if err != nil {
			ast.Equal(io.EOF, err)
			return
		}
		klog.InfoS("clear", "key", string(iter.Key()))
		batch := st.BeginBatchWrite()
		batch.DelCurrent(iter)
		err = batch.Commit(ctx)
		ast.NoError(err)
	}
}

func getEnd(prefix []byte) (end []byte) {
	end = make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	return
}

const (
	testKey = "/test/key"
	testVal = "/test/value"
)

var (
	testID     = fmt.Sprint(time.Now().UnixNano())
	prefix     = fmt.Sprintf("/kubebrain/unit_test/%s", testID)
	identities = int64(0)
)

type testcase interface {
	// run performs test logic and check return and event
	run(t *testing.T, s suite, output <-chan []*proto.Event)
}

type createTestcase struct {
	description   string
	putReq        *proto.CreateRequest
	expectedResp  *proto.CreateResponse
	expectedEvent *proto.Event
}

func (ct *createTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(ct.description, func(t *testing.T) {
		checkSkip(t)
		ast := assert.New(t)
		backend := s.backend
		resp, err := backend.Create(context.Background(), ct.putReq)
		ast.Equal(ct.expectedResp, resp)
		if ct.expectedResp == nil {
			ast.Error(err)
		} else {
			ast.NoError(err)
		}

		// sleep for a while and then check watcher
		waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
		if ct.expectedEvent != nil {
			waitUntilEventChanFilledOrTimeout(output)
			if !ast.Equal(1, len(output), expectAEvent) {
				ast.FailNow(noExpectedEvent)
			}
			actual := <-output
			if !ast.Equal(1, len(actual), expectAMvccPBEvent) {
				return
			}
			ast.Equal(ct.expectedEvent, actual[0])
			return
		}
		ast.Equal(0, len(output), expectNoEvent)
		return
	})

}

type deleteTestcase struct {
	description   string
	key           string
	expectedResp  *proto.DeleteResponse
	expectedEvent *proto.Event
}

func (dt *deleteTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(dt.description, func(t *testing.T) {
		checkSkip(t)
		ast := assert.New(t)
		backend := s.backend
		resp, err := backend.Delete(context.Background(), &proto.DeleteRequest{
			Key:      []byte(dt.key),
			Revision: 0,
		})
		ast.Equal(dt.expectedResp, resp)
		if dt.expectedResp == nil {
			ast.Error(err)
		} else {
			ast.NoError(err)
		}

		// sleep for a while and then check watcher
		waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
		if dt.expectedEvent != nil {
			waitUntilEventChanFilledOrTimeout(output)
			if !ast.Equal(1, len(output), expectAEvent) {
				ast.FailNow(noExpectedEvent)
			}
			actual := <-output
			if !ast.Equal(1, len(actual), expectAMvccPBEvent) {
				return
			}
			ast.Equal(dt.expectedEvent, actual[0])
			return
		}
		ast.Equal(0, len(output), expectNoEvent)
	})
}

type getTestcase struct {
	description  string
	getReq       *proto.GetRequest
	expectedResp *proto.GetResponse
}

func (gt *getTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(gt.description, func(st *testing.T) {
		checkSkip(st)
		backend := s.backend
		ast := assert.New(st)
		resp, err := backend.Get(context.Background(), gt.getReq)
		if resp != nil {
			ast.NoError(err)
		} else {
			ast.Error(err)
		}
		ast.Equal(gt.expectedResp, resp)

		time.Sleep(interval)
		ast.Equal(0, len(output), expectNoEvent)
	})
}

type rangeTestcase struct {
	description  string
	rangeReq     *proto.RangeRequest
	expectedResp *proto.RangeResponse
}

func (rt *rangeTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(rt.description, func(st *testing.T) {
		checkSkip(st)
		backend := s.backend
		ast := assert.New(st)

		resp, err := backend.List(context.Background(), rt.rangeReq)
		if resp != nil {
			ast.NoError(err)
		} else {
			ast.Error(err)
		}
		ast.Equal(rt.expectedResp, resp)

		time.Sleep(interval)
		ast.Equal(0, len(output), expectNoEvent)
	})
}

type countTestcase struct {
	description  string
	countReq     *proto.CountRequest
	expectedResp *proto.CountResponse
}

func (rt *countTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(rt.description, func(st *testing.T) {
		checkSkip(st)
		backend := s.backend
		ast := assert.New(st)

		resp, err := backend.Count(context.Background(), rt.countReq)
		if resp != nil {
			ast.NoError(err)
		} else {
			ast.Error(err)
		}
		ast.Equal(rt.expectedResp, resp)

		time.Sleep(interval)
		ast.Equal(0, len(output), expectNoEvent)
	})
}

type partitionsTestcase struct {
	description  string
	rangeReq     *proto.RangeRequest
	expectedResp *proto.RangeResponse
}

func (gpt *partitionsTestcase) run(t *testing.T, s suite, _ <-chan []*proto.Event) {
	t.Run(gpt.description, func(t *testing.T) {
		checkSkip(t)
		ast := assert.New(t)
		backend := s.backend
		lpReq := &proto.ListPartitionRequest{Key: gpt.rangeReq.Key, End: gpt.rangeReq.End}
		resp, err := backend.GetPartitions(s.ctx, lpReq)
		if gpt.expectedResp == nil {
			ast.Error(err)
			ast.Nil(resp)
			return
		}
		ast.NoError(err)
		ps := extractPartitions(resp)

		var kvs []*proto.KeyValue
		for i := 1; i < len(ps); i++ {
			wrCh, err := backend.ListByStream(s.ctx, ps[i-1], ps[i], 0)
			ast.NoError(err)
			for wr := range wrCh {
				kvs = append(kvs, wr.RangeResponse.Kvs...)
			}
		}
		ast.Equal(sortKvs(gpt.expectedResp.Kvs), sortKvs(kvs))
	})
}

func sortKvs(kvs []*proto.KeyValue) []*proto.KeyValue {
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	return kvs
}

func extractPartitions(resp *proto.ListPartitionResponse) [][]byte {
	ret := make([][]byte, len(resp.PartitionKeys))
	for idx, kv := range resp.PartitionKeys {
		ret[idx] = kv
	}
	return ret
}

type updateTestcase struct {
	description   string
	key           string
	value         string
	revision      uint64
	expectedResp  *proto.UpdateResponse
	expectedEvent *proto.Event
}

func (ut *updateTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(ut.description, func(t *testing.T) {
		checkSkip(t)
		backend := s.backend
		ast := assert.New(t)
		resp, err := backend.Update(context.Background(), &proto.UpdateRequest{
			Kv: &proto.KeyValue{
				Key:      []byte(ut.key),
				Value:    []byte(ut.value),
				Revision: ut.revision,
			},
			Lease: 0,
		})
		ast.Equal(ut.expectedResp, resp)
		if ut.expectedResp == nil {
			ast.Error(err)
		} else {
			ast.NoError(err)
		}

		// sleep for a while and then check watcher
		waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
		if ut.expectedEvent != nil {
			waitUntilEventChanFilledOrTimeout(output)
			if !ast.Equal(1, len(output), expectAEvent) {
				ast.FailNow(noExpectedEvent)
			}
			actual := <-output
			if !ast.Equal(1, len(actual), expectAMvccPBEvent) {
				return
			}
			ast.Equal(ut.expectedEvent, actual[0])
			return
		}
		ast.Equal(0, len(output), expectNoEvent)
	})
}

func testBackendCreate(t *testing.T, targetStorage storageType) {
	s, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := s.ast
	backend := s.backend

	testKey := path.Join(prefix, testKey)
	initRevision := backend.GetCurrentRevision()
	ctx, cancel := context.WithCancel(context.Background())
	output, err := backend.Watch(ctx, prefix+"/", 0) // must end with slash as prefix
	if !ast.NoError(err) {
		ast.FailNow(setWatcherError)
	}
	defer cancel()

	// Table Driven Tests
	testcases := []createTestcase{
		{
			description:   "create once",
			putReq:        newCreateRequest(testKey, testVal),
			expectedResp:  newCreateResponse(initRevision+1, true),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+1, newKeyValue(testKey, testVal, initRevision+1)),
		},
		{
			description:  "create twice",
			putReq:       newCreateRequest(testKey, path.Join(testVal, "2")),
			expectedResp: newCreateResponse(initRevision+2, false),
		},
	}

	for _, testcase := range testcases {
		testcase.run(t, s, output)
	}

}

func testBackendDelete(t *testing.T, targetStorage storageType) {
	s, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := s.ast
	backend := s.backend

	// prepare context
	testKey := path.Join(prefix, testKey)
	resp, err := backend.Create(context.Background(), newCreateRequest(testKey, testVal))
	if !ast.NoError(err) {
		ast.FailNow("can not preset kv pair")
	}

	// ! must sleep here to ensure async event has been processed
	waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
	initRevision := backend.GetCurrentRevision()
	if !ast.Equal(resp.Header.Revision, initRevision) {
		ast.FailNow(backendIsNotReady)
	}

	ctx, cancel := context.WithCancel(context.Background())
	output, err := backend.Watch(ctx, prefix+"/", 0)
	if !ast.NoError(err) {
		ast.FailNow(setWatcherError)
	}
	defer cancel()

	// Table Driven Tests
	// NOTICE: testcase depends on the sequence of running to generate context.
	//         please run all testcase in sequence but not only some of them while debugging.
	testcases := []deleteTestcase{
		{
			description: "key not found",
			key:         path.Join(prefix, "/test/key/not/found"),
			// todo(xueyingcai): delete key which doesn't exist in storage doesn't return error, so expect a non-nil expectedResp
			expectedResp: newDelResponse(initRevision+1, false, nil),
		},
		{
			description:   "delete success",
			key:           testKey,
			expectedResp:  newDelResponse(initRevision+2, true, newKeyValue(testKey, testVal, initRevision)),
			expectedEvent: newEvent(proto.Event_DELETE, initRevision+2, newKeyValue(testKey, testVal, initRevision)),
		},
		// todo(xueyingcai): add testcase about revision
	}

	for _, testcase := range testcases {
		testcase.run(t, s, output)
	}
}

func testBackendUpdate(t *testing.T, targetStorage storageType) {
	s, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := s.ast
	backend := s.backend

	// prepare context
	testKey := path.Join(prefix, testKey)
	initRevision := backend.GetCurrentRevision()

	ctx, cancel := context.WithCancel(context.Background())
	output, err := backend.Watch(ctx, prefix+"/", 0)
	if !ast.NoError(err) {
		ast.FailNow(setWatcherError)
	}
	defer cancel()

	// Table Driven Tests
	// NOTICE: testcase depends on the sequence of running to generate context.
	//         please run all testcase in sequence but not only some of them while debugging.
	testcases := []updateTestcase{
		{
			description:   "update a key which does not exist without revision",
			key:           testKey,
			value:         testVal,
			expectedResp:  newUpdateResponse(initRevision+1, true, nil),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+1, newKeyValue(testKey, testVal, initRevision+1)),
		},
		{
			description:  "update a key which exists without revision",
			key:          testKey,
			value:        testVal,
			expectedResp: newUpdateResponse(initRevision+2, false, newKeyValue(testKey, testVal, initRevision+1)),
		},
		{
			description:   "update a key which exists with valid revision",
			key:           testKey,
			value:         testVal,
			revision:      initRevision + 1,
			expectedResp:  newUpdateResponse(initRevision+3, true, nil), // valid revision should return PutResponse
			expectedEvent: newEvent(proto.Event_PUT, initRevision+3, newKeyValue(testKey, testVal, initRevision+3)),
		},
		{
			description: "update a key which exist with invalid revision",
			key:         testKey,
			value:       testVal,
			// todo
			revision:     initRevision + 1,
			expectedResp: newUpdateResponse(initRevision+4, false, newKeyValue(testKey, testVal, initRevision+3)),
		},
	}
	for _, testcase := range testcases {
		testcase.run(t, s, output)
	}
}

func testBackendRange(t *testing.T, targetStorage storageType) {

	s, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := s.ast
	backend := s.backend

	// prepare context
	testKey := path.Join(prefix, testKey)
	endKey := prefixEnd(testKey)
	format := func(prefix string, i int) string { return path.Join(prefix, fmt.Sprintf("%05d", i)) }
	const injectLen = 10 // decreasing `injectLen` should be carefully

	var resp *proto.CreateResponse
	var err error
	invalidRevision := backend.GetCurrentRevision()

	kvList := make([]*proto.KeyValue, injectLen)
	for i := 0; i < injectLen; i++ {
		key := format(testKey, i)
		val := format(testVal, i)
		resp, err = backend.Create(context.Background(), &proto.CreateRequest{
			Key:   []byte(key),
			Value: []byte(val),
			Lease: 0,
		})
		if !ast.NoError(err) {
			ast.FailNow("can not preset kv pair")
		}
		kvList[i] = newKeyValue(key, val, resp.Header.Revision)
	}
	// after preset, we have data below (rev means the latest revision)
	// 0. rev = ${latestRevision}
	// 1. key = f(i)
	// 2. val = g(i)
	// 3. revision = rev - (injectLen-1) + i
	//
	// revision  = rev-(injectLen-1)      rev-(injectLen-2)   rev-(injectLen-3)    ...     rev-1      rev
	// i         =       0                       1                   2             ...  injectLen-2  injectLen-1

	// ! must sleep here to ensure async event has been processed
	waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
	initRevision := backend.GetCurrentRevision()
	if !ast.Equal(resp.Header.Revision, initRevision) {
		ast.FailNow(backendIsNotReady)
	}

	ctx, cancel := context.WithCancel(context.Background())
	output, err := backend.Watch(ctx, prefix+"/", 0)
	if !ast.NoError(err) {
		ast.FailNow(setWatcherError)
	}
	defer cancel()

	t.Run("native", func(t *testing.T) {
		// ! NOTICE: range operation is run on the snapshot define by revision.
		// !         revision of KeyValue returned in RangeResponse <= revision in RangeRequest.
		testcases := []testcase{
			// * get
			// 1. no use of revision in api server now
			// 2. no use of revision in response
			&getTestcase{
				description:  "get a existent key with valid revision",
				getReq:       newGetRequest(0, format(testKey, injectLen-1)),
				expectedResp: newGetResponse(initRevision, newKeyValue(format(testKey, injectLen-1), format(testVal, injectLen-1), initRevision)),
			},
			&getTestcase{
				description:  "get a existent key with revision after it was created",
				getReq:       newGetRequest(initRevision, format(testKey, injectLen-2)),
				expectedResp: newGetResponse(initRevision, newKeyValue(format(testKey, injectLen-2), format(testVal, injectLen-2), initRevision-1)),
			},
			&getTestcase{
				description:  "get a existent key with revision before it was created",
				getReq:       newGetRequest(invalidRevision, format(testKey, injectLen-1)),
				expectedResp: newGetResponse(initRevision, nil),
			},
			&getTestcase{
				description:  "get a nonexistent key",
				getReq:       newGetRequest(0, format(testKey, -1)),
				expectedResp: newGetResponse(initRevision, nil),
			},
			// * list
			&rangeTestcase{
				description:  "list with prefix",
				rangeReq:     newRangeRequest(0, testKey, endKey, 0),
				expectedResp: newRangeResponse(initRevision, kvList...),
			},
			&rangeTestcase{
				description:  "list with range end",
				rangeReq:     newRangeRequest(0, testKey, format(testKey, injectLen-2), 0),
				expectedResp: newRangeResponse(initRevision, kvList[:injectLen-2]...),
			},
			&rangeTestcase{
				//! COMPATIBILITY PROBLEM:
				//! etcd expected below response
				//! expectedResp: newLimitedRangeResponse(initRevision, injectLen, kvList[:injectLen-4]...),
				// todo(xueyingcai): if limit is set and there is more number of kvs matching conditions,
				//                   etcd returns the count of all kvs that match the conditions,
				//                   but kv storage backend just try to scan at most limit+1 kvs that
				//                   matching conditions to avoid scanning all kvs.
				description:  "list with range end & limit",
				rangeReq:     newRangeRequest(0, testKey, format(testKey, injectLen-2), injectLen-4),
				expectedResp: newLimitedRangeResponse(initRevision, injectLen-3, kvList[:injectLen-4]...),
			},
			&rangeTestcase{
				description:  "list with invalid prefix",
				rangeReq:     newRangeRequest(0, endKey, format(endKey, injectLen-2), 0),
				expectedResp: newRangeResponse(initRevision), // diff: kvs []*KeyValue(nil) <=> []*KeyValue{}
			},
			&rangeTestcase{
				description: "list with invalid range end",
				rangeReq:    newRangeRequest(0, format(endKey, injectLen-2), endKey, 0),
			},
			&rangeTestcase{
				description: "list with range end & limit & revision",
				// revision is set to the 2nd input value from last
				rangeReq:     newRangeRequest(initRevision-2, format(testKey, 1), format(testKey, injectLen-1), injectLen-5),
				expectedResp: newLimitedRangeResponse(initRevision, injectLen-4, kvList[1:injectLen-4]...),
			},
			&rangeTestcase{
				description:  "list with dir prefix",
				rangeReq:     newRangeRequest(0, testKey, prefixEnd(testKey), injectLen-5),
				expectedResp: newLimitedRangeResponse(initRevision, injectLen-4, kvList[0:injectLen-5]...),
			},
			// * count
			// todo(xueyingcai): Count does not limit revision now
			&countTestcase{
				description:  "count with valid prefix",
				countReq:     newCountRequest(0, testKey),
				expectedResp: newCountResponse(initRevision, injectLen),
			},
			&countTestcase{
				description:  "count with invalid prefix",
				countReq:     newCountRequest(0, endKey),
				expectedResp: newCountResponse(initRevision, 0),
			},
		}
		for _, testcase := range testcases {
			testcase.run(t, s, output)
		}
	})

	// todo(xueyingcai): GetPartitions and RangeStream are tightly coupled right now, test individually after refactoring
	t.Run("partitions", func(t *testing.T) {
		testcases := []partitionsTestcase{
			{
				description:  "list with prefix",
				rangeReq:     newRangeRequest(0, testKey, endKey, 0),
				expectedResp: newRangeResponse(initRevision, kvList...),
			},
			{
				description:  "list with range end",
				rangeReq:     newRangeRequest(0, testKey, format(testKey, injectLen-2), 0),
				expectedResp: newRangeResponse(initRevision, kvList[:injectLen-2]...),
			},
		}

		for _, testcase := range testcases {
			testcase.run(t, s, nil)
		}
	})
}

func testBackendCompact(t *testing.T, targetStorage storageType) {
	// todo(xueyingcai): add test
	suite, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := suite.ast
	testKey := path.Join(prefix, testKey)
	{
		// create
		resp1, err := suite.backend.Create(suite.ctx, &proto.CreateRequest{
			Key:   []byte(testKey),
			Value: []byte(testVal),
			Lease: 0,
		})
		ast.NoError(err)
		newVal := path.Join(testVal, "new")
		rev1 := resp1.Header.Revision
		// update
		resp2, err := suite.backend.Update(suite.ctx, &proto.UpdateRequest{
			Kv: &proto.KeyValue{
				Key:      []byte(testKey),
				Value:    []byte(newVal),
				Revision: rev1,
			},
			Lease: 0,
		})
		ast.NoError(err)
		ast.Equal(newUpdateResponse(rev1+1, true, nil), resp2)
		rev2 := resp2.Header.Revision

		// wait for revision update
		waitUntilRevisionEqualOrTimeout(suite.backend, resp2.GetHeader().GetRevision())

		// get
		resp, err := suite.backend.Get(suite.ctx, newGetRequest(rev1, testKey))
		ast.NoError(err)
		ast.Equal(newGetResponse(rev2, newKeyValue(testKey, testVal, rev1)), resp)

		// compaction
		_, err = suite.backend.Compact(suite.ctx, 0)
		ast.NoError(err)

		// check
		resp, err = suite.backend.Get(suite.ctx, newGetRequest(rev1, testKey))
		ast.NoError(err)
		ast.Equal(newGetResponse(rev2, nil), resp)

		// delete
		dresp, err := suite.backend.Delete(suite.ctx, newDelRequest(0, testKey))
		ast.NoError(err)
		ast.True(dresp.Succeeded)

		// wait for revision update
		waitUntilRevisionEqualOrTimeout(suite.backend, dresp.GetHeader().GetRevision())

		// compaction
		_, err = suite.backend.Compact(suite.ctx, 0)
		ast.NoError(err)

		resp, err = suite.backend.Get(suite.ctx, newGetRequest(0, testKey))
		ast.NoError(err)
		ast.Equal(newGetResponse(dresp.Header.Revision, nil), resp)
	}
}

func testBackEnd(t *testing.T, st storageType) {
	t.Run("create", func(t *testing.T) {
		testBackendCreate(t, st)
	})

	t.Run("delete", func(t *testing.T) {
		testBackendDelete(t, st)
	})

	t.Run("update", func(t *testing.T) {
		testBackendUpdate(t, st)
	})

	t.Run("range", func(t *testing.T) {
		testBackendRange(t, st)
	})

	t.Run("compact", func(t *testing.T) {
		testBackendCompact(t, st)
	})

	t.Run("resource_lock", func(t *testing.T) {
		testBackendResourceLock(t, st)
	})

	t.Run("delete_and_create", func(t *testing.T) {
		testBackendDeleteAndCreate(t, st)
	})

	t.Run("write_and_watch", func(t *testing.T) {
		testBackendWriteAndWatch(t, st)
	})
}

type resourceLockTestWrapper struct {
	resourcelock.Interface
	sync.Locker
	*sync.Cond
}

func newResourceLockTestWrapper(itf resourcelock.Interface) *resourceLockTestWrapper {
	var locker sync.Locker = &sync.Mutex{}
	rtw := &resourceLockTestWrapper{
		Interface: itf,
		Cond:      sync.NewCond(locker),
		Locker:    locker,
	}
	return rtw
}

func (rtw *resourceLockTestWrapper) Create(ler resourcelock.LeaderElectionRecord) error {
	err := rtw.Interface.Create(ler)
	rtw.Signal()
	return err
}

func (rtw *resourceLockTestWrapper) Update(ler resourcelock.LeaderElectionRecord) error {
	err := rtw.Interface.Update(ler)
	rtw.Signal()
	return err
}

func (rtw *resourceLockTestWrapper) Get() (*resourcelock.LeaderElectionRecord, error) {
	ler, err := rtw.Interface.Get()
	fmt.Println(ler, err)
	rtw.Signal()
	return ler, err
}

func (rtw *resourceLockTestWrapper) WaitForUsed(atLeaseTimes int) {
	rtw.Lock()
	defer rtw.Unlock()
	for i := 0; i < atLeaseTimes; i++ {
		rtw.Wait()
	}
}

func testBackendResourceLock(t *testing.T, targetStorage storageType) {
	suiteA, closerA := newTestSuites(t, targetStorage)
	defer closerA()
	backendA := suiteA.backend
	backendB := NewBackend(suiteA.kv, Config{
		Prefix:   prefix,
		Identity: getStorageIdentity(),
	}, suiteA.metrics)

	ast := assert.New(t)

	rlA := newResourceLockTestWrapper(backendA.GetResourceLock())
	rlB := newResourceLockTestWrapper(backendB.GetResourceLock())

	var startLeadingCounter, stopLeadingCounter int64 = 0, 0
	var wg sync.WaitGroup
	lecA := leaderelection.LeaderElectionConfig{
		Name:            path.Join(prefix, "resource_lock"),
		Lock:            rlA,
		ReleaseOnCancel: true,
		RenewDeadline:   50 * interval,
		LeaseDuration:   80 * interval,
		RetryPeriod:     interval,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				fmt.Println("start")
				atomic.AddInt64(&startLeadingCounter, 1)
				wg.Done()
			},
			OnStoppedLeading: func() {
				fmt.Println("stop")
				atomic.AddInt64(&stopLeadingCounter, 1)
				wg.Done()
			},
		},
	}
	lecB := lecA
	lecB.Lock = rlB

	leA, err := leaderelection.NewLeaderElector(lecA)
	ast.NoError(err)
	leB, err := leaderelection.NewLeaderElector(lecB)
	ast.NoError(err)

	ctxA, cancelA := context.WithCancel(suiteA.ctx)
	ctxB, cancelB := context.WithCancel(context.Background())

	t.Run("acquire_a", func(t *testing.T) {
		ast := assert.New(t)
		wg.Add(1)
		go leA.Run(ctxA)

		wg.Wait()
		ast.Equal(int64(1), atomic.LoadInt64(&startLeadingCounter))
		ast.Equal(int64(0), atomic.LoadInt64(&stopLeadingCounter))
	})

	t.Run("acquire_b", func(t *testing.T) {
		ast := assert.New(t)

		go leB.Run(ctxB)

		// ensure signal is called more than twice to ensure Create and Update could be called
		rlB.WaitForUsed(2)
		ast.Equal(int64(1), atomic.LoadInt64(&startLeadingCounter))
		ast.Equal(int64(0), atomic.LoadInt64(&stopLeadingCounter))
	})

	t.Run("cancel_a", func(t *testing.T) {
		ast := assert.New(t)
		wg.Add(2)
		cancelA()

		wg.Wait()
		ast.Equal(int64(2), atomic.LoadInt64(&startLeadingCounter))
		ast.Equal(int64(1), atomic.LoadInt64(&stopLeadingCounter))
	})

	t.Run("cancel_b", func(t *testing.T) {
		ast := assert.New(t)
		wg.Add(1)
		cancelB()

		wg.Wait()
		ast.Equal(int64(2), atomic.LoadInt64(&startLeadingCounter))
		ast.Equal(int64(2), atomic.LoadInt64(&stopLeadingCounter))
	})

}

func testBackendDeleteAndCreate(t *testing.T, targetStorage storageType) {
	suite, closer := newTestSuites(t, targetStorage)
	defer closer()
	initRevision := suite.backend.GetCurrentRevision()
	ast := suite.ast

	ch, err := suite.backend.Watch(suite.ctx, prefix, 0)
	ast.NoError(err)

	testKey := path.Join(prefix, "delete/and/create")
	val1 := "val1"
	val2 := "val2"
	tcs := []testcase{
		&createTestcase{
			description:   "first create",
			putReq:        newCreateRequest(testKey, val1),
			expectedResp:  newCreateResponse(initRevision+1, true),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+1, newKeyValue(testKey, val1, initRevision+1)),
		},
		&deleteTestcase{
			description:   "delete",
			key:           testKey,
			expectedResp:  newDelResponse(initRevision+2, true, newKeyValue(testKey, val1, initRevision+1)),
			expectedEvent: newEvent(proto.Event_DELETE, initRevision+2, newKeyValue(testKey, val1, initRevision+1)),
		},
		&createTestcase{
			description:   "twice create",
			putReq:        newCreateRequest(testKey, val2),
			expectedResp:  newCreateResponse(initRevision+3, true),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+3, newKeyValue(testKey, val2, initRevision+3)),
		},
		&getTestcase{
			description:  "check",
			getReq:       newGetRequest(0, testKey),
			expectedResp: newGetResponse(initRevision+3, newKeyValue(testKey, val2, initRevision+3)),
		},
	}

	for _, tc := range tcs {
		tc.run(t, suite, ch)
	}
}

func testBackendWriteAndWatch(t *testing.T, targetStorage storageType) {

	suite, closer := newTestSuites(t, targetStorage)
	defer closer()
	initRevision := suite.backend.GetCurrentRevision()
	ast := suite.ast
	p := "create/and/watch"
	times := 10
	var cresp *proto.CreateResponse
	var err error
	for i := 0; i < times; i++ {
		testKey := path.Join(prefix, p, strconv.Itoa(i))
		cresp, err = suite.backend.Create(suite.ctx, newCreateRequest(testKey, testVal))
		if !ast.NoError(err) {
			ast.FailNow("can not preset kv pair")
		}
		ast.Equal(initRevision+uint64(i+1), cresp.Header.Revision)
		ast.True(cresp.Succeeded)
	}

	var dresp *proto.DeleteResponse
	for i := 0; i < times; i++ {
		testKey := path.Join(prefix, p, strconv.Itoa(i))
		dresp, err = suite.backend.Delete(suite.ctx, newDelRequest(initRevision+uint64(i+1), testKey))
		if !ast.NoError(err) {
			ast.FailNow("can not delete kv pair")
		}
		ast.Equal(initRevision+uint64(i+times+1), dresp.Header.Revision)
		ast.True(dresp.Succeeded)
	}

	waitUntilRevisionEqualOrTimeout(suite.backend, dresp.GetHeader().GetRevision())
	ctx, cancel := context.WithCancel(suite.ctx)
	_, err = suite.backend.Watch(ctx, prefix, initRevision)
	ast.Error(err)
	cancel()

	t.Run("check all events", func(t *testing.T) {
		ast := assert.New(t)
		events := getEventsFromRev(suite.ctx, suite.backend, initRevision+1, times*2)
		t.Logf("get events size=%d", len(events))
		// check create events
		for i := 0; i < times; i++ {
			event := events[i]
			ast.Equal(proto.Event_CREATE, event.Type)
			ast.Equal(initRevision+uint64(i+1), event.Revision)
			ast.True(strings.HasSuffix(string(event.Kv.Key), strconv.Itoa(i)))
			ast.Equal([]byte(testVal), event.Kv.Value)
		}

		// check delete events
		offset := times
		for i := 0; i < times; i++ {
			event := events[i+offset]
			ast.Equal(proto.Event_DELETE, event.Type)
			ast.Equal(initRevision+uint64(i+offset+1), event.Revision)
			ast.True(strings.HasSuffix(string(event.Kv.Key), strconv.Itoa(i)))
			ast.Equal([]byte(testVal), event.Kv.Value)
		}
	})

	t.Run("check delete event", func(t *testing.T) {
		events := getEventsFromRev(suite.ctx, suite.backend, initRevision+uint64(times+1), times)
		t.Logf("get events size=%d", len(events))
		// check delete events
		offset := times
		for i := 0; i < times; i++ {
			event := events[i]
			ast.Equal(proto.Event_DELETE, event.Type)
			ast.Equal(initRevision+uint64(i+offset+1), event.Revision)
			ast.True(strings.HasSuffix(string(event.Kv.Key), strconv.Itoa(i)))
			ast.Equal([]byte(testVal), event.Kv.Value)
		}
	})
}

func getEventsFromRev(ctx context.Context, b Backend, fromRev uint64, size int) []*proto.Event {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, _ := b.Watch(ctx, prefix, fromRev)
	allEvents := make([]*proto.Event, 0, size)
	for events := range ch {
		allEvents = append(allEvents, events...)
		if len(allEvents) == cap(allEvents) {
			cancel()
			break
		}
	}
	return allEvents
}

func TestUncertainRewrite(t *testing.T) {
	defaultRetryInterval, defaultCheckInterval := retryInterval, checkInterval
	retryInterval, checkInterval = 100*time.Millisecond, 100*time.Millisecond
	defer func() { retryInterval, checkInterval = defaultRetryInterval, defaultCheckInterval }()

	s, closer := newTestSuites(t, badgerStorage)
	defer closer()
	b := s.backend.(*backend)
	testKey := path.Join(prefix, testKey)
	initRev := b.GetCurrentRevision()
	invalidPrevRev := initRev - 1
	t.Log("init revision", initRev)
	{
		// 1
		t.Log("put while key doesn't exist, expect nothing change")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), initRev+1, invalidPrevRev, false, proto.Event_PUT, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+1)
		resp, err := b.Get(s.ctx, newGetRequest(0, testVal))
		s.ast.NoError(err)
		s.ast.Nil(resp.Kv)
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 2
		t.Log("del while key doesn't exist, expect nothing change")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), initRev+2, invalidPrevRev, false, proto.Event_DELETE, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+2)
		resp, err := b.Get(s.ctx, newGetRequest(0, testVal))
		s.ast.NoError(err)
		s.ast.Nil(resp.Kv)
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 3
		t.Log("write the key and expected no error")
		rev, err := b.create(s.ctx, []byte(testKey), []byte(testVal))
		s.ast.Equal(initRev+3, rev)
		s.ast.NoError(err)

		// 4
		t.Log("imitate the uncertain case, there should be an async retry to write the same data again")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), rev, 0, false, proto.Event_PUT, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+4)
		resp, err := b.Get(s.ctx, newGetRequest(0, testKey))
		s.ast.NoError(err)
		s.ast.NotNil(resp.Kv)
		s.ast.Equal(int64(initRev)+4, int64(resp.Kv.Revision))
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 5
		t.Log("imitate there is something changed and result is uncertain, expected prev revision is not equal to the current, just do nothing")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), initRev+5, invalidPrevRev, false, proto.Event_PUT, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+5)
		resp, err := b.Get(s.ctx, newGetRequest(0, testKey))
		s.ast.NoError(err)
		s.ast.NotNil(resp.Kv)
		s.ast.Equal(int64(initRev)+4, int64(resp.Kv.Revision))
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 6
		t.Log("delete")
		rev, kv, err := b.delete(s.ctx, 0, []byte(testKey))
		s.ast.NotNil(kv)
		s.ast.Equal(int64(initRev)+4, int64(kv.Revision))
		s.ast.Equal(int64(initRev)+6, int64(rev))
		s.ast.NoError(err)

		// 7
		t.Log("imitate the uncertain case, there should be an async retry to write the same data again, expected there is an async retry")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), rev, initRev+4, false, proto.Event_DELETE, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+7)
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 8
		t.Log("delete while there is uncertain result")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), initRev+8, invalidPrevRev, false, proto.Event_PUT, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+8)
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// check event
		watchCtx, cancel := context.WithCancel(s.ctx)
		t.Log("watch rev", initRev+4)
		ch, err := b.Watch(watchCtx, testKey, initRev+4)
		s.ast.NoError(err)

		expectedEvents := []*proto.Event{
			newEvent(proto.Event_PUT, initRev+4, newKeyValue(testKey, testVal, initRev+4)),
			// todo: if the event if right?
			newEvent(proto.Event_DELETE, initRev+7, newKeyValue(testKey, testVal, initRev+4)),
		}
		var actualEvents []*proto.Event

		for batch := range ch {
			actualEvents = append(actualEvents, batch...)
			for _, ev := range batch {
				klog.InfoS(ev.Type.String(), ev.Kv == nil)
			}

			if len(actualEvents) == len(expectedEvents) {
				break
			}
		}
		cancel()
		s.ast.Equal(expectedEvents, actualEvents)
	}
}

func waitUntilRetryQueueDrainOrTimeout(ctx context.Context, b *backend, expectedRev uint64) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if b.GetCurrentRevision() == expectedRev && b.asyncFifoRetry.Size() == 0 {
				return
			}
		}
	}
}

func TestBackend(t *testing.T) {
	for name, st := range storages {
		t.Run(name, func(t *testing.T) {
			testBackEnd(t, st)
		})
	}
}

func prefixEnd(p string) string {
	return string(PrefixEnd([]byte(p)))
}

func waitUntilEventChanFilledOrTimeout(eventChan <-chan []*proto.Event) {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if len(eventChan) != 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func waitUntilRevisionEqualOrTimeout(b Backend, expectedRev uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if b.GetCurrentRevision() == expectedRev {
				time.Sleep(interval)
				return
			}
		}
	}
}
