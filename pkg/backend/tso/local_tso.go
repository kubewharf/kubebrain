package tso

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync/atomic"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
	"k8s.io/klog/v2"
)

const (
	tsoKey            = "tso"
	timeout           = 500 * time.Millisecond
	maxRetryTimes     = 10
	retryInterVal     = 500 * time.Millisecond
	defaultBufferSzie = 5000
)

type TsoObject struct {
	MaxRev uint64 `json:"MaxRev"`
}

func NewLocalTSO(storage storage.KvStorage, prefix string) TSO {
	return &localTSO{
		storage:    storage,
		tsoKey:     []byte(path.Join(prefix, tsoKey)),
		tsoObject:  &TsoObject{},
		bufferSize: defaultBufferSzie,
	}
}

type localTSO struct {
	storage           storage.KvStorage
	committedRevision uint64
	dealRevision      uint64
	prevDealRevision  uint64
	tsoObject         *TsoObject
	tsoKey            []byte
	bufferSize        uint64
	sg                singleflight.Group
}

func (l *localTSO) init() error {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	val, err := l.storage.Get(ctx, l.tsoKey)
	if err != nil {
		if !errors.Is(err, storage.ErrKeyNotFound) {
			return err
		}

		ts := uint64(time.Now().UnixNano())
		l.dealRevision, l.committedRevision = ts, ts
		return nil
	}

	oldTsoObj := &TsoObject{}
	err = json.Unmarshal(val, oldTsoObj)
	if err != nil {
		// ? force overwrite
		ts := uint64(time.Now().UnixNano())
		l.dealRevision, l.committedRevision = ts, ts
		return nil
	}

	l.dealRevision, l.committedRevision = oldTsoObj.MaxRev, oldTsoObj.MaxRev
	return nil
}

func (l *localTSO) mustAllocateBuffer() {
	err := l.singleFlightAllocateBuffer()
	if err != nil {
		msg := fmt.Sprintf("can not allocate tso buffer err=\"%v\"", err.Error())
		klog.Fatalf(msg)
		return
	}
}

func (l *localTSO) singleFlightAllocateBuffer() error {
	_, err, _ := l.sg.Do("allocateBuffer", func() (interface{}, error) {
		return nil, l.allocateBuffer()
	})
	return err
}

func (l *localTSO) allocateBuffer() error {

	newMaxRev := atomic.LoadUint64(&l.dealRevision) + atomic.LoadUint64(&l.bufferSize)
	tsoObj := &TsoObject{}
	tsoObj.MaxRev = newMaxRev
	val, _ := json.Marshal(tsoObj)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	batch := l.storage.BeginBatchWrite()
	batch.Put(l.tsoKey, val, 0)
	err := batch.Commit(ctx)
	if err != nil {
		return err
	}

	atomic.StoreUint64(&l.tsoObject.MaxRev, newMaxRev)
	return nil

}

func (l *localTSO) run() (err error) {
	for {
		l.adjustAllocateBufferSize()
		l.mustAllocateBuffer()
		time.Sleep(retryInterVal)
	}
}

func (l *localTSO) adjustAllocateBufferSize() {

	dealRevision := atomic.LoadUint64(&l.dealRevision)
	if l.prevDealRevision == 0 {
		l.prevDealRevision = dealRevision
		return
	}

	l.bufferSize = (l.prevDealRevision - dealRevision) * 2
	return
}

func (l *localTSO) doWithRetry(f func() error) (err error) {
	for i := 0; i < maxRetryTimes; i++ {
		err = f()
		if err == nil {
			return nil
		}
		time.Sleep(retryInterVal)
	}
	return err
}

func (l *localTSO) Init(_ uint64) {
	err := l.doWithRetry(l.init)
	if err != nil {
		msg := "can not init local tso"
		klog.Fatal(msg)
		panic(msg)
	}

	l.mustAllocateBuffer()

	go l.run()
	return
}

// GetRevision implement TSO interface
func (l *localTSO) GetRevision() (maxCommittedRevision uint64) {
	return atomic.LoadUint64(&l.committedRevision)
}

// Deal implement TSO interface
func (l *localTSO) Deal() (revision uint64, err error) {
	klog.InfoS("deal")
	var prevRev, maxRev uint64

	f := func() bool {

		klog.InfoS("prev", "rev", prevRev)
		prevRev = atomic.LoadUint64(&l.dealRevision)
		maxRev = atomic.LoadUint64(&l.committedRevision)
		if prevRev == maxRev {
			l.mustAllocateBuffer()
		}
		return !atomic.CompareAndSwapUint64(&l.dealRevision, prevRev, prevRev+1)
	}
	// loop until success
	// assume it will not conflict too often
	for f() {
	}
	klog.InfoS("deal", "rev", prevRev+1)
	return prevRev + 1, nil
}

// Commit implement TSO interface
func (l *localTSO) Commit(revision uint64) {
	atomic.StoreUint64(&l.committedRevision, revision)
	// in case leader transfer, need to update tso and pre tso
	preTSO := atomic.LoadUint64(&l.dealRevision)
	if preTSO < revision {
		atomic.CompareAndSwapUint64(&l.dealRevision, preTSO, revision)
	}
}
