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

package tikv

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type clientBalancer struct {
	clients []*txnkv.Client
	idx     uint64
}

const clientNum = 200

func NewKvStorage(pdAddrs []string) (storage.KvStorage, error) {
	clients := make([]*txnkv.Client, 0, clientNum)
	for i := 0; i < clientNum; i++ {
		txnClient, err := txnkv.NewClient(pdAddrs)
		if err != nil {
			closeClient(clients)
			return nil, errors.Wrap(err, "failed to create txn client")
		}
		clients = append(clients, txnClient)
	}
	s := NewKvStoreWithClient(clients)
	return s, nil
}

func NewKvStoreWithStorage(sts []*tikv.KVStore) storage.KvStorage {
	clients := make([]*txnkv.Client, 0, len(sts))
	for _, st := range sts {
		clients = append(clients, &txnkv.Client{KVStore: st})
	}
	return NewKvStoreWithClient(clients)
}

func NewKvStoreWithClient(clients []*txnkv.Client) storage.KvStorage {
	s := &store{
		clientBalancer: &clientBalancer{clients: clients},
		closed:         make(chan struct{}),
	}
	return s
}

func closeClient(clients []*txnkv.Client) {
	for _, client := range clients {
		_ = client.Close()
	}
}

// SupportTTL implements storage.KvStorage interface
func (s *store) SupportTTL() bool {
	return false
}

func (c *clientBalancer) getClient() *txnkv.Client {
	idx := atomic.AddUint64(&c.idx, 1)
	return c.clients[int(idx)%len(c.clients)]
}

type store struct {
	*clientBalancer
	closed chan struct{}
}

func (s *store) Del(ctx context.Context, key []byte) (err error) {
	b := s.BeginBatchWrite()
	b.Del(key)
	return b.Commit(ctx)
}

func (s *store) DelCurrent(ctx context.Context, iter storage.Iter) (err error) {
	b := s.BeginBatchWrite()
	b.DelCurrent(iter)
	return b.Commit(ctx)
}

func (s *store) GetTimestampOracle(ctx context.Context) (timestamp uint64, err error) {
	timestamp, err = s.getClient().GetOracle().GetTimestamp(ctx, oracleOption)
	if err != nil {
		return 0, errors.Wrap(err, "fail to get timestamp")
	}
	return timestamp, err
}

func maxBytes(a []byte, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

func minBytes(a []byte, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return b
	}
	return a
}

func (s *store) GetPartitions(ctx context.Context, start, end []byte) (partitions []storage.Partition, err error) {
	pdClient := s.getClient().GetPDClient()
	// scan regions without limit
	regs, err := pdClient.ScanRegions(ctx, start, end, -1)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get regions")
	}
	// ! StartKey of the first region and EndKey of the last region may be empty
	if len(regs) > 1 {
		partitions = make([]storage.Partition, len(regs))
		for idx, reg := range regs {

			if len(reg.Meta.StartKey) != 0 {
				partitions[idx].Start = maxBytes(reg.Meta.StartKey, start)
			} else {
				partitions[idx].Start = start
			}

			if len(reg.Meta.EndKey) != 0 {
				partitions[idx].End = minBytes(reg.Meta.EndKey, end)
			} else {
				partitions[idx].End = end
			}

		}
	} else {
		partitions = []storage.Partition{{Start: start, End: end}}
	}

	return partitions, err
}

type tiKvIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close()
}

func (s *store) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	var err error
	reverse := bytes.Compare(start, end) > 0
	if timestamp == 0 {
		timestamp, err = s.GetTimestampOracle(ctx)
		if err != nil {
			return nil, err
		}
	}
	snapshot := s.getClient().GetSnapshot(timestamp)
	var it tiKvIterator

	if !reverse {
		it, err = snapshot.Iter(start, end)
	} else {
		// iter of tikv failed to scan the start, so append \x00 to start to get it
		next := append(start, '\x00')
		it, err = snapshot.IterReverse(next)
	}

	if err != nil {
		return nil, errors.Wrap(err, "failed to create TiKV iter")
	}
	return &iter{iter: it, limit: int(limit), end: end, reverse: reverse}, err
}

func (s *store) BeginBatchWrite() storage.BatchWrite {
	b := &batch{}
	var err error
	f := func(ctx context.Context) error {
		return err
	}
	b.txn, err = s.getClient().Begin()
	b.list = append(b.list, f)
	return b
}

func (s *store) Get(ctx context.Context, key []byte) (val []byte, err error) {
	txn, err := s.getClient().Begin()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create read txn")
	}

	val, err = txn.Get(ctx, key)
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, storage.ErrKeyNotFound
		}
		return nil, errors.Wrapf(err, "failed to get key %s", string(key))
	}

	err = txn.Commit(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to commit read txn of key %s", string(key))
	}
	return val, nil
}

// Close implements storage.KvStorage interface
func (s *store) Close() error {
	close(s.closed)
	closeClient(s.clients)
	return nil
}

var oracleOption = &oracle.Option{TxnScope: oracle.GlobalTxnScope}
