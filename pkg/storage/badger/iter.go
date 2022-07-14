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

package badger

import (
	"bytes"
	"context"
	"io"

	"github.com/dgraph-io/badger"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type iter struct {
	txn     *badger.Txn
	iIter   *badger.Iterator
	reverse bool
	err     error
	start   []byte
	end     []byte
	limit   uint64
	counter uint64
	seeked  bool
}

func newIter(db *badger.DB, start, end []byte, ts uint64, limit uint64) storage.Iter {
	i := &iter{}
	i.start = start
	i.end = end
	i.txn = db.NewTransaction(false)
	i.reverse = bytes.Compare(start, end) > 0
	i.limit = limit
	i.iIter = i.txn.NewIterator(badger.IteratorOptions{Reverse: i.reverse})
	return i
}

func (i *iter) inRange() bool {
	if i.limit != 0 && i.counter >= i.limit {
		return false
	}

	i.counter++
	key := i.iIter.Item().Key()
	cmp := bytes.Compare(key, i.end)
	if i.reverse {
		return cmp > 0
	} else {
		return cmp < 0
	}
}

func (i *iter) Next(ctx context.Context) (err error) {
	if i.err != nil {
		return i.err
	}

	if i.seeked {
		i.iIter.Next()
	} else {
		i.iIter.Seek(i.start)
		i.seeked = true
	}

	if !i.iIter.Valid() || !i.inRange() {
		i.err = io.EOF
		return i.err
	}

	return
}

func (i *iter) Key() []byte {
	return i.iIter.Item().KeyCopy(nil)
}

func (i *iter) Val() (v []byte) {
	v, i.err = i.iIter.Item().ValueCopy(nil)
	return v
}

func (i *iter) Close() error {
	i.iIter.Close()
	defer i.txn.Discard()
	return i.txn.Commit()
}
