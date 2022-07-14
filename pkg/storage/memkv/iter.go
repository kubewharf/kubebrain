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

package memkv

import (
	"bytes"
	"context"
	"io"

	"github.com/huandu/skiplist"
)

type iter struct {
	store    *store
	start    []byte
	end      []byte
	backward bool
	limit    int
	idx      int
	buf      []kv
}

type kv struct {
	key []byte
	val []byte
}

func newIter(store *store, start []byte, end []byte, limit int) *iter {
	it := &iter{
		store: store,
		start: start,
		end:   end,
		limit: limit,
		idx:   -1,
	}
	it.init()
	return it
}

func (it *iter) init() {

	it.store.mu.Lock()
	var sentry *skiplist.Element
	defer func() {
		// clear sentry inserted for list
		if sentry != nil {
			it.store.skl.RemoveElement(sentry)
		}

		it.store.mu.Unlock()
	}()

	it.backward = bytes.Compare(it.start, it.end) < 0

	elem := it.store.skl.Get(it.start)
	if elem == nil {
		sentry = it.store.skl.Set(it.start, placeholder)
		if it.backward {
			elem = sentry.Next()
		} else {
			elem = sentry.Prev()
		}
	}

	for it.inRange(elem) {
		it.buf = append(it.buf, kv{key: elem.Key().([]byte), val: elem.Value.([]byte)})
		if it.backward {
			elem = elem.Next()
		} else {
			elem = elem.Prev()
		}
	}
	return
}

func (it *iter) inRange(elem *skiplist.Element) bool {
	if elem == nil {
		return false
	}

	key := elem.Key().([]byte)
	cmp := bytes.Compare(key, it.end)
	if it.backward {
		return cmp < 0
	} else {
		return cmp > 0
	}
}

// Key implements storage.Iter interface
func (it *iter) Key() []byte {
	return it.buf[it.idx].key
}

// Val implements storage.Iter interface
func (it *iter) Val() []byte {
	return it.buf[it.idx].val
}

var (
	placeholder = []byte("placeholder")
)

// Next implements storage.Iter interface
func (it *iter) Next(ctx context.Context) (err error) {
	it.idx++
	if it.idx >= len(it.buf) {
		return io.EOF
	}
	return nil
}

// Close implements storage.Iter interface
func (it *iter) Close() error {
	return nil
}
