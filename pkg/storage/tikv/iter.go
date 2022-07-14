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
	"io"
)

type iter struct {
	iter    tiKvIterator
	count   int
	limit   int
	reverse bool
	end     []byte
	moved   bool
}

func (i *iter) Key() []byte {
	return i.iter.Key()
}

func (i *iter) Val() []byte {
	return i.iter.Value()
}

func (i *iter) Next(ctx context.Context) (err error) {

	if !i.iter.Valid() ||
		(i.limit > 0 && i.count >= i.limit) {
		return io.EOF
	}

	if !i.moved {
		i.moved = true
		return nil
	}

	i.count++
	err = i.iter.Next()
	if err != nil {
		return err
	} else if !i.iter.Valid() {
		return io.EOF
	}

	return i.checkBorder()
}

func (i *iter) checkBorder() error {
	key := i.iter.Key()
	if i.reverse && bytes.Compare(key, i.end) <= 0 {
		return io.EOF
	} else if !i.reverse && bytes.Compare(key, i.end) >= 0 {
		return io.EOF
	}
	return nil
}

func (i *iter) Close() error {
	i.iter.Close()
	return nil
}
