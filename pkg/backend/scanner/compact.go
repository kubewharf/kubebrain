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
	"container/list"
	"time"
)

type compactRecord struct {
	revision uint64
	time     time.Time
}

type compactRecordQueue struct {
	list *list.List
}

func newCompactRecordQueue() *compactRecordQueue {
	return &compactRecordQueue{
		list: list.New(),
	}
}

func (c *compactRecordQueue) push(cr *compactRecord) {
	c.list.PushBack(cr)
}

func (c *compactRecordQueue) pop() {
	c.list.Remove(c.list.Front())
}

func (c *compactRecordQueue) head() *compactRecord {
	elem := c.list.Front()
	if elem == nil {
		return nil
	}
	return elem.Value.(*compactRecord)
}
