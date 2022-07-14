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

package retry

import (
	"sync"
	"time"

	. "github.com/kubewharf/kubebrain/pkg/backend/common"
)

type eventQueue struct {
	mu        sync.RWMutex
	queueSize int
	head      *eventNode
	tail      *eventNode
}

func (e *eventQueue) size() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.queueSize
}

func (e *eventQueue) push(event *WatchEvent) {
	node := newEventNode(event)

	e.mu.Lock()
	defer e.mu.Unlock()

	e.queueSize++
	if e.head == nil {
		e.head = node
	}
	if e.tail != nil {
		e.tail.next = node
	}
	e.tail = node
}

func (e *eventQueue) pop() {

	e.mu.Lock()
	defer e.mu.Unlock()
	e.queueSize--
	if e.queueSize == 0 {
		e.head, e.tail = nil, nil
		return
	}
	e.head = e.head.next
}

func (e *eventQueue) getHead() *eventNode {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.head
}

type eventNode struct {
	event *WatchEvent
	time  time.Time
	next  *eventNode
}

func newEventNode(event *WatchEvent) *eventNode {
	return &eventNode{
		event: event,
		time:  time.Now(),
	}
}
