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
	"sort"
	"sync"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

type Ring struct {
	s, e int64
	l    int
	arr  []*proto.Event
	sync.RWMutex
}

func NewRing(l int) *Ring {
	return &Ring{
		arr: make([]*proto.Event, l, l),
		l:   l,
	}
}

func (r *Ring) Add(event *proto.Event) {
	r.Lock()
	defer r.Unlock()
	r.arr[r.index(r.e)] = event
	if r.e == r.s+int64(r.l) {
		r.s++
	}
	r.e++
}

func (r *Ring) Size() int {
	return r.l
}

func (r *Ring) newest() *proto.Event {
	return r.arr[r.index(r.e-1)]
}

func (r *Ring) oldest() *proto.Event {
	return r.arr[r.index(r.s)]
}

func (r *Ring) Reset() {
	r.Lock()
	defer r.Unlock()
	r.s, r.e = 0, 0
}

func (r *Ring) isEmpty() bool {
	return r.e == 0
}

func (r *Ring) index(i int64) int {
	return int(i % int64(r.l))
}

type FindRet struct {
	empty  bool
	high   bool
	low    bool
	newest *proto.Event
	oldest *proto.Event
	events []*proto.Event
}

// FindEvents return event start from(inclusive)  revision
func (r *Ring) FindEvents(revision uint64) (ret *FindRet) {
	r.RLock()
	defer r.RUnlock()
	ret = &FindRet{}

	if r.isEmpty() {
		ret.empty = true
		return
	}

	ret.newest, ret.oldest = r.newest(), r.oldest()
	if revision > ret.newest.Revision {
		ret.high = true
		return
	}

	if revision < ret.oldest.Revision {
		ret.low = true
		return
	}

	idx := sort.Search(int(r.e-r.s), func(i int) bool {
		return r.arr[r.index(r.s+int64(i))].Revision >= revision
	})

	ret.events = make([]*proto.Event, int(r.e-r.s-int64(idx)), int(r.e-r.s-int64(idx)))

	if r.index(r.e) > r.index(r.s+int64(idx)) {
		copy(ret.events, r.arr[r.index(r.s+int64(idx)):r.index(r.e)])
		return
	}
	copy(ret.events, r.arr[r.index(r.s+int64(idx)):])
	copy(ret.events[r.l-r.index(r.s+int64(idx)):], r.arr[:r.index(r.e)])
	return ret
}
