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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

func TestRing(t *testing.T) {
	eq := assert.Equal
	eqv := assert.EqualValues
	at := assert.True
	r := NewRing(10)
	eq(t, r.Size(), 10)

	t.Run("Add0-10", func(t *testing.T) {
		t.Parallel()
		for i := 0; i < 10; i++ {
			r.Add(nE(i))
		}
	})
	t.Run("Add10-20", func(t *testing.T) {
		t.Parallel()
		for i := 10; i < 20; i++ {
			r.Add(nE(i))
		}
	})
	t.Run("FindEvents", func(t *testing.T) {
		t.Parallel()
		_ = r.FindEvents(11)
	})
	r.Reset()
	ret := r.FindEvents(5)
	at(t, ret.empty)
	r.Add(nE(1))
	eqv(t, r.oldest().Revision, 1)
	eqv(t, r.newest().Revision, 1)

	r.Reset()
	for i := 1; i <= 20; i++ {
		r.Add(nE(i))
	}

	tableTests := []struct {
		rev    uint64
		high   bool
		low    bool
		oldV   uint64
		newV   uint64
		events []uint64
	}{
		{9, false, true, 11, 20, []uint64{}},
		{10, false, true, 11, 20, []uint64{}},
		{11, false, false, 11, 20, []uint64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
		{12, false, false, 11, 20, []uint64{12, 13, 14, 15, 16, 17, 18, 19, 20}},
		{15, false, false, 11, 20, []uint64{15, 16, 17, 18, 19, 20}},
		{19, false, false, 11, 20, []uint64{19, 20}},
		{20, false, false, 11, 20, []uint64{20}},
		{21, true, false, 11, 20, []uint64{}},
		{30, true, false, 11, 20, []uint64{}},
	}

	convertRev := func(events []*proto.Event) []uint64 {
		actualEvs := make([]uint64, 0, len(events))
		for _, e := range events {
			actualEvs = append(actualEvs, e.Revision)
		}
		return actualEvs
	}

	for _, tcase := range tableTests {
		t.Run(fmt.Sprintf("Test FindEvents-%d", tcase.rev), func(t *testing.T) {
			ret := r.FindEvents(tcase.rev)
			eq(t, tcase.high, ret.high)
			eq(t, tcase.low, ret.low)
			eq(t, tcase.newV, ret.newest.Revision)
			eq(t, tcase.oldV, ret.oldest.Revision)
			eq(t, tcase.events, convertRev(ret.events))
		})
	}

	t.Run("NewRingFindEvents", func(t *testing.T) {
		r = NewRing(10)
		for i := 1; i <= 7; i++ {
			r.Add(nE(i))
		}
		ret = r.FindEvents(7)
		eq(t, ret.newest.Revision, uint64(7))
		eq(t, []uint64{7}, convertRev(ret.events))
	})
}

func BenchmarkRing(b *testing.B) {
	b.Run("Add", func(b *testing.B) {
		r := NewRing(200000)
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				r.Add(nE(i))
				i++
			}
		})
	})
	r := NewRing(200000)
	for i := 1; i < 200000+200; i++ {
		r.Add(nE(i))
	}
	b.Run("FindEvents", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				r.FindEvents(uint64(i%10000 + 180000))
				i++
			}
		})
	})
}

func nE(i int) *proto.Event {
	e := &proto.Event{
		Revision: uint64(i),
		Kv: &proto.KeyValue{
			Revision: uint64(i),
		},
	}
	return e
}
