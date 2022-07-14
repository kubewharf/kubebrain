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

package tso

import (
	"sync/atomic"
)

// TSO is the controller of continuous revision windows
type TSO interface {

	// Init set the starting line for dealing
	Init(start uint64)

	// GetRevision returns the max committed continuous revision grow from init revision
	GetRevision() (maxCommittedRevision uint64)

	// Deal will generate a new revision for txn
	Deal() (revision uint64, err error)

	// Commit is used for notifying that txn with revision has been done
	Commit(revision uint64)
}

// todo: implement TSOController refer to tidb placement driver
// 		 https://github.com/tikv/pd/blob/master/server/tso

// naiveTSO is the allocator of revision based on the tso service of kv store
type naiveTSO struct {
	committedRevision uint64
	dealRevision      uint64
}

// GetRevision implement TSO interface
func (n *naiveTSO) GetRevision() (maxCommittedRevision uint64) {
	return atomic.LoadUint64(&n.committedRevision)
}

// Deal implement TSO interface
func (n *naiveTSO) Deal() (revision uint64, err error) {
	return atomic.AddUint64(&n.dealRevision, 1), err
}

// Commit implement TSO interface
func (n *naiveTSO) Commit(revision uint64) {
	// todo: CAS to ensure revision increase continuously
	//swapped := atomic.CompareAndSwapUint64(&n.committedRevision, revision-1, revision)
	//if !swapped {
	//	panic("committed revision must increase continuously")
	//}

	atomic.StoreUint64(&n.committedRevision, revision)
	// in case leader transfer, need to update tso and pre tso
	preTSO := atomic.LoadUint64(&n.dealRevision)
	if preTSO < revision {
		atomic.CompareAndSwapUint64(&n.dealRevision, preTSO, revision)
	}
}

// Init implement TSO interface
func (n *naiveTSO) Init(start uint64) {
	atomic.StoreUint64(&n.committedRevision, start)
	atomic.StoreUint64(&n.dealRevision, start)
}

func NewTSO() TSO {
	return &naiveTSO{}
}
