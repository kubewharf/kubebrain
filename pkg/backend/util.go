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
	"encoding/binary"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend/scanner"
)

var (
	tombStoneBytes = []byte("tombstone")
	noPrefixEnd    = []byte{0}
	events         = []byte("/events/")
)

// retry config
var (
	retryInterval       = 5 * time.Second
	checkInterval       = time.Second
	eventsTTL     int64 = 3600
)

const (
	compactKey      = "compact_key"
	unaryRpcTimeout = 1 * time.Second
)

func (c Config) getScannerConfig() scanner.Config {
	// todo: expose TTL as args
	return scanner.Config{
		CompactKey: getCompactKey(c.Prefix),
		Tombstone:  tombStoneBytes,
		TTL:        time.Second * time.Duration(eventsTTL),
	}
}

func getCompactKey(prefix string) []byte {
	return []byte(fmt.Sprintf("%s/%s", prefix, compactKey))
}

type KeyVal struct {
	Key      []byte
	Val      []byte
	Revision uint64
}

func PrefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	// next prefix does not exist (e.g., 0xffff);
	// default to WithFromKey policy
	return noPrefixEnd
}

func uint64ToBytes(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

type Key []byte

func (k *Key) String() string {
	return string(*k)
}

const (
	klogLevel klog.Level = 10
)
