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
	"context"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

// Scanner is used to control concurrent range in partitions for list or compact
type Scanner interface {

	// Range run scan in partitions concurrently
	Range(ctx context.Context, start []byte, end []byte, revision uint64, limit int64) ([]*proto.KeyValue, error)

	// RangeStream run scan in partitions concurrently and returns value by stream
	RangeStream(ctx context.Context, start []byte, end []byte, revision uint64) chan *proto.StreamRangeResponse

	// Count run scan in partitions concurrently and returns the count of user key
	Count(ctx context.Context, start []byte, end []byte, revision uint64) (int, error)

	// Compact will gc the kvs which are too old
	Compact(ctx context.Context, start []byte, end []byte, revision uint64)
}
