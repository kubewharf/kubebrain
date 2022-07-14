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
	"context"

	"github.com/kubewharf/kubebrain/pkg/backend/common"
)

// AsyncFifoRetry is the interface to retry uncertain operation
type AsyncFifoRetry interface {
	// Run start retrying
	Run(ctx context.Context)

	// Append add Events
	Append(event *common.WatchEvent)

	// Size return the length of queue
	Size() int

	// MinRevision should return the revision of head
	// If queue is empty, return 0
	MinRevision() uint64
}
