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

package mock

import (
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

func TestNewMinimalMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMinimalMetrics(ctrl)

	t.Run("no tag", func(t *testing.T) {
		var tags []metrics.T
		m.EmitGauge("a", "b", tags...)
		m.EmitHistogram("a", "b", tags...)
		m.EmitCounter("a", "b", tags...)
	})

	t.Run("1 tag", func(t *testing.T) {
		tags := []metrics.T{metrics.Tag("a", "b")}
		m.EmitGauge("a", "b", tags...)
		m.EmitHistogram("a", "b", tags...)
		m.EmitCounter("a", "b", tags...)
	})

	t.Run("1+ tag", func(t *testing.T) {
		tags := []metrics.T{metrics.Tag("a", "b"), metrics.Tag("a", "b")}
		m.EmitGauge("a", "b", tags...)
		m.EmitHistogram("a", "b", tags...)
		m.EmitCounter("a", "b", tags...)
	})

}
