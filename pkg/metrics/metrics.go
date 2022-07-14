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

package metrics

import (
	"net/http"

	"google.golang.org/grpc"
)

func Tag(name, value string) T {
	return T{
		Name:  name,
		Value: value,
	}
}

type T struct {
	Name  string
	Value string
}

// Metrics abstracts metrics common define
type Metrics interface {

	// GetGrpcServerOption returns the grpc.ServerOptions for metrics interceptors
	GetGrpcServerOption() []grpc.ServerOption

	// GetHttpHandlers returns the map from path to handler register some handle for metrics
	GetHttpHandlers() map[string]http.Handler

	// EmitCounter emits the count
	EmitCounter(name string, value interface{}, tags ...T) error

	// EmitGauge emits the state
	EmitGauge(name string, value interface{}, tags ...T) error

	// EmitHistogram emits the histogram
	EmitHistogram(name string, value interface{}, tags ...T) error // histogram
}
