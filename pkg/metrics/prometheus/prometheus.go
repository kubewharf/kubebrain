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

package prometheus

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

var (
	registerer = prometheus.DefaultRegisterer
	gather     = prometheus.DefaultGatherer
)

type prometheusWrapper struct {
	globalLabelNames []string
	globalLabels     []metrics.T

	counterVecMu  sync.RWMutex
	counterVecMap map[string]*prometheus.CounterVec

	gaugeVecMu  sync.RWMutex
	gaugeVecMap map[string]*prometheus.GaugeVec

	histogramVecMu  sync.RWMutex
	histogramVecMap map[string]*prometheus.HistogramVec
}

// NewMetrics returns the prometheus implement of metrics.Metrics
func NewMetrics(globalLabels ...metrics.T) metrics.Metrics {
	pw := &prometheusWrapper{
		globalLabels:    globalLabels,
		counterVecMap:   make(map[string]*prometheus.CounterVec),
		gaugeVecMap:     make(map[string]*prometheus.GaugeVec),
		histogramVecMap: make(map[string]*prometheus.HistogramVec),
	}

	pw.globalLabelNames = pw.extractLabelNames(globalLabels)
	return pw
}

// GetHttpHandlers implements metrics.Metrics interface
func (pw *prometheusWrapper) GetHttpHandlers() map[string]http.Handler {
	return map[string]http.Handler{
		"/metrics": promhttp.Handler(),
	}
}

// GetGrpcServerOption implements metrics.Metrics interface
func (pw *prometheusWrapper) GetGrpcServerOption() []grpc.ServerOption {
	return GetGrpcServerOptions()
}

// EmitCounter implements metrics.Metrics interface
func (pw *prometheusWrapper) EmitCounter(name string, value interface{}, labels ...metrics.T) error {
	flt, _ := convert2float64(value)
	pw.mustGetCounterVec(name, labels).With(pw.labelsToMap(labels)).Add(flt)
	return nil
}

// EmitGauge implements metrics.Metrics interface
func (pw *prometheusWrapper) EmitGauge(name string, value interface{}, labels ...metrics.T) error {
	flt, _ := convert2float64(value)
	pw.mustGetGaugeVec(name, labels).With(pw.labelsToMap(labels)).Set(flt)
	return nil
}

// EmitHistogram implements metrics.Metrics interface
func (pw *prometheusWrapper) EmitHistogram(name string, value interface{}, labels ...metrics.T) error {
	flt, _ := convert2float64(value)
	pw.mustGetHistogramVec(name, labels).With(pw.labelsToMap(labels)).Observe(flt)
	return nil
}

func convert2float64(i interface{}) (float64, error) {
	switch s := i.(type) {
	case int:
		return float64(s), nil
	case float64:
		return s, nil
	case float32:
		return float64(s), nil
	case int64:
		return float64(s), nil
	case int32:
		return float64(s), nil
	case int16:
		return float64(s), nil
	case int8:
		return float64(s), nil
	case uint:
		return float64(s), nil
	case uint64:
		return float64(s), nil
	case uint32:
		return float64(s), nil
	case uint16:
		return float64(s), nil
	case uint8:
		return float64(s), nil
	case string:
		v, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return v, nil
		}
		return 0, fmt.Errorf("unable to cast %#v of type %T to float64", i, i)
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("unable to cast %#v of type %T to float64", i, i)
	}
}

func (pw *prometheusWrapper) labelsToMap(labels []metrics.T) (ret map[string]string) {
	ret = make(map[string]string)

	for _, label := range pw.globalLabels {
		ret[label.Name] = label.Value
	}

	for _, label := range labels {
		ret[label.Name] = label.Value
	}
	return
}

func (pw *prometheusWrapper) extractLabelNames(labels []metrics.T) (ret []string) {
	ret = make([]string, len(labels)+len(pw.globalLabelNames))
	copy(ret, pw.globalLabelNames)

	offset := len(pw.globalLabelNames)
	for i, label := range labels {
		ret[i+offset] = label.Name
	}

	return
}

func (pw *prometheusWrapper) mustGetGaugeVec(name string, labels []metrics.T) (vec *prometheus.GaugeVec) {
	// return direct if it's exist
	pw.gaugeVecMu.RLock()
	vec = pw.gaugeVecMap[name]
	pw.gaugeVecMu.RUnlock()
	if vec != nil {
		return vec
	}

	// create a new metric
	pw.gaugeVecMu.Lock()
	defer pw.gaugeVecMu.Unlock()

	// double check
	vec = pw.gaugeVecMap[name]
	if vec != nil {
		return vec
	}

	vec = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: formatName(name)}, pw.extractLabelNames(labels))
	registerer.MustRegister(vec)
	pw.gaugeVecMap[name] = vec
	return vec
}

func (pw *prometheusWrapper) mustGetCounterVec(name string, labels []metrics.T) (vec *prometheus.CounterVec) {
	// return direct if it's exist
	pw.counterVecMu.RLock()
	vec = pw.counterVecMap[name]
	pw.counterVecMu.RUnlock()
	if vec != nil {
		return vec
	}

	// create a new metric
	pw.counterVecMu.Lock()
	defer pw.counterVecMu.Unlock()

	// double check
	vec = pw.counterVecMap[name]
	if vec != nil {
		return vec
	}

	vec = prometheus.NewCounterVec(prometheus.CounterOpts{Name: formatName(name)}, pw.extractLabelNames(labels))
	registerer.MustRegister(vec)
	pw.counterVecMap[name] = vec
	return vec
}

func (pw *prometheusWrapper) mustGetHistogramVec(name string, labels []metrics.T) (vec *prometheus.HistogramVec) {
	// return direct if it's exist
	pw.histogramVecMu.RLock()
	vec = pw.histogramVecMap[name]
	pw.histogramVecMu.RUnlock()
	if vec != nil {
		return vec
	}

	// create a new metric
	pw.histogramVecMu.Lock()
	defer pw.histogramVecMu.Unlock()

	// double check
	vec = pw.histogramVecMap[name]
	if vec != nil {
		return vec
	}
	vec = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: formatName(name)}, pw.extractLabelNames(labels))
	registerer.MustRegister(vec)
	pw.histogramVecMap[name] = vec
	return vec
}

func formatName(name string) string {
	return strings.Replace(name, ".", "_", -1)
}
