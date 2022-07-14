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
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// GetGrpcServerOptions returns the grpc interceptor to collect metrics
func GetGrpcServerOptions() []grpc.ServerOption {
	grpc_prometheus.EnableHandlingTimeHistogram()
	return []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	}
}

// EmitMetrics convert prometheus metrics for
func EmitMetrics(metricsCli metrics.Metrics) {
	ticker := time.NewTicker(time.Second * 20)
	defer ticker.Stop()
	for {
		<-ticker.C
		_ = emitMetrics(metricsCli)
	}
}

func emitMetrics(metricsCli metrics.Metrics) error {
	const (
		leTag       = "le"
		quantileTag = "quantile"
	)

	mfs, err := gather.Gather()
	if err != nil {
		return err
	}
	for _, mf := range mfs {
		if mf.Type == nil {
			continue
		}
		for _, m := range mf.Metric {
			// construct labels
			tags := make([]metrics.T, 0, len(m.Label)+1)
			for i := range m.Label {
				if m.Label[i].Name != nil && m.Label[i].Value != nil && len(*m.Label[i].Name) > 0 && len(*m.Label[i].Value) > 0 {
					tags = append(tags, metrics.Tag(*m.Label[i].Name, *m.Label[i].Value))
				}
			}
			// counter type
			if *mf.Type == dto.MetricType_COUNTER && m.GetCounter() != nil && m.GetCounter().Value != nil {
				metricsCli.EmitGauge(mf.GetName(), *m.Counter.Value, tags...)
			} else if *mf.Type == dto.MetricType_GAUGE && m.GetGauge() != nil && m.GetGauge().Value != nil {
				metricsCli.EmitGauge(mf.GetName(), *m.Gauge.Value, tags...)
			} else if *mf.Type == dto.MetricType_HISTOGRAM && m.GetHistogram() != nil {
				tags = append(tags, metrics.Tag(leTag, ""))
				// histogram type
				var bucketSum uint64
				for _, bucket := range m.Histogram.Bucket {
					if bucket.UpperBound == nil || bucket.CumulativeCount == nil {
						continue
					}
					for i := range tags {
						if tags[i].Name == leTag {
							tags[i].Value = fmt.Sprintf("%f", *bucket.UpperBound)
							break
						}
					}
					metricsCli.EmitGauge(mf.GetName()+"_bucket", *bucket.CumulativeCount-bucketSum, tags...)
					bucketSum = *bucket.CumulativeCount
				}
			} else if *mf.Type == dto.MetricType_SUMMARY && m.GetSummary() != nil {
				tags = append(tags, metrics.Tag(quantileTag, ""))
				for _, q := range m.Summary.Quantile {
					if q.Quantile == nil || q.Value == nil {
						continue
					}
					for i := range tags {
						if tags[i].Name == quantileTag {
							tags[i].Value = fmt.Sprintf("%f", *q.Quantile)
							break
						}
					}
					metricsCli.EmitGauge(mf.GetName()+"_summary", *q.Value, tags...)
				}
				if m.GetSummary() != nil {
					metricsCli.EmitGauge(mf.GetName()+"_sum", m.GetSummary().GetSampleSum())
					metricsCli.EmitGauge(mf.GetName()+"_count", m.GetSummary().GetSampleCount())
				}
			}
		}
	}
	return nil
}
