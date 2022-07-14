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

package option

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/pflag"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/endpoint"
	imetrics "github.com/kubewharf/kubebrain/pkg/metrics"
	metrics "github.com/kubewharf/kubebrain/pkg/metrics/prometheus"
	storagemetrics "github.com/kubewharf/kubebrain/pkg/storage/metrics"
	"github.com/kubewharf/kubebrain/pkg/util"
)

type KubeBrainOption struct {
	epsConf *endpoint.Config

	// key prefix
	Prefix string

	// skipped key prefix
	// if specified, compact will ignore keys with this prefix.
	// used when kube-brain cluster split by object type but share one underlying storage cluster

	// TODO validate key by prefix when read and write
	SkippedPrefixes []string

	ClusterName string

	storageConfig *storageConfig

	EnableStorageMetrics bool
}

func NewOptions() *KubeBrainOption {
	return &KubeBrainOption{
		epsConf: &endpoint.Config{
			Port:                    2379,
			PeerPort:                2380,
			ClientSecurityConfig:    &endpoint.SecurityConfig{},
			PeerSecurityConfig:      &endpoint.SecurityConfig{},
			EnableEtcdCompatibility: false,
		},
		Prefix:        "",
		ClusterName:   "default",
		storageConfig: newStorageConfig(),
	}
}

// AddFlags adds flags to fs and binds them to options.
func (o *KubeBrainOption) AddFlags(fs *pflag.FlagSet) {
	// parse flags
	fs.IntVar(&o.epsConf.Port, "port", o.epsConf.Port, "the port kubebrain listen on for client")
	fs.IntVar(&o.epsConf.PeerPort, "peer-port", o.epsConf.PeerPort, "the port kubebrain listen on for peer communication")
	fs.IntVar(&o.epsConf.InfoPort, "info-port", o.epsConf.InfoPort, "the port kubebrain listen on for node info")
	fs.StringVar(&o.Prefix, "key-prefix", o.Prefix, "key prefix.")
	fs.StringSliceVar(&o.SkippedPrefixes, "skip-key-prefix", o.SkippedPrefixes, "skipped key prefix.")
	fs.StringVar(&o.ClusterName, "cluster-name", o.ClusterName, "cluster name.")

	// security
	fs.StringVar(&o.epsConf.ClientSecurityConfig.CertFile, "cert-file",
		o.epsConf.ClientSecurityConfig.CertFile, "Path to the client server ClientTLS cert file.")
	fs.StringVar(&o.epsConf.ClientSecurityConfig.KeyFile, "key-file",
		o.epsConf.ClientSecurityConfig.CertFile, "Path to the client server ClientTLS key file.")
	fs.StringVar(&o.epsConf.ClientSecurityConfig.CA, "trusted-ca-file",
		o.epsConf.ClientSecurityConfig.CA, "Path to the client server ClientTLS trusted CA cert file.")
	fs.BoolVar(&o.epsConf.ClientSecurityConfig.ClientAuth, "client-cert-auth",
		o.epsConf.ClientSecurityConfig.ClientAuth, "Enable client cert authentication.")
	fs.BoolVar(&o.epsConf.ClientSecurityConfig.AllowInsecure, "allow-insecure",
		o.epsConf.ClientSecurityConfig.AllowInsecure, "Allow insecure access even if client TLS config is set.")
	fs.StringVar(&o.epsConf.PeerSecurityConfig.CertFile, "peer-cert-file",
		o.epsConf.PeerSecurityConfig.CertFile, "Path to the peer server ClientTLS cert file.")
	fs.StringVar(&o.epsConf.PeerSecurityConfig.KeyFile, "peer-key-file",
		o.epsConf.PeerSecurityConfig.CertFile, "Path to the peer server ClientTLS key file.")
	fs.StringVar(&o.epsConf.PeerSecurityConfig.CA, "peer-trusted-ca-file",
		o.epsConf.PeerSecurityConfig.CA, "Path to the peer server ClientTLS trusted CA cert file.")
	fs.BoolVar(&o.epsConf.PeerSecurityConfig.ClientAuth, "peer-client-cert-auth",
		o.epsConf.PeerSecurityConfig.ClientAuth, "Enable client cert authentication.")
	fs.BoolVar(&o.epsConf.PeerSecurityConfig.AllowInsecure, "peer-allow-insecure",
		o.epsConf.ClientSecurityConfig.AllowInsecure, "Allow insecure access even if peer TLS config is set.")
	fs.BoolVar(&o.epsConf.EnableEtcdCompatibility, "compatible-with-etcd",
		o.epsConf.EnableEtcdCompatibility, "Enable full compatibility with usage of etcd3 in kube-apiserver")

	fs.BoolVar(&o.EnableStorageMetrics, "enable-storage-metrics", o.EnableStorageMetrics, "enable storage metrics.")
	o.storageConfig.addFlag(fs)
}

// Validate checks the option before running
func (o *KubeBrainOption) Validate() error {
	err := o.epsConf.Validate()
	if err != nil {
		return err
	}

	if strings.HasSuffix(o.Prefix, "/") {
		return fmt.Errorf("prefix %s is invalid, make sure it has no / suffix", o.Prefix)
	}

	for _, skippedPrefix := range o.SkippedPrefixes {
		if !strings.HasPrefix(skippedPrefix, o.Prefix) {
			return fmt.Errorf("skipped prefix %s has no prefix %s", skippedPrefix, o.Prefix)
		}
		if strings.HasSuffix(skippedPrefix, "/") {
			return fmt.Errorf("skipped prefix %s is invalid, make sure it has no / suffix", skippedPrefix)
		}
	}

	return o.storageConfig.validate()
}

// Run runs the storage engine
func (o *KubeBrainOption) Run(ctx context.Context) error {
	// add cluster metric tag
	metricsCli := metrics.NewMetrics(imetrics.Tag("cluster", o.ClusterName))

	localIP := util.GetHost()
	if len(localIP) == 0 {
		return fmt.Errorf("local ip is empty")
	}
	identity := fmt.Sprintf("%s:%d", localIP, o.epsConf.PeerPort)
	klog.InfoS("build identity", "identity", identity)

	kv, err := o.storageConfig.buildStorage()
	if err != nil {
		return err
	}

	config := backend.Config{
		Prefix:                  o.Prefix,
		Identity:                identity,
		SkippedPrefixes:         o.SkippedPrefixes,
		EnableEtcdCompatibility: o.epsConf.EnableEtcdCompatibility,
	}

	if o.EnableStorageMetrics {
		kv = storagemetrics.NewKvStorage(kv, metricsCli)
	}

	b := backend.NewBackend(kv, config, metricsCli)
	return endpoint.NewEndpoint(b, metricsCli, o.epsConf).Run(ctx)
}
