// Copyright 2023 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package auth

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"
)

func GetTLSConfig(certFile, keyFile, ca string) (tlsConfig *tls.Config, err error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		klog.ErrorS(err, "can not load key pair", "cert", certFile, "key", keyFile)
		return nil, errors.Wrapf(err, "can not load key pair")
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	// load ca file
	certPool := x509.NewCertPool()
	caFileBytes, err := os.ReadFile(ca)
	if err != nil {
		klog.ErrorS(err, "can not load ca cert", "ca", ca)
		return nil, errors.Wrapf(err, "can not load ca cert")
	}
	certPool.AppendCertsFromPEM(caFileBytes)
	tlsConfig.RootCAs = certPool

	klog.InfoS("init tls config success")
	return tlsConfig, nil
}
