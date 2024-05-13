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

package endpoint

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/server"
)

type secureMode int

func (s secureMode) String() string {
	if s < 0 || int(s) >= len(secureModeStrings) {
		return "INVALID_SECURE_MODE"
	}
	return secureModeStrings[s]
}

const (
	modeOnlyInsecure secureMode = iota
	modeOnlySecure
	modeBothInsecureAndSecure
)

var secureModeStrings = []string{
	"ONLY_INSECURE",
	"ONLY_SECURE",
	"BOTH_INSECURE_AND_SECURE",
}

type Config struct {
	// Port is the listened port for client server
	Port int

	// PeerPort is the listened port for peer server
	PeerPort int

	// InfoPort is the listened port for info server
	InfoPort int

	// ClientSecurityConfig is the security config for client server
	ClientSecurityConfig *SecurityConfig

	// PeerSecurityConfig is the security config for peer server
	PeerSecurityConfig *SecurityConfig

	// EnableEtcdCompatibility is the flag if KubeWharf should try to be compatible with etcd3
	EnableEtcdCompatibility bool
}

func (c *Config) getServerConfig() server.Config {
	return server.Config{
		EnableEtcdProxy: c.EnableEtcdCompatibility,
		ClientTLS:       c.PeerSecurityConfig.getClientTLSConfig(),
	}
}

// SecurityConfig is the configuration of server tls
type SecurityConfig struct {

	// CertFile is the file path of server's cert
	CertFile string

	// KeyFile is the file path of server's private key
	KeyFile string

	// CA is the file path of ca's cert
	CA string

	// ClientAuth indicate if client certs should be verified
	ClientAuth bool

	// AllowInsecure indicates if server can be access in insecure mode without tls
	AllowInsecure bool

	serverTlsConfig *tls.Config
	clientTlsConfig *tls.Config
	once            sync.Once
	err             error
}

// Validate checks if config is valid
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("invalid config")
	}

	if c.Port == 0 {
		return fmt.Errorf("invalid port %d", c.Port)
	}

	if c.PeerPort == 0 || c.PeerPort == c.Port {
		return fmt.Errorf("invalid peer port %d", c.PeerPort)
	}

	if c.InfoPort != 0 && (c.InfoPort == c.Port || c.InfoPort == c.PeerPort) {
		return fmt.Errorf("invalid info port %d", c.InfoPort)
	}

	klog.InfoS("validate client security config", c.ClientSecurityConfig.ToKvs()...)
	err := c.ClientSecurityConfig.validate()
	if err != nil {
		klog.ErrorS(err, "invalid client security config")
		return err
	}

	klog.InfoS("validate peer security config", c.PeerSecurityConfig.ToKvs()...)
	err = c.PeerSecurityConfig.validate()
	if err != nil {
		klog.ErrorS(err, "invalid peer security config")
		return err
	}
	return nil
}

// ToKvs make config to kvs for klog
func (sc *SecurityConfig) ToKvs() []interface{} {
	if sc == nil {
		return []interface{}{}
	}

	return []interface{}{
		"cert", sc.CertFile,
		"key", sc.KeyFile,
		"ca", sc.CA,
		"clientAuth", strconv.FormatBool(sc.ClientAuth),
	}
}

func (sc *SecurityConfig) validate() error {
	if sc.isInsecure() {
		return nil
	}

	return sc.init()
}

func (sc *SecurityConfig) mode() secureMode {
	if sc.isInsecure() {
		return modeOnlyInsecure
	} else if sc.AllowInsecure {
		return modeBothInsecureAndSecure
	}
	return modeOnlySecure
}

func (sc *SecurityConfig) isInsecure() bool {
	if sc == nil {
		return true
	}

	return sc.CertFile == "" &&
		sc.KeyFile == "" &&
		sc.CA == "" &&
		sc.ClientAuth == false
}

func (sc *SecurityConfig) init() (err error) {
	if sc == nil {
		return nil
	}
	sc.once.Do(func() {
		// load cert
		cert, err := tls.LoadX509KeyPair(sc.CertFile, sc.KeyFile)
		if err != nil {
			klog.ErrorS(err, "can not load key pair", "cert", sc.CertFile, "key", sc.KeyFile)
			sc.err = errors.Wrapf(err, "can not load key pair")
			return
		}

		sc.serverTlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}

		sc.clientTlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}

		// load ca file
		if sc.CA != "" {
			certPool := x509.NewCertPool()
			caFileBytes, err := os.ReadFile(sc.CA)
			if err != nil {
				klog.ErrorS(err, "can not load ca cert", "ca", sc.CA)
				sc.err = errors.Wrapf(err, "can not load ca cert")
				return
			}
			certPool.AppendCertsFromPEM(caFileBytes)

			sc.serverTlsConfig.ClientAuth = tls.NoClientCert
			sc.serverTlsConfig.ClientCAs = certPool
			sc.serverTlsConfig.RootCAs = certPool

			sc.clientTlsConfig.RootCAs = certPool
		}

		if sc.CA != "" || sc.ClientAuth {
			sc.serverTlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
		return
	})
	return sc.err
}

func (sc *SecurityConfig) getServerTLSConfig() (ret *tls.Config) {
	_ = sc.init()
	return sc.serverTlsConfig
}

func (sc *SecurityConfig) getClientTLSConfig() (ret *tls.Config) {
	_ = sc.init()
	return sc.clientTlsConfig
}
