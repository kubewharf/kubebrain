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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	ast := assert.New(t)

	configs := []Config{
		{},
		{
			Port: 2379,
		},
		{
			Port:     2379,
			PeerPort: 2379,
		},
		{
			Port:                 2379,
			PeerPort:             2380,
			ClientSecurityConfig: &SecurityConfig{ClientAuth: true},
		},
		{
			Port:               2379,
			PeerPort:           2380,
			PeerSecurityConfig: &SecurityConfig{ClientAuth: true},
		},
	}

	for _, config := range configs {
		ast.Error(config.Validate())
	}

	conf := Config{
		Port:     2379,
		PeerPort: 2380,
		ClientSecurityConfig: &SecurityConfig{
			CertFile:      getAuthPath("server.crt"),
			KeyFile:       getAuthPath("server.key"),
			CA:            getAuthPath("ca.crt"),
			ClientAuth:    true,
			AllowInsecure: true,
		},
		PeerSecurityConfig: &SecurityConfig{
			CertFile:      getAuthPath("server.crt"),
			KeyFile:       getAuthPath("server.key"),
			CA:            getAuthPath("ca.crt"),
			ClientAuth:    true,
			AllowInsecure: true,
		},
	}

	ast.NoError(conf.Validate())
	ast.NotNil(conf.ClientSecurityConfig.getServerTLSConfig())
	ast.NotNil(conf.PeerSecurityConfig.getServerTLSConfig())
	ast.NotNil(conf.PeerSecurityConfig.getClientTLSConfig())
}
