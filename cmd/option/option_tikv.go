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

//go:build !badger
// +build !badger

package option

import (
	"fmt"

	"github.com/spf13/pflag"

	"github.com/kubewharf/kubebrain/pkg/storage"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
)

type storageConfig struct {
	pdAddrs []string
}

func newStorageConfig() *storageConfig {
	return &storageConfig{}
}

func (s *storageConfig) addFlag(fs *pflag.FlagSet) {
	fs.StringSliceVar(&s.pdAddrs, "pd-addrs", s.pdAddrs, "addresses of TiKV PD servers")
}

func (s *storageConfig) validate() error {
	if len(s.pdAddrs) == 0 {
		return fmt.Errorf("invalid param --pd-addrs")
	}
	return nil
}

func (s *storageConfig) buildStorage() (storage.KvStorage, error) {
	return storagetikv.NewKvStorage(s.pdAddrs)
}
