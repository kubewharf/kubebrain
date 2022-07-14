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

//go:build badger
// +build badger

package option

import (
	"github.com/spf13/pflag"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/badger"
)

type storageConfig struct {
	badger.Config
}

func newStorageConfig() *storageConfig {
	s := &storageConfig{}
	s.Dir = "./data"
	return s
}

func (s *storageConfig) addFlag(fs *pflag.FlagSet) {
	fs.StringVar(&s.Dir, "data-dir", s.Dir, "data dir of")
}

func (s *storageConfig) validate() error {
	return nil
}

func (s *storageConfig) buildStorage() (storage.KvStorage, error) {
	return badger.NewKvStorage(s.Config)
}
