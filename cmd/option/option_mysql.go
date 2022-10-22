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

//go:build mysql
// +build mysql

package option

import (
	"github.com/spf13/pflag"

	"github.com/kubewharf/kubebrain/pkg/storage"
	storagemysql "github.com/kubewharf/kubebrain/pkg/storage/mysql"
)

type storageConfig struct {
	config storagemysql.Config
}

func newStorageConfig() *storageConfig {
	return &storageConfig{
		config: storagemysql.Config{
			UserName: "root",
			Password: "",
			URL:      "127.0.0.1:4000",
			DBName:   "kubebrain",
		},
	}
}

func (s *storageConfig) addFlag(fs *pflag.FlagSet) {
	fs.StringVar(&s.config.UserName, "db-username", s.config.UserName, "username of database")
	fs.StringVar(&s.config.Password, "db-password", s.config.Password, "password of database")
	fs.StringVar(&s.config.URL, "db-url", s.config.URL, "url of mysql")
	fs.StringVar(&s.config.DBName, "db-name", s.config.DBName, "name of database")
}

func (s *storageConfig) validate() error {
	return nil
}

func (s *storageConfig) buildStorage() (storage.KvStorage, error) {
	return storagemysql.NewKvStorage(s.config)
}
