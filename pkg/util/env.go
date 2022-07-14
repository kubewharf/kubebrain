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

package util

import (
	"os"
	"strings"

	"github.com/spf13/cast"
	"k8s.io/klog/v2"
)

const env = "KUBE_DEBUG"

const (
	envForceIPv6       = "ipv6_only"
	envSkipPrivateAddr = "skip_private_addr"
)

var (
	forceInIPv6Mode = false
	skipPrivateAddr = false
)

var klogLevel klog.Level = 4

func init() {
	s := os.Getenv(env)
	for _, input := range strings.Split(s, ",") {
		key, val := parseEnv(input)
		switch key {
		case envForceIPv6:
			forceInIPv6Mode = cast.ToBool(val)
		case envSkipPrivateAddr:
			skipPrivateAddr = cast.ToBool(val)
		}
	}
}

func parseEnv(s string) (key string, val string) {
	ss := strings.Split(s, "=")
	if len(ss) != 2 {
		return s, ""
	}
	return ss[0], ss[1]
}
