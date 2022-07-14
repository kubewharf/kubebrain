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
	"net/http"
	"net/http/pprof"
)

const httpPrefixPProf = "/debug/pprof"

func getPProfHandlers() map[string]http.Handler {

	m := make(map[string]http.Handler)
	m[httpPrefixPProf+"/"] = http.HandlerFunc(pprof.Index)
	m[httpPrefixPProf+"/profile"] = http.HandlerFunc(pprof.Profile)
	m[httpPrefixPProf+"/symbol"] = http.HandlerFunc(pprof.Symbol)
	m[httpPrefixPProf+"/cmdline"] = http.HandlerFunc(pprof.Cmdline)
	m[httpPrefixPProf+"/trace"] = http.HandlerFunc(pprof.Trace)
	m[httpPrefixPProf+"/heap"] = pprof.Handler("heap")
	m[httpPrefixPProf+"/goroutine"] = pprof.Handler("goroutine")
	m[httpPrefixPProf+"/threadcreate"] = pprof.Handler("threadcreate")
	m[httpPrefixPProf+"/block"] = pprof.Handler("block")
	m[httpPrefixPProf+"/mutex"] = pprof.Handler("mutex")

	return m
}
