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

package backend

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

var (
	testEventsTTL int64 = 1
)

func TestCompactExpiredEvents(t *testing.T) {
	defaultEventsTTL := eventsTTL
	eventsTTL = testEventsTTL
	defer func() {
		// recover
		eventsTTL = defaultEventsTTL
	}()

	s, c := newTestSuites(t, tiKvStorage)
	defer c()
	prefix := path.Join(prefix, "events")
	keys := []string{
		path.Join(prefix, "0"),
		path.Join(prefix, "1"),
		path.Join(prefix, "2"),
	}

	t.Run("create events", func(t *testing.T) {
		ast := assert.New(t)
		rev := s.backend.GetCurrentRevision()
		for _, key := range keys {
			resp, err := s.backend.Create(context.Background(), &proto.CreateRequest{
				Key:   []byte(key),
				Value: []byte(key),
			})
			ast.NoError(err)
			ast.True(resp.Succeeded)
			rev = resp.Header.Revision
		}

		// call compact to generate compact history
		waitUntilRevisionEqualOrTimeout(s.backend, rev)
		s.backend.Compact(context.Background(), rev)

		resp, err := s.backend.List(context.Background(), &proto.RangeRequest{
			Key: []byte(prefix),
			End: PrefixEnd([]byte(prefix)),
		})
		ast.NoError(err)
		for idx, kv := range resp.Kvs {
			ast.Equal(keys[idx], string(kv.Key))
		}
	})

	t.Run("compact after ttl and check", func(t *testing.T) {
		ast := assert.New(t)
		//! make the revision move on, otherwise calling of `Compact` will error
		wresp, err := s.backend.Create(context.Background(), &proto.CreateRequest{
			Key:   []byte(keys[0]),
			Value: []byte(keys[0]),
		})
		ast.NoError(err)
		ast.False(wresp.Succeeded)

		time.Sleep(time.Duration(eventsTTL) * 2 * time.Second)
		_, err = s.backend.Compact(context.Background(), wresp.Header.Revision)
		ast.NoError(err)
		resp, err := s.backend.List(context.Background(), &proto.RangeRequest{
			Key: []byte(prefix),
			End: PrefixEnd([]byte(prefix)),
		})

		ast.NoError(err)
		ast.Equal(0, len(resp.Kvs))
	})
}
