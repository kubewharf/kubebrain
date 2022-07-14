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

package coder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompatible(t *testing.T) {
	ast := assert.New(t)
	bs := []byte{87, 251, 128, 139, 47, 114, 101, 103, 105, 115, 116, 114, 121, 47, 116, 101, 115, 116, 36, 0, 0, 0, 0, 0, 0, 0, 0}
	c := NewNormalCoder()

	uk, rev, err := c.Decode(bs)
	ast.Equal("/registry/test",string(uk))
	ast.Equal(0, int(rev))
	ast.NoError(err)
}
