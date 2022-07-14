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

// Coder defines the interface that transforms user key to and from internal key
type Coder interface {
	// EncodeObjectKey encodes user key to internal object key
	EncodeObjectKey(key []byte, revision uint64) []byte

	// EncodeRevisionKey encodes user key to internal revision key
	// todo: deprecated
	EncodeRevisionKey(key []byte) []byte

	// Decode decodes internal key to user key and revision
	Decode(internalKey []byte) (userKey []byte, revision uint64, err error)
}
