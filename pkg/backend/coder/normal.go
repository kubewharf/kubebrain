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
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"github.com/pkg/errors"
)

var (
	magic      = "\x57\xfb\x80\x8b"
	magicBytes = []byte(magic)

	// splitKey must be less than [ 0-9 a-z A-Z - . _ ], so keys of one object are continuous
	splitKey  = string(splitByte)
	splitByte = byte('$')
)

func NewNormalCoder() Coder {
	return &normalEncoderDecoder{}
}

// normalEncoderDecoder encode user key with larger revision to larger internal key
type normalEncoderDecoder struct{}

// EncodeObjectKey implements Coder interface
func (n *normalEncoderDecoder) EncodeObjectKey(userKey []byte, revision uint64) []byte {
	key := make([]byte, len(magicBytes)+len(userKey)+1+8)
	// {magic}:{raw_key}:{split_key}:{revision}
	copy(key, magicBytes)
	copy(key[len(magicBytes):], userKey)
	copy(key[len(magicBytes)+len(userKey):], splitKey)
	binary.BigEndian.PutUint64(key[len(magicBytes)+len(userKey)+1:], revision)
	return key
}

// EncodeRevisionKey implements Coder interface
func (n *normalEncoderDecoder) EncodeRevisionKey(key []byte) []byte {
	return n.EncodeObjectKey(key, 0)
}

// Decode implements Coder interface
func (n *normalEncoderDecoder) Decode(internalKey []byte) (userKey []byte, revision uint64, err error) {
	if bytes.Compare(internalKey[:len(magicBytes)], magicBytes) != 0 {
		return nil, 0, errors.Errorf("magic number not right for object key %v", hex.EncodeToString(internalKey))
	}

	if internalKey[len(internalKey)-9] != splitByte {
		return nil, 0, errors.Errorf("split byte not right for object key %v", hex.EncodeToString(internalKey))
	}

	revision = binary.BigEndian.Uint64(internalKey[len(internalKey)-8:])
	userKey = internalKey[len(magic) : len(internalKey)-9]
	return
}
