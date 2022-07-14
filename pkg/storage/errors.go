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

package storage

import (
	"fmt"

	"github.com/pkg/errors"
)

var ErrUncertainResult = fmt.Errorf("uncertain result")

type errUncertainResult struct {
	originErr error
}

// Error implements error interface
func (e *errUncertainResult) Error() string {
	return fmt.Sprintf("uncertain error: %s", e.originErr.Error())
}

// Is implements interface for error compare
func (e *errUncertainResult) Is(err error) bool {
	return err == ErrUncertainResult || errors.Is(e.originErr, err)
}

// NewErrUncertainResult wraps an error as uncertain error
func NewErrUncertainResult(originErr error) error {
	return &errUncertainResult{
		originErr: originErr,
	}
}

// Conflict wraps the error of failed compare which causes WriteBatch can not be committed
type Conflict struct {
	// Idx is the index of failed compare in write batch
	Idx int

	// Key is the key of failed compare in write batch
	Key []byte

	// Val is the old value of failed compare in write batch
	Val []byte
}

// Is implement error compare
func (c *Conflict) Is(err error) bool {
	return err == ErrCASFailed
}

// Error implements error interface
func (c *Conflict) Error() string {
	return ErrCASFailed.Error()
}

// NewErrConflict generate a new conflict error for failed compare cause WriteBatch can not be committed
func NewErrConflict(idx int, key, val []byte) error {
	return &Conflict{
		Idx: idx,
		Key: key,
		Val: val,
	}
}
