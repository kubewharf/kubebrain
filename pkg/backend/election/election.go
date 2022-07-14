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

package election

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// ResourceLockManager is the manager provide resource lock based on common storage
type ResourceLockManager interface {
	// GetResourceLock should base on storage.KvStorage
	GetResourceLock() resourcelock.Interface
}

func getElectionKey(prefix string) []byte {
	return []byte(fmt.Sprintf("%s/election", prefix))
}

type Config struct {
	Prefix   string
	Identity string
	Timeout  time.Duration
}

// NewResourceLockManager build a manager for resource lock
func NewResourceLockManager(config Config, store storage.KvStorage) ResourceLockManager {
	return &resourceLockManager{
		resourceLock: &resourceLock{
			store: store,
			lockConfig: resourcelock.ResourceLockConfig{
				Identity: config.Identity,
			},
			electionKey: getElectionKey(config.Prefix),
			timeout:     config.Timeout,
		},
	}
}

type resourceLockManager struct {
	*resourceLock
}

// GetResourceLock implements ResourceLockManager interface
func (r *resourceLockManager) GetResourceLock() resourcelock.Interface {
	return r.resourceLock
}

type resourceLock struct {
	store       storage.KvStorage
	lockConfig  resourcelock.ResourceLockConfig
	record      resourcelock.LeaderElectionRecord
	lastVal     []byte
	electionKey []byte
	tso         uint64
	timeout     time.Duration
}

// Get implements resourcelock.Interface
func (r *resourceLock) Get() (*resourcelock.LeaderElectionRecord, error) {
	klog.V(8).Info("[resource lock] get lock")

	err := r.getRecord()
	if err != nil {
		return nil, err
	}

	err = r.getTso()
	if err != nil {
		return nil, err
	}

	return &r.record, nil
}

func (r *resourceLock) getRecord() (err error) {
	ctx, cancel := r.genContext(context.Background())
	defer cancel()
	var val []byte
	val, err = r.store.Get(ctx, r.electionKey)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return apierrors.NewNotFound(schema.GroupResource{}, string(r.electionKey))
		}
		return err
	}
	r.lastVal = val
	var record resourcelock.LeaderElectionRecord
	if err := json.Unmarshal(val, &record); err != nil {
		return err
	}
	r.record = record
	return nil
}

func (r *resourceLock) getTso() (err error) {
	ctx, cancel := r.genContext(context.Background())
	defer cancel()
	r.tso, err = r.store.GetTimestampOracle(ctx)
	return err
}

// Create implements resourcelock.Interface
func (r *resourceLock) Create(ler resourcelock.LeaderElectionRecord) error {
	lerBytes, err := json.Marshal(ler)
	if err != nil {
		return err
	}
	batch := r.store.BeginBatchWrite()
	batch.PutIfNotExist(r.electionKey, lerBytes, 0)
	ctx, cancel := r.genContext(context.Background())
	defer cancel()
	err = batch.Commit(ctx)
	if err != nil {
		return err
	}
	r.lastVal = lerBytes
	r.tso, err = r.store.GetTimestampOracle(context.Background())
	return err
}

// Update implements resourcelock.Interface
func (r *resourceLock) Update(ler resourcelock.LeaderElectionRecord) error {
	klog.V(8).Info("[resource lock] update lock")
	if r.tso == 0 {
		return errors.New("endpoint not initialized, call get or create first")
	}

	recordBytes, err := json.Marshal(ler)
	if err != nil {
		return err
	}

	batch := r.store.BeginBatchWrite()
	batch.CAS(r.electionKey, recordBytes, r.lastVal, 0)
	ctx, cancel := r.genContext(context.Background())
	defer cancel()
	err = batch.Commit(ctx)
	if err != nil {
		return err
	}

	r.tso, err = r.store.GetTimestampOracle(context.Background())
	return err
}

// RecordEvent implements resourcelock.Interface
func (r *resourceLock) RecordEvent(s string) {
	klog.InfoS("record event", "identity", r.lockConfig.Identity, "events", s)
}

// Identity implements resourcelock.Interface
func (r *resourceLock) Identity() string {
	return r.lockConfig.Identity
}

func (r *resourceLock) Describe() string {
	if len(r.record.HolderIdentity) > 0 {
		return fmt.Sprintf("%s,%d", r.record.HolderIdentity, r.tso)
	}
	return fmt.Sprintf("empty,%d", r.tso)
}

func (r *resourceLock) genContext(ctx context.Context) (newCtx context.Context, cancel func()) {
	return context.WithTimeout(ctx, r.timeout)
}
