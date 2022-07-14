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
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"strings"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

func (b *backend) Compact(ctx context.Context, revision uint64) (*proto.CompactResponse, error) {
	curRevision := b.tso.GetRevision()
	if revision == 0 || revision > curRevision {
		revision = curRevision
	}

	uncertainRev := b.asyncFifoRetry.MinRevision()
	if uncertainRev != 0 {
		// head of retry queue is the uncertain event with the least revision.
		// set compact revision less than the least uncertain revision if there is uncertain event
		// so that uncertain op will not be compacted.
		revision = minUint64(uncertainRev-1, revision)
	}

	err := b.compact(ctx, revision)
	if err != nil {
		klog.Errorf("backend compact with revision %d failed %v", revision, err)
	}
	compactResponse := &proto.CompactResponse{
		Header: responseHeader(revision),
	}
	return compactResponse, err
}

func (b *backend) compact(ctx context.Context, revision uint64) error {
	err := b.setCompactRecord(ctx, revision)
	if err != nil {
		return err
	}

	borders := b.getCompactBorders()
	for i := 0; i < len(borders); i += 2 {
		start := borders[i]
		end := borders[i+1]
		b.scanner.Compact(ctx, start, end, revision)
	}
	return nil
}

func (b *backend) setCompactRecord(ctx context.Context, revision uint64) error {
	// get stored compact revision
	val, err := b.kv.Get(ctx, getCompactKey(b.config.Prefix))
	if err != nil && err != storage.ErrKeyNotFound {
		klog.ErrorS(err, "get compact revision failed")
		return err
	}
	// stored compact revision is not nil
	if len(val) > 0 {
		// compare stored compact revision with current compact revision
		compactRevision := binary.BigEndian.Uint64(val)
		if compactRevision > revision {
			klog.InfoS("compact revision too large", "compactRev", compactRevision, "currentRev", revision)
			// revision has already been compacted
			return nil
		}
	}
	revisionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(revisionBytes, revision)
	batch := b.kv.BeginBatchWrite()
	if len(val) > 0 {
		// if compact revision already set before
		batch.CAS(getCompactKey(b.config.Prefix), revisionBytes, val, 0)
	} else {
		// compact revision firstly set
		batch.PutIfNotExist(getCompactKey(b.config.Prefix), revisionBytes, 0)
	}
	err = batch.Commit(ctx)
	b.metricCli.EmitCounter("backend.set_compact_revision", 1)
	if err != nil {
		klog.ErrorS(err, "set compact key failed", "revision", revision)
		b.metricCli.EmitCounter("backend.set_compact_revision.err", 1)
		return err
	}
	return nil
}

func (b *backend) getCompactBorders() [][]byte {
	// exclude skipped key prefix
	var keyPrefixes []string
	keyPrefixes = append(keyPrefixes, b.config.Prefix)
	keyPrefixes = append(keyPrefixes, b.config.SkippedPrefixes...)

	// construct compact borders
	var compactBorders [][]byte
	for _, key := range keyPrefixes {
		if !strings.HasSuffix(key, "/") {
			key = key + "/"
		}
		compactBorders = append(compactBorders, b.coder.EncodeObjectKey([]byte(key), 0))
		compactBorders = append(compactBorders, b.coder.EncodeObjectKey(PrefixEnd([]byte(key)), 0))
	}
	// sort to make sure compact in right range
	sort.Slice(compactBorders, func(i, j int) bool {
		return bytes.Compare(compactBorders[i], compactBorders[j]) < 0
	})
	return compactBorders
}
