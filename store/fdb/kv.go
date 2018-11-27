// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package fdbkv

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var (
	_ kv.Storage = (*fdbStore)(nil)
)

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*fdbStore
}

var mc storeCache

func init() {
	mc.cache = make(map[string]*fdbStore)
	rand.Seed(time.Now().UnixNano())
}

type fdbStore struct {
	mu        sync.Mutex
	uuid      string
	db        fdb.Database
}

func (s *fdbStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.db.CreateTransaction()
	if err != nil {
		return nil, errors.Trace(err)
	}
	txn := newFdbTxn(t)
	return txn, nil
}

func (s *fdbStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	txn, err := s.db.CreateTransaction()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newFdbSnapshot(txn), nil
}

func (s *fdbStore) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.cache, s.uuid)
	return nil
}

func (s *fdbStore) UUID() string {
	return s.uuid
}

func (s *fdbStore) CurrentVersion() (kv.Version, error) {
	txn, err := s.db.CreateTransaction()
	if err != nil {
		return kv.Version{Ver: 0}, errors.Trace(err)
	}
	defer txn.Cancel()

	version, err := txn.GetReadVersion().Get()
	if err != nil {
		return kv.Version{Ver: 0}, errors.Trace(err)
	}

	return kv.Version{Ver: uint64(version)}, nil
}

// Driver implements engine Driver.
type Driver struct {
}

// Open opens or creates an FoundationDB storage with given path.
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	uuid := fmt.Sprintf("fdb-default")
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	fdb.MustAPIVersion(600)

	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	s := &fdbStore{
		uuid:      uuid,
		db:        db,
	}
	mc.cache[uuid] = s
	return s, nil
}
