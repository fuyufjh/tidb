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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/ngaut/log"
)

var (
	_ kv.Snapshot = (*fdbSnapshot)(nil)
	_ kv.Iterator = (*fdbIterator)(nil)
)

// fdbSnapshot implements MvccSnapshot interface.
type fdbSnapshot struct {
	snapshot fdb.Snapshot
	txn      fdb.Transaction
	released bool
}

// newHBaseSnapshot creates a snapshot of an HBase store.
func newFdbSnapshot(txn fdb.Transaction) *fdbSnapshot {
	return &fdbSnapshot{
		snapshot: txn.Snapshot(),
		txn:      txn,
		released: false,
	}
}

// Get gets the value for key k from snapshot.
func (s *fdbSnapshot) Get(k kv.Key) ([]byte, error) {
	future := s.snapshot.Get(fdb.Key(k))
	v, err := future.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return v, nil
}

// BatchGet implements kv.Snapshot.BatchGet interface.
func (s *fdbSnapshot) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	futures := make([]fdb.FutureByteSlice, len(keys))
	for i, key := range keys {
		futures[i] = s.snapshot.Get(key)
	}
	m := make(map[string][]byte, len(keys))
	for i, future := range futures {
		key := keys[i]
		value, err := future.Get()
		if err != nil {
			return nil, errors.Trace(err)
		}
		m[string(key)] = value
	}
	return m, nil
}

func (s *fdbSnapshot) Seek(k kv.Key) (kv.Iterator, error) {
	result := s.snapshot.GetRange(fdb.KeyRange{Begin: k, End: keyEnd}, fdb.RangeOptions{})
	return newFdbIterator(result.Iterator()), nil
}

func (s *fdbSnapshot) SeekReverse(k kv.Key) (kv.Iterator, error) {
	return nil, kv.ErrNotImplemented
}

func (s *fdbSnapshot) Release() {
	if !s.released {
		s.txn.Cancel()
		s.released = true
	}
}

type fdbIterator struct {
	iter  *fdb.RangeIterator
	cur   fdb.KeyValue
	valid bool
}

func (it *fdbIterator) Close() {
	// do nothing
}

func (it *fdbIterator) Next() error {
	var err error
	it.valid = it.iter.Advance()
	if !it.valid {
		return nil
	}
	it.cur, err = it.iter.Get()
	if err != nil {
		return err
	}
	return nil
}

func (it *fdbIterator) Valid() bool {
	return it.valid
}

func (it *fdbIterator) Key() kv.Key {
	return kv.Key(it.cur.Key)
}

func (it *fdbIterator) Value() []byte {
	return []byte(it.cur.Value)
}

func newFdbIterator(iter *fdb.RangeIterator) kv.Iterator {
	iterator := &fdbIterator{
		iter:  iter,
		valid: true,
	}
	// in our implement the iterator need to call Next() before check Valid()
	err := iterator.Next()
	if err != nil {
		iterator.valid = false
		log.Error("iterator.Next(): " + err.Error())
	}
	return iterator
}
