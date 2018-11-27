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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"math/rand"
)

var (
	_ kv.Transaction = (*fdbTxn)(nil)
)

var (
	keyEnd   = fdb.Key{0xFF}
	keyStart = fdb.Key("")
)

// fdbTxn implements kv.Transaction. It is thread safe.
type fdbTxn struct {
	id       int
	txn      fdb.Transaction
	snapshot fdb.Snapshot
	valid    bool
	dirty    bool
}

func newFdbTxn(t fdb.Transaction) *fdbTxn {
	return &fdbTxn{
		id:       int(rand.Int31()),
		txn:      t,
		snapshot: t.Snapshot(),
		valid:    true,
		dirty:    false,
	}
}

// Implement transaction interface

func (txn *fdbTxn) Get(k kv.Key) ([]byte, error) {
	log.Debugf("[kv] get key: %q txn %d", k, txn.txn, txn.id)
	value, err := txn.snapshot.Get(k).Get()
	if err == nil && value == nil {
		return nil, kv.ErrNotExist
	}
	return value, err
}

func (txn *fdbTxn) Set(k kv.Key, v []byte) error {
	log.Debugf("[kv] set %q txn %d", k, txn.id)
	txn.dirty = true
	txn.txn.Set(k, v)
	return nil
}

func (txn *fdbTxn) String() string {
	return "fdb_txn"
}

func (txn *fdbTxn) Seek(k kv.Key) (kv.Iterator, error) {
	log.Debugf("[kv] seek %q txn %d", k, txn.id)
	keyRange := fdb.KeyRange{k, keyEnd}
	rangeResult := txn.snapshot.GetRange(keyRange, fdb.RangeOptions{Reverse: false})
	iterator := rangeResult.Iterator()
	return newFdbIterator(iterator), nil
}

func (txn *fdbTxn) SeekReverse(k kv.Key) (kv.Iterator, error) {
	log.Debugf("[kv] seek prev %q txn %d", k, txn.id)
	keyRange := fdb.KeyRange{Begin: keyStart, End: k}
	rangeResult := txn.snapshot.GetRange(keyRange, fdb.RangeOptions{Reverse: true})
	iterator := rangeResult.Iterator()
	return newFdbIterator(iterator), nil
}

func (txn *fdbTxn) Delete(k kv.Key) error {
	log.Debugf("[kv] delete %q txn %d", k, txn.id)
	txn.dirty = true
	txn.txn.Clear(k)
	return nil
}

func (txn *fdbTxn) SetOption(opt kv.Option, val interface{}) {
	//txn.us.SetOption(opt, val)
}

func (txn *fdbTxn) DelOption(opt kv.Option) {
	//txn.us.DelOption(opt)
}

func (txn *fdbTxn) Commit() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	log.Debugf("[kv] start to commit txn %d", txn.id)
	txn.valid = false

	err := txn.txn.Commit().Get()
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	log.Debugf("[kv] commit successfully txn %d", txn.id)
	return nil
}

func (txn *fdbTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	log.Warnf("[kv] rollback txn %d", txn.id)
	txn.valid = false
	txn.txn.Cancel()
	return nil
}

func (txn *fdbTxn) LockKeys(keys ...kv.Key) error {
	for _, key := range keys {
		if err := txn.txn.AddWriteConflictKey(key); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (txn *fdbTxn) IsReadOnly() bool {
	return !txn.dirty
}

func (txn *fdbTxn) StartTS() uint64 {
	rv := txn.txn.GetReadVersion()
	return uint64(rv.MustGet())
}

func (txn *fdbTxn) GetClient() kv.Client {
	return &fdbClient{}
}

type fdbClient struct {
}

func (c *fdbClient) SupportRequestType(reqType, subType int64) bool {
	return false
}

func (c *fdbClient) Send(req *kv.Request) kv.Response {
	return nil
}
