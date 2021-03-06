package internal

import (
	"errors"
	"github.com/xiaobogaga/fakedb2/util"
	"sync"
)

var transManagerLog = util.GetLog("transactionManager")

type TransactionManager struct {
	Lock    sync.Mutex
	TransId uint64
	Oracle  *Oracle
	Buf     *BufferManager
	Log     *LogManager
	Trans   map[uint64]*Transaction
}

func NewTransactionManager(startTransactionId uint64, bufManager *BufferManager,
	logManager *LogManager, oracle *Oracle) *TransactionManager {
	transManager := &TransactionManager{
		Lock:    sync.Mutex{},
		TransId: startTransactionId,
		Buf:     bufManager,
		Log:     logManager,
		Trans:   map[uint64]*Transaction{},
		Oracle:  oracle,
	}
	return transManager
}

type Transaction struct {
	Oracle        *Oracle
	TransactionId uint64
	Buf           *BufferManager
	HoldingLocks  map[int32]bool
	Lock          sync.Mutex
	Log           *LogManager
	WriteSet      map[int32]*WriteSet // page id -> WriteSet
	PrevLsn       int64
	StartTS       int64
	CommitTS      int64
}

type WriteSet struct {
	BeforeValue *Pair
	AfterValue  *Pair
}

func (trans *TransactionManager) NewTransaction() *Transaction {
	trans.Lock.Lock()
	defer trans.Lock.Unlock()
	ret := &Transaction{
		TransactionId: trans.TransId,
		Buf:           trans.Buf,
		HoldingLocks:  map[int32]bool{},
		Log:           trans.Log,
		PrevLsn:       InvalidLsn,
		WriteSet:      map[int32]*WriteSet{},
		Oracle:        trans.Oracle,
	}
	trans.Trans[ret.TransactionId] = ret
	transManagerLog.InfoF("create new transaction, txnId: %d.", ret.TransactionId)
	trans.TransId++
	return ret
}

var txnLog = util.GetLog("transaction")

func (txn *Transaction) Begin() {
	txnLog.InfoF("begin, txnId: %d", txn.TransactionId)
	ts := txn.Oracle.StartTs(txn.TransactionId)
	txn.StartTS = ts
	txn.CommitTS = InvalidTimeStamp
	log := &LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   InvalidLsn,
		TransactionId: txn.TransactionId,
		TP:            TransBeginLogType,
		StartTs:       txn.StartTS,
		CommitTs:      InvalidTimeStamp,
	}
	txn.Log.Append(log) // Begin Log
	txn.PrevLsn = log.LSN
}

func (txn *Transaction) GetChangedRows() (ret []int32) {
	for row, _ := range txn.WriteSet {
		ret = append(ret, row)
	}
	return ret
}

func (txn *Transaction) Commit() error {
	txnLog.InfoF("commit, txnId: %d", txn.TransactionId)
	rows := txn.GetChangedRows()
	commitTs, err := txn.Oracle.Commit(txn, txn.StartTS, rows)
	if err != nil {
		transManagerLog.InfoF("trans commit failed, err: %v", err)
		txn.Rollback()
		return err
	}
	txn.CommitTS = commitTs
	return nil
}

func (txn *Transaction) Rollback() {
	txnLog.InfoF("rollback, txnId: %d", txn.TransactionId)
	log := &LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   InvalidLsn,
		TransactionId: txn.TransactionId,
		TP:            TransAbortLogType,
		StartTs:       txn.StartTS,
		CommitTs:      InvalidTimeStamp,
	}
	txn.Log.Append(log) // Begin Log
	txn.PrevLsn = log.LSN
	txn.Undo(log.LSN)
	txn.WriteSet = map[int32]*WriteSet{}
	return
}

func (txn *Transaction) Undo(lsn int64) {
	for pageId, writeSet := range txn.WriteSet {
		txn.Buf.Undo(pageId, writeSet.BeforeValue.Key, writeSet.BeforeValue.Value, lsn, txn.StartTS, InvalidTimeStamp)
	}
	return
}

var txnLockConflict = errors.New("cannot acquire lock, now transaction aborted")

// Return whether found the key, the value of the key if found. error. Also return the index of the key: 0(not found), 1, 2
func (txn *Transaction) Get(key []byte) (int32, bool, []byte, error) {
	txnLog.InfoF("get, txnId: %d, key: %s", txn.TransactionId, string(key))
	found, value, err := txn.Buf.Get(1, key, txn.StartTS)
	if err != nil {
		return 0, false, nil, err
	}
	if found {
		return 1, true, value, nil
	}
	found, value, err = txn.Buf.Get(2, key, txn.StartTS)
	if err != nil {
		return 0, false, nil, err
	}
	if found {
		return 2, true, value, nil
	}
	return 0, false, nil, nil
}

func checkKeyValueSizeLimit(key []byte, value []byte) bool {
	if 8+4+len(key)+4+len(value) > PageSize {
		return false
	}
	return true
}

var KeyValueSizeTooLargeError = errors.New("key value is too large")

func (txn *Transaction) Set(key, value []byte) error {
	txnLog.InfoF("set, txnId: %d, key: %s, value: %s", txn.TransactionId, string(key), string(value))
	if !checkKeyValueSizeLimit(key, value) {
		return KeyValueSizeTooLargeError
	}
	id, found, v, err := txn.Get(key)
	if err != nil {
		return err
	}
	if !found {
		return txn.Add(key, value)
	}
	// Todo:
	// * support lock on txn.
	// * support before and after value.
	beforeValue := &Pair{Key: key, Value: v}
	afterValue := &Pair{Key: key, Value: value}
	log := &LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   InvalidLsn,
		TransactionId: txn.TransactionId,
		PageId:        int32(id),
		TP:            SetLogType,
		ActionTP:      SetAction,
		BeforeValue:   beforeValue.Serialize(),
		AfterValue:    afterValue.Serialize(),
		StartTs:       txn.StartTS,
		CommitTs:      InvalidTimeStamp,
	}
	log = txn.Log.Append(log)
	txn.PrevLsn = log.LSN
	err = txn.Buf.Set(id, key, value, log.LSN, txn.StartTS, log.CommitTs)
	if err != nil {
		return err
	}
	txn.AddSetWrite(id, afterValue, beforeValue)
	return nil
}

func (txn *Transaction) Add(key, value []byte) error {
	if txn.Buf.Size(txn.StartTS) >= 2 {
		return errors.New("full")
	}
	if !checkKeyValueSizeLimit(key, value) {
		return KeyValueSizeTooLargeError
	}
	txnLog.InfoF("add, txnId: %d, key: %s, value: %s", txn.TransactionId, string(key), string(value))
	pair := &Pair{Key: key, Value: value}
	emptyPair := &Pair{}
	if txn.Buf.IsEmpty(1, txn.StartTS) {
		log := &LogRecord{
			PrevLsn:       txn.PrevLsn,
			UndoNextLsn:   InvalidLsn,
			TransactionId: txn.TransactionId,
			PageId:        1,
			TP:            SetLogType,
			ActionTP:      AddAction,
			BeforeValue:   emptyPair.Serialize(),
			AfterValue:    pair.Serialize(),
			StartTs:       txn.StartTS,
			CommitTs:      InvalidTimeStamp,
		}
		txn.Log.Append(log)
		txn.PrevLsn = log.LSN
		txn.Buf.Set(1, key, value, log.LSN, txn.StartTS, log.CommitTs)
		txn.AddAddWrite(1, pair, emptyPair)
		return nil
	}
	log := &LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   InvalidLsn,
		TransactionId: txn.TransactionId,
		PageId:        2,
		TP:            SetLogType,
		ActionTP:      AddAction,
		BeforeValue:   emptyPair.Serialize(),
		AfterValue:    pair.Serialize(),
		StartTs:       txn.StartTS,
		CommitTs:      InvalidTimeStamp,
	}
	log = txn.Log.Append(log)
	txn.PrevLsn = log.LSN
	err := txn.Buf.Set(2, key, value, log.LSN, txn.StartTS, log.CommitTs)
	if err != nil {
		return err
	}
	txn.AddAddWrite(2, pair, emptyPair)
	return nil
}

var ErrKeyNotFound = errors.New("key not found")

func (txn *Transaction) Del(key []byte) error {
	txnLog.InfoF("del, txnId: %d, key: %s", txn.TransactionId, string(key))
	id, found, value, err := txn.Get(key)
	if err != nil {
		return err
	}
	if !found {
		return ErrKeyNotFound
	}
	pair := &Pair{Key: key, Value: value}
	emptyPair := &Pair{}
	log := &LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   InvalidLsn,
		TransactionId: txn.TransactionId,
		PageId:        int32(id),
		TP:            SetLogType,
		ActionTP:      DelAction,
		BeforeValue:   pair.Serialize(),
		AfterValue:    emptyPair.Serialize(),
		StartTs:       txn.StartTS,
		CommitTs:      InvalidTimeStamp,
	}
	log = txn.Log.Append(log)
	txn.PrevLsn = log.LSN
	err = txn.Buf.Del(id, log.LSN, txn.StartTS, log.CommitTs)
	if err != nil {
		return err
	}
	txn.AddDelWrite(id, emptyPair, pair)
	return nil
}

func (txn *Transaction) AddLock(id int32) {
	txn.HoldingLocks[id] = true
}

func (txn *Transaction) RemoveLock(id int32) {
	delete(txn.HoldingLocks, id)
}

func (txn *Transaction) AddSetWrite(id int32, new, orig *Pair) {
	old, ok := txn.WriteSet[id]
	if !ok {
		txn.WriteSet[id] = &WriteSet{BeforeValue: orig, AfterValue: new}
		return
	}
	old.AfterValue = new
}

func (txn *Transaction) AddDelWrite(id int32, new, orig *Pair) {
	old, ok := txn.WriteSet[id]
	if !ok {
		txn.WriteSet[id] = &WriteSet{
			BeforeValue: orig,
			AfterValue:  new,
		}
		return
	}
	old.AfterValue = new
}

func (txn *Transaction) AddAddWrite(id int32, new, orig *Pair) {
	old, ok := txn.WriteSet[id]
	if !ok {
		txn.WriteSet[id] = &WriteSet{
			BeforeValue: orig,
			AfterValue:  new,
		}
		return
	}
	old.AfterValue = new
}
