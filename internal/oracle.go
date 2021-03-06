package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/xiaobogaga/fakedb2/util"
	"sync"
)

type Oracle struct {
	TimeStamp int64
	Lock      sync.Mutex
	Status    map[int32]*CommitStatus // row to commit status
}

var pageCommitSize = 36

type CommitStatus struct {
	PageId   int32
	TransId  uint64
	StartTs  int64
	CommitTs int64
	Lsn      int64
}

func (status *CommitStatus) Serialize() []byte {
	data := make([]byte, pageCommitSize)
	binary.BigEndian.PutUint32(data, uint32(status.PageId))
	binary.BigEndian.PutUint64(data[4:], uint64(status.TransId))
	binary.BigEndian.PutUint64(data[12:], uint64(status.StartTs))
	binary.BigEndian.PutUint64(data[20:], uint64(status.CommitTs))
	binary.BigEndian.PutUint64(data[28:], uint64(status.Lsn))
	return data
}

func (status *CommitStatus) Deserialize(data []byte) error {
	if len(data) < pageCommitSize {
		return damagedPacket
	}
	status.PageId = int32(binary.BigEndian.Uint32(data))
	status.TransId = binary.BigEndian.Uint64(data[4:])
	status.StartTs = int64(binary.BigEndian.Uint64(data[12:]))
	status.CommitTs = int64(binary.BigEndian.Uint64(data[20:]))
	status.Lsn = int64(binary.BigEndian.Uint64(data[28:]))
	return nil
}

func (status *CommitStatus) Len() int {
	return pageCommitSize
}

func (status *CommitStatus) String() string {
	return fmt.Sprintf("[pageId: %d, transId: %d, startTs: %d, commitTs: %d, lsn: %d]", status.PageId,
		status.TransId, status.StartTs, status.CommitTs, status.Lsn)
}

var oracleLog = util.GetLog("oracle")

func NewOracle(commitTable map[int32]*CommitStatus) *Oracle {
	oracleLog.InfoF("create oracle with commitTable: %s", commitTable)
	startTs := int64(InvalidTimeStamp)
	for _, status := range commitTable {
		if startTs < status.CommitTs {
			startTs = status.CommitTs
		}
		if startTs < status.StartTs {
			startTs = status.StartTs
		}
	}
	oracle := &Oracle{TimeStamp: startTs + 1, Lock: sync.Mutex{}, Status: commitTable}
	return oracle
}

var InvalidTimeStamp = int64(-1)

func (oracle *Oracle) StartTs(txnId uint64) int64 {
	oracle.Lock.Lock()
	defer oracle.Lock.Unlock()
	ret := oracle.TimeStamp
	oracleLog.InfoF("transaction: %d get start timestamp: %d", txnId, ret)
	oracle.TimeStamp++
	return ret
}

// The caller will call this method to check whether can commit the changes within rows: rows.
// Write write conflict checking.
func (oracle *Oracle) Commit(txn *Transaction, startTs int64, rows []int32) (int64, error) {
	oracle.Lock.Lock()
	defer oracle.Lock.Unlock()
	for _, r := range rows {
		status, ok := oracle.Status[r]
		if ok && status.CommitTs > startTs {
			oracleLog.InfoF("transaction %d conflict with transaction: %d on rows: %d", txn.TransactionId, status.TransId, r)
			return -1, errors.New(fmt.Sprintf("transaction %d conflict with transaction: %d on row: %d", txn.TransactionId, status.TransId, r))
		}
	}
	rowBytes := SerializeRows(rows)
	commitTs := oracle.TimeStamp
	log := &LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   InvalidLsn,
		TransactionId: txn.TransactionId,
		TP:            TransCommitLogType,
		Force:         true,
		StartTs:       txn.StartTS,
		CommitTs:      commitTs,
		BeforeValue:   rowBytes,
	}
	txn.Log.Append(log) // Begin Log
	prevLsn := txn.PrevLsn
	txn.PrevLsn = log.LSN
	txn.Log.WaitFlush(log)
	oracleLog.InfoF("transaction: %d commit with start timestamp: %d, commit timestamp: %d on rows: %v", txn.TransactionId, startTs, commitTs, rows)
	oracle.TimeStamp++
	for _, r := range rows {
		status := &CommitStatus{PageId: r, TransId: txn.TransactionId, StartTs: startTs, CommitTs: commitTs, Lsn: prevLsn}
		oracle.Status[r] = status
	}
	txn.Buf.MarkCommit(txn.TransactionId, startTs, commitTs, rows)
	return commitTs, nil
}

func (oracle *Oracle) CommitStatus() []*CommitStatus {
	oracle.Lock.Lock()
	defer oracle.Lock.Unlock()
	ret := make([]*CommitStatus, len(oracle.Status))
	i := 0
	for _, entry := range oracle.Status {
		ret[i] = &CommitStatus{
			PageId:   entry.PageId,
			TransId:  entry.TransId,
			StartTs:  entry.StartTs,
			CommitTs: entry.CommitTs,
			Lsn:      entry.Lsn,
		}
		i++
	}
	return ret
}
