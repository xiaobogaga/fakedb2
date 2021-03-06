package internal

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/xiaobogaga/fakedb2/util"
	"os"
	"testing"
	"time"
)

var namePrefix = "fakedb2-test"
var dataFile = "/tmp/" + namePrefix + ".db"
var checkPointFile = "/tmp/" + namePrefix + ".checkpoint"
var walFile = "/tmp/" + namePrefix + ".walFile"

func clearTestData(t *testing.T) {
	err := os.Remove(dataFile)
	assert.Nil(t, err)
	err = os.Remove(checkPointFile)
	assert.Nil(t, err)
	err = os.Remove(walFile)
	assert.Nil(t, err)
}

func TestOracle_Commit(t *testing.T) {
	util.InitLogger("", 1024, time.Second, true)
	ctx := context.Background()
	flushDuration := time.Second
	bufManager, err := NewBufferManager(ctx, dataFile, flushDuration)
	assert.Nil(t, err)
	bufferSize := 1024
	logManager, err := NewLogManager(ctx, bufferSize, checkPointFile, walFile, flushDuration)
	assert.Nil(t, err)
	Recovery(logManager, bufManager)
	startTimeStamp := logManager.MaxTimeStamp + 1
	status := map[int32]*CommitStatus{}
	oracle := NewOracle(status)
	txnManager := NewTransactionManager(logManager.MaxTransactionId+1, bufManager, logManager, oracle)
	txn1 := txnManager.NewTransaction()
	txn2 := txnManager.NewTransaction()
	txn3 := txnManager.NewTransaction()
	assert.Equal(t, int64(startTimeStamp), oracle.StartTs(txn1.TransactionId))
	assert.Equal(t, int64(startTimeStamp+1), oracle.StartTs(txn2.TransactionId))
	assert.Equal(t, int64(startTimeStamp+2), oracle.StartTs(txn3.TransactionId))

	commitTs, err := oracle.Commit(txn2, startTimeStamp+1, []int32{1, 2})
	assert.Nil(t, err)
	assert.Equal(t, int64(startTimeStamp + 3), commitTs)
	commitTs, err = oracle.Commit(txn3, startTimeStamp+2, nil)
	assert.Nil(t, err)
	assert.Equal(t, int64(startTimeStamp + 4), commitTs)
	_, err = oracle.Commit(txn1, startTimeStamp, []int32{1})
	assert.NotNil(t, err)
	clearTestData(t)
}

func TestOracle_CommitStatus_Serialize(t *testing.T) {
	status := &CommitStatus{
		PageId:   1,
		TransId:  2,
		StartTs:  3,
		CommitTs: 4,
		Lsn:      5,
	}
	data := status.Serialize()
	assert.Equal(t, status.Len(), len(data))
	a := &CommitStatus{}
	err := a.Deserialize(data)
	assert.Nil(t, err)
	assert.Equal(t, status, a)
}