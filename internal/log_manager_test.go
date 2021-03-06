package internal

import (
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogRecord_Serialize_Deserialize(t *testing.T) {
	log := &LogRecord{
		LSN:           1,
		PrevLsn:       2,
		UndoNextLsn:   3,
		TransactionId: 4,
		PageId:        5,
		TP:            TransEndLogType,
		ActionTP:      SetAction,
		StartTs:       1,
		CommitTs:      2,
		BeforeValue:   []byte{1},
		AfterValue:    []byte{2},
	}
	data := log.Serialize()
	assert.Equal(t, 4+8*4+4+2+8*2+4+4+2, len(data))
	assert.Equal(t, len(data), log.Len())
	aLog := &LogRecord{}
	err := aLog.Deserialize(data)
	assert.Nil(t, err)
	assert.Equal(t, log, aLog)
}

func TestLogRecord_Serialize(t *testing.T) {
	//util.InitLogger("", 1024, time.Second, true)
	//file := "/tmp/fakedb2.wal"
	//f, err := os.OpenFile(file, os.O_RDONLY, 0666)
	//stat, err := f.Stat()
	//assert.Nil(t, err)
	//fmt.Printf("size: %d\n", stat.Size())
	//assert.Nil(t, err)
	//l, err := ReadLog(f, 15860)
	//assert.Nil(t, err)
	//fmt.Printf("log: %s\n", l)
}

//func TestLogManager_FlushPeriodly(t *testing.T) {
//	f, err := os.OpenFile("/tmp/fakedb.wal2", os.O_RDWR|os.O_CREATE, 0666)
//	assert.Nil(t, err)
//	logManager := &LogManager{
//		Ctx:       context.Background(),
//		Lsn:       0,
//		WAL:       f,
//		LogBuffer: make(chan *LogRecord, 1),
//	}
//	log := &LogRecord{
//		LSN:           0,
//		PrevLsn:       InvalidLsn,
//		UndoNextLsn:   InvalidLsn,
//		TransactionId: 0,
//		PageId:        0,
//		TP:            TransEndLogType,
//		Force:         true,
//	}
//	logManager.LogBuffer <- log
//	logManager.FlushPeriodly()
//}

func TestTransactionTableEntry_Serialize(t *testing.T) {
	entry := &TransactionTableEntry{
		TransactionId: 1,
		State:         2,
		Lsn:           3,
		UndoNextLsn:   4,
	}
	data := entry.Serialize()
	assert.Equal(t, entry.Len(), len(data))
	another := &TransactionTableEntry{}
	err := another.Deserialize(data)
	assert.Nil(t, err)
	assert.Equal(t, entry, another)
	assert.NotNil(t, entry.Deserialize(data[2:]))
	entries := []*TransactionTableEntry{entry, entry}
	pretty.Printf("%+v\n", entries)
}

func TestPair_Deserialize(t *testing.T) {
	pair := &Pair{Key: []byte{1}, Value: []byte{2}}
	data := pair.Serialize()
	assert.Equal(t, pair.Len(), len(data))
	another := &Pair{}
	err := another.Deserialize(data)
	assert.Nil(t, err)
	assert.Equal(t, pair, another)
	assert.NotNil(t, another.Deserialize(data[1:]))
}
