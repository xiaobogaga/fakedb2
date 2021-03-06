package internal

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/kr/pretty"
	"github.com/xiaobogaga/fakedb2/util"
	"io"
	"os"
	"sync"
	"time"
)

var logManagerLog = util.GetLog("logManager")

type LogManager struct {
	WALLock                sync.Mutex
	CheckPointWriter       *os.File
	WAL                    *os.File // File is needs to be on append | f_sync
	Lsn                    int64
	Lock                   sync.Mutex
	LogBuffer              chan *LogRecord
	Ctx                    context.Context
	FlushedLsn             int64
	LastCheckPointLsn      int64
	ActiveTransactionTable map[uint64]*TransactionTableEntry
	FlushDuration          time.Duration
	MaxTransactionId       uint64
	MaxTimeStamp           int64
}

// Read checkpoint. The checkPoint point to the latest lsn of begin checkpoint.
// The checkPoint file is written in WAL mode. and each checkPoint occupy 8 bytes.
// To make sure data is complete, we will truncate those checkpoint whose length is less than 8 bytes.
func ReadLastCheckPoint(reader *os.File) (int64, error) {
	stat, err := reader.Stat()
	if err != nil {
		return 0, err
	}
	size := stat.Size()
	if size < 8 {
		reader.Seek(0, io.SeekStart)
		return 0, nil
	}
	checkPointAddr := size - size%8 - 8
	_, err = reader.Seek(checkPointAddr, io.SeekStart)
	if err != nil {
		return 0, err
	}
	bytes := make([]byte, 8)
	for i := 0; i < 8; {
		size, err := reader.Read(bytes[i:])
		if err != nil {
			return 0, err
		}
		i += size
	}
	return int64(binary.BigEndian.Uint64(bytes)), nil
}

func NewLogManager(ctx context.Context, bufferSize int, checkPointFileName string, WAL string, flushDuration time.Duration) (*LogManager, error) {
	checkPointWriter, err := os.OpenFile(checkPointFileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	lastCheckPointLsn, err := ReadLastCheckPoint(checkPointWriter)
	if err != nil {
		return nil, err
	}
	wal, err := os.OpenFile(WAL, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &LogManager{
		CheckPointWriter:       checkPointWriter,
		WAL:                    wal,
		Lsn:                    InvalidLsn,
		Lock:                   sync.Mutex{},
		LogBuffer:              make(chan *LogRecord, bufferSize),
		Ctx:                    ctx,
		LastCheckPointLsn:      lastCheckPointLsn,
		ActiveTransactionTable: map[uint64]*TransactionTableEntry{},
		FlushDuration:          flushDuration,
		MaxTimeStamp:           InvalidTimeStamp,
		MaxTransactionId:       0,
	}, nil
}

const InvalidLsn int64 = -1

// Todo: maybe we need a crc here.
type LogRecord struct {
	LSN           int64
	PrevLsn       int64
	UndoNextLsn   int64
	TransactionId uint64
	PageId        int32
	TP            LogRecordType
	ActionTP      ActionType
	// Todo:
	StartTs     int64
	CommitTs    int64
	BeforeValue []byte
	AfterValue  []byte
	Done        chan bool
	Force       bool
}

type ActionType byte

const (
	DelAction ActionType = iota
	SetAction
	AddAction
)

var ActionTypeNameMap = map[ActionType]string{
	DelAction: "del",
	SetAction: "set",
	AddAction: "add",
}

type LogRecordType byte

const (
	SetLogType LogRecordType = iota
	CompensationLogType
	TransBeginLogType
	TransCommitLogType
	TransEndLogType
	TransAbortLogType
	CheckPointBeginLogType
	CheckPointEndLogType
	DummyLogType
)

var LogRecordTypeNameMap = map[LogRecordType]string{
	SetLogType:             "set",
	CompensationLogType:    "compensation",
	TransBeginLogType:      "begin",
	TransCommitLogType:     "commit",
	TransEndLogType:        "end",
	TransAbortLogType:      "abort",
	CheckPointBeginLogType: "begin_checkpoint",
	CheckPointEndLogType:   "end_checkpoint",
}

type TransactionTableEntry struct {
	TransactionId uint64
	State         TransactionState
	Lsn           int64
	UndoNextLsn   int64
}

var transTableSize = 25

type TransactionState byte

const (
	TransactionC TransactionState = iota
	TransactionU
	TransactionP
	TransactionE
)

var TransactionStateNameMap = map[TransactionState]string{
	TransactionC: "commit",
	TransactionU: "undecided",
	TransactionP: "prepare",
	TransactionE: "end",
}

func (entry *TransactionTableEntry) Serialize() []byte {
	ret := make([]byte, transTableSize)
	binary.BigEndian.PutUint64(ret, entry.TransactionId)
	ret[8] = byte(entry.State)
	binary.BigEndian.PutUint64(ret[9:], uint64(entry.Lsn))
	binary.BigEndian.PutUint64(ret[17:], uint64(entry.UndoNextLsn))
	return ret
}

func (entry *TransactionTableEntry) Deserialize(data []byte) error {
	if len(data) < transTableSize {
		return damagedPacket
	}
	entry.TransactionId = binary.BigEndian.Uint64(data)
	entry.State = TransactionState(data[8])
	entry.Lsn = int64(binary.BigEndian.Uint64(data[9:]))
	entry.UndoNextLsn = int64(binary.BigEndian.Uint64(data[17:]))
	return nil
}

func (entry *TransactionTableEntry) Len() int {
	return transTableSize
}

func (entry *TransactionTableEntry) String() string {
	return fmt.Sprintf("[transId: %d, state: %s, lsn: %d, undoNextLsn: %d]", entry.TransactionId,
		TransactionStateNameMap[entry.State], entry.Lsn, entry.UndoNextLsn)
}

// FixedLength of logRecord: 4 + 8 * 4 + 4 + 1 + 4 + before value bytes + 4 + after value bytes.
func (log *LogRecord) Serialize() []byte {
	data := make([]byte, log.Len())
	binary.BigEndian.PutUint32(data, uint32(log.Len()))
	binary.BigEndian.PutUint64(data[4:], uint64(log.LSN))
	binary.BigEndian.PutUint64(data[12:], uint64(log.PrevLsn))
	binary.BigEndian.PutUint64(data[20:], uint64(log.UndoNextLsn))
	binary.BigEndian.PutUint64(data[28:], log.TransactionId)
	binary.BigEndian.PutUint32(data[36:], uint32(log.PageId))
	data[40] = byte(log.TP)
	data[41] = byte(log.ActionTP)
	binary.BigEndian.PutUint64(data[42:], uint64(log.StartTs))
	binary.BigEndian.PutUint64(data[50:], uint64(log.CommitTs))
	binary.BigEndian.PutUint32(data[58:], uint32(len(log.BeforeValue)))
	copy(data[62:], log.BeforeValue)
	binary.BigEndian.PutUint32(data[62+len(log.BeforeValue):], uint32(len(log.AfterValue)))
	copy(data[66+len(log.BeforeValue):], log.AfterValue)
	return data
}

func (log *LogRecord) Deserialize(data []byte) error {
	if len(data) < 66 {
		return damagedPacket
	}
	log.LSN = int64(binary.BigEndian.Uint64(data[4:]))
	log.PrevLsn = int64(binary.BigEndian.Uint64(data[12:]))
	log.UndoNextLsn = int64(binary.BigEndian.Uint64(data[20:]))
	log.TransactionId = binary.BigEndian.Uint64(data[28:])
	log.PageId = int32(binary.BigEndian.Uint32(data[36:]))
	log.TP = LogRecordType(data[40])
	log.ActionTP = ActionType(data[41])
	log.StartTs = int64(binary.BigEndian.Uint64(data[42:]))
	log.CommitTs = int64(binary.BigEndian.Uint64(data[50:]))
	beforeValueLen := binary.BigEndian.Uint32(data[58:])
	if uint32(len(data)) < 58+beforeValueLen {
		return damagedPacket
	}
	log.BeforeValue = data[62 : beforeValueLen+62]
	afterValueLen := binary.BigEndian.Uint32(data[62+beforeValueLen:])
	if uint32(len(data)) < 66+beforeValueLen+afterValueLen {
		return damagedPacket
	}
	log.AfterValue = data[66+beforeValueLen : 66+beforeValueLen+afterValueLen]
	// TODO: support CRC
	return nil
}

func (log *LogRecord) Len() int {
	return 4 + 8*4 + 4 + 2 + 8*2 + 4 + len(log.BeforeValue) + 4 + len(log.AfterValue)
}

func SerializeRows(rows []int32) []byte {
	rowBytes := make([]byte, len(rows)*4+4)
	binary.BigEndian.PutUint32(rowBytes, uint32(len(rows)))
	for i, row := range rows {
		binary.BigEndian.PutUint32(rowBytes[4+i*4:], uint32(row))
	}
	return rowBytes
}

func DeserializeRows(data []byte) []int32 {
	rowLen := binary.BigEndian.Uint32(data)
	rows := make([]int32, rowLen)
	for i := uint32(0); i < rowLen; i++ {
		rows[i] = int32(binary.BigEndian.Uint32(data[4+i*4:]))
	}
	return rows
}

func (log *LogRecord) String() string {
	switch log.TP {
	case TransBeginLogType, TransAbortLogType, TransEndLogType:
		return fmt.Sprintf("log: lsn: %d, prevLsn: %d, undoNextLsn: %d, startTs: %d, commitTs: %d, tranceId: %d, force: %t, log: %s, len: %d.",
			log.LSN, log.PrevLsn, log.UndoNextLsn, log.StartTs, log.CommitTs, log.TransactionId, log.Force, LogRecordTypeNameMap[log.TP], log.Len())
	case TransCommitLogType:
		rows := DeserializeRows(log.BeforeValue)
		return fmt.Sprintf("log: lsn: %d, prevLsn: %d, undoNextLsn: %d, startTs: %d, commitTs: %d, tranceId: %d, force: %t, rows: %v, log: %s, len: %d.",
			log.LSN, log.PrevLsn, log.UndoNextLsn, log.StartTs, log.CommitTs, log.TransactionId, log.Force, rows, LogRecordTypeNameMap[log.TP], log.Len())
	case CheckPointBeginLogType:
		return fmt.Sprintf("log: lsn: %d, prevLsn: %d, undoNextLsn: %d, startTs: %d, commitTs: %d, log: %s, len: %d.", log.LSN, log.PrevLsn,
			log.UndoNextLsn, log.StartTs, log.CommitTs, LogRecordTypeNameMap[log.TP], log.Len())
	case CheckPointEndLogType:
		transTable, commitStatus := DeserializeTransactionTableEntryAndPageCommitTable(log.BeforeValue)
		trans := pretty.Sprintf("%v", transTable)
		dirtyPages := pretty.Sprintf("%v", DeserializeDirtyPageRecord(log.AfterValue))
		commitTable := pretty.Sprintf("%v", commitStatus)
		return fmt.Sprintf("log: lsn: %d, prevLsn: %d, undoNextLsn: %d, startTs: %d, commitTs: %d, log: %s, trans: %s, dirtyPages: %s, commit: %s, len: %d.",
			log.LSN, log.PrevLsn, log.UndoNextLsn, log.StartTs, log.CommitTs, LogRecordTypeNameMap[log.TP], trans, dirtyPages, commitTable, log.Len())
	case SetLogType, CompensationLogType:
		pair := &Pair{}
		err := pair.Deserialize(log.BeforeValue)
		if err != nil {
			panic(err)
		}
		pair2 := &Pair{}
		err = pair2.Deserialize(log.AfterValue)
		if err != nil {
			panic(err)
		}
		return fmt.Sprintf("log: lsn: %d, prevLsn: %d, undoNextLsn: %d, startTs: %d, commitTs: %d, pageId: %d, tranceId: %d, "+
			"force: %t, action: %s, log: %s. before: [key: %s, value: %s], after: [key: %s, value: %s], len: %d.", log.LSN,
			log.PrevLsn, log.UndoNextLsn, log.StartTs, log.CommitTs, log.PageId, log.TransactionId, log.Force,
			ActionTypeNameMap[log.ActionTP], LogRecordTypeNameMap[log.TP], string(pair.Key), string(pair.Value),
			string(pair2.Key), string(pair2.Value), log.Len())
	default:
		panic("wrong type")
	}
}

// Wait until the log l is flushed to WAL.
func (log *LogManager) WaitFlush(l *LogRecord) {
	<-l.Done
}

var LogFlushCapacity = 1024 * 16

func (log *LogManager) FlushPeriodly() {
	logManagerLog.InfoF("start flush log goroutine, lsn: %d", log.Lsn)
	defer logManagerLog.InfoF("end flush log goroutine")
	limit := LogFlushCapacity
	buf := make([]byte, LogFlushCapacity)
	var pendingLog []*LogRecord
	i := 0
	var l *LogRecord
	flush := false
	for {
		l = nil
		select {
		case <-log.Ctx.Done():
			return
		case l = <-log.LogBuffer:
		case <-time.After(log.FlushDuration):
			flush = true
		}
		if l == nil && !flush {
			return
		}
		if flush {
			log.Write(buf[:i], i)
			log.NoticeDone(pendingLog)
			pendingLog = nil
			i = 0
			flush = false
			continue
		}
		if l.Force && l.TP == DummyLogType {
			// logManagerLog.InfoF("data len: %d", i)
			log.Write(buf[:i], i)
			log.NoticeDone(pendingLog)
			pendingLog = nil
			i = 0
			continue
		}
		data := l.Serialize()
		// logManagerLog.InfoF("data len: %d", len(data))
		if len(data) > (limit - i) {
			log.Write(buf[:i], i)
			log.NoticeDone(pendingLog)
			pendingLog = nil
			i = 0
		}
		if len(data) > limit {
			// Flush directly.
			log.Write(data, len(data))
			log.NoticeDone([]*LogRecord{l})
			continue
		}
		pendingLog = append(pendingLog, l)
		copy(buf[i:], data)
		i += len(data)
		if l.Force {
			// Force writing to logs.
			log.Write(buf[:i], i)
			log.NoticeDone(pendingLog)
			i = 0
			pendingLog = nil
		}
	}
}

func (log *LogManager) NoticeDone(logs []*LogRecord) {
	for _, l := range logs {
		logManagerLog.InfoF("write a log: %s", l)
		close(l.Done)
	}
}

func (log *LogManager) Write(data []byte, len int) {
	if len == 0 {
		return
	}
	log.WALLock.Lock()
	i := 0
	for i < len {
		size, err := log.WAL.Write(data[i:])
		if err != nil {
			panic(err)
		}
		i += size
	}
	err := log.WAL.Sync()
	if err != nil {
		panic(err)
	}
	log.WALLock.Unlock()
	log.Lock.Lock()
	log.FlushedLsn += int64(len)
	log.Lock.Unlock()
}

func IsTransLog(l *LogRecord) bool {
	return l.TP != CheckPointBeginLogType && l.TP != CheckPointEndLogType && l.TP != DummyLogType
}

func (log *LogManager) Append(l *LogRecord) *LogRecord {
	log.Lock.Lock()
	if log.MaxTransactionId < l.TransactionId {
		log.MaxTransactionId = l.TransactionId
	}
	if log.MaxTimeStamp < l.StartTs {
		log.MaxTimeStamp = l.StartTs
	}
	if log.MaxTimeStamp < l.CommitTs {
		log.MaxTimeStamp = l.CommitTs
	}
	if l.TP == CheckPointBeginLogType {
		l.StartTs = log.MaxTimeStamp
		l.TransactionId = log.MaxTransactionId
	}
	l.LSN = log.Lsn
	log.Lsn += int64(l.Len())
	if IsTransLog(l) {
		log.updateActiveTransactionTable(l)
	}
	log.Lock.Unlock()

	l.Done = make(chan bool)
	log.LogBuffer <- l
	return l
}

// Todo: need to check further.
func transActionStateFromLogRecord(l *LogRecord) TransactionState {
	switch l.TP {
	case TransBeginLogType:
		return TransactionP
	case TransCommitLogType:
		return TransactionC
	case TransEndLogType:
		return TransactionE
	default:
		return TransactionU
	}
}

func (log *LogManager) CheckPoint(bufManager *BufferManager, oracle *Oracle) {
	logManagerLog.InfoF("start checkpoint goroutine")
	defer logManagerLog.InfoF("end checkpoint goroutine")
	for {
		select {
		case <-time.After(log.FlushDuration):
		case <-log.Ctx.Done():
			return
		}
		err := log.DoCheckPoint(bufManager, oracle)
		if err != nil {
			panic(err)
		}
	}
}

func (log *LogManager) DoCheckPoint(bufManager *BufferManager, oracle *Oracle) error {
	beginCheckPointRecord := &LogRecord{
		TP: CheckPointBeginLogType,
	}
	log.Append(beginCheckPointRecord)
	transTable := log.CloneTransactionTable()
	dirtyTable := bufManager.DirtyPageRecordTable()
	pageCommitTable := oracle.CommitStatus()

	transCommitBytes := SerializeTransactionTableEntryAndPageCommitTable(transTable, pageCommitTable)
	dirtyPageBytes := SerializeDirtyPageRecord(dirtyTable)

	// check point finish log
	finishCheckPointLogRecord := &LogRecord{
		TP:          CheckPointEndLogType,
		BeforeValue: transCommitBytes,
		AfterValue:  dirtyPageBytes,
		Force:       true,
	}
	log.Append(finishCheckPointLogRecord)
	log.WaitFlush(finishCheckPointLogRecord)
	logManagerLog.InfoF("do a checkpoint: ")
	return log.WriteBeginCheckPoint(beginCheckPointRecord.LSN)
}

type CheckPointAddr struct {
	LSN int64 // Todo: add crc
}

func (log *LogManager) WriteBeginCheckPoint(checkPointLsn int64) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(checkPointLsn))
	for i := 0; i < 8; {
		size, err := log.CheckPointWriter.Write(data[i:])
		if err != nil {
			return err
		}
		i += size
	}
	return nil
}

func (log *LogManager) CloneTransactionTable() (ret []*TransactionTableEntry) {
	log.Lock.Lock()
	defer log.Lock.Unlock()
	for txnId, v := range log.ActiveTransactionTable {
		ret = append(ret, &TransactionTableEntry{
			TransactionId: txnId,
			State:         v.State,
			Lsn:           v.Lsn,
			UndoNextLsn:   v.UndoNextLsn,
		})
	}
	return ret
}

func (log *LogManager) FlushLogs() {
	l := &LogRecord{
		TP:    DummyLogType,
		Force: true,
	}
	log.LogBuffer <- l
}

// Thread-safe
func (log *LogManager) GetBeginCheckPointLSN() int64 {
	return log.LastCheckPointLsn
}

func (log *LogManager) GetFlushedLsn() int64 {
	log.Lock.Lock()
	defer log.Lock.Unlock()
	return log.FlushedLsn
}

// Only used during recovery. Should be single goroutine. no lock protection needed.

func (log *LogManager) AppendRecoveryLog(l *LogRecord) {
	if log.MaxTransactionId < l.TransactionId {
		log.MaxTransactionId = l.TransactionId
	}
	if log.MaxTimeStamp < l.StartTs {
		log.MaxTimeStamp = l.StartTs
	}
	if log.MaxTimeStamp < l.CommitTs {
		log.MaxTimeStamp = l.CommitTs
	}
	offset := log.Lsn
	l.LSN = log.Lsn
	log.Lsn += int64(l.Len())
	if IsTransLog(l) {
		log.updateActiveTransactionTable(l)
	}
	l.Done = make(chan bool)
	data := l.Serialize()
	_, err := log.WAL.Seek(offset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	for i := 0; i < l.Len(); {
		size, err := log.WAL.Write(data[i:])
		if err != nil {
			panic(err)
		}
		i += size
	}
}

func (log *LogManager) updateActiveTransactionTable(l *LogRecord) {
	old, ok := log.ActiveTransactionTable[l.TransactionId]
	if !ok {
		old = &TransactionTableEntry{
			TransactionId: l.TransactionId,
			State:         transActionStateFromLogRecord(l),
			Lsn:           l.LSN,
			UndoNextLsn:   l.LSN,
		}
		log.ActiveTransactionTable[l.TransactionId] = old
	} else {
		old.State = transActionStateFromLogRecord(l)
		old.Lsn, old.UndoNextLsn = l.LSN, l.LSN
	}
	logManagerLog.InfoF("update trans state: id: %d, now: %s", old.TransactionId, TransactionStateNameMap[old.State])
	if old.State == TransactionC || old.State == TransactionE {
		delete(log.ActiveTransactionTable, old.TransactionId)
	}
}

func (log *LogManager) LogIterator(lsn int64) *LogIterator {
	return &LogIterator{
		LSN:     lsn,
		WAL:     log.WAL,
		NextLog: nil,
	}
}

func (log *LogManager) ReadLog(offset int64) (*LogRecord, error) {
	l, err := ReadLog(log.WAL, offset)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (log *LogManager) UpdateMaxTimeStampAndMaxTransactionId(l *LogRecord) {
	if log.MaxTransactionId < l.TransactionId {
		log.MaxTransactionId = l.TransactionId
	}
	if log.MaxTimeStamp < l.StartTs {
		log.MaxTimeStamp = l.StartTs
	}
	if log.MaxTimeStamp < l.CommitTs {
		log.MaxTimeStamp = l.CommitTs
	}
}

type LogIterator struct {
	LSN     int64
	WAL     *os.File
	NextLog *LogRecord
}

func ReadLog(wal *os.File, offset int64) (*LogRecord, error) {
	_, err := wal.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, 4)
	for i := 0; i < 4; {
		size, err := wal.Read(bytes[i:])
		if err != nil {
			return nil, err
		}
		i += size
	}
	len := binary.BigEndian.Uint32(bytes) - 4
	data := make([]byte, len)
	for i := 0; i < int(len); {
		size, err := wal.Read(data[i:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF {
			logManagerLog.InfoF("read a wrong length log")
			break
		}
		i += size
	}
	data = append(bytes, data...)
	ret := &LogRecord{}
	err = ret.Deserialize(data)
	return ret, err
}

func (ite *LogIterator) HasNext() bool {
	var err error
	if ite.NextLog == nil {
		ite.NextLog, err = ReadLog(ite.WAL, ite.LSN)
		if err != nil {
			return false
		}
		if ite.NextLog != nil {
			ite.LSN += int64(ite.NextLog.Len())
		}
	}
	return ite.NextLog != nil
}

func (ite *LogIterator) Next() *LogRecord {
	ret := ite.NextLog
	ite.NextLog = nil
	return ret
}

type Pair struct {
	Key   []byte
	Value []byte
}

func (pair *Pair) Serialize() []byte {
	data := make([]byte, pair.Len())
	binary.BigEndian.PutUint32(data, uint32(len(pair.Key)))
	copy(data[4:], pair.Key)
	binary.BigEndian.PutUint32(data[4+len(pair.Key):], uint32(len(pair.Value)))
	copy(data[8+len(pair.Key):], pair.Value)
	return data
}

func (pair *Pair) Deserialize(data []byte) error {
	if len(data) < 8 {
		return damagedPacket
	}
	keyLen := binary.BigEndian.Uint32(data)
	if uint32(len(data)) < keyLen+8 {
		return damagedPacket
	}
	pair.Key = data[4 : 4+keyLen]
	valueLen := binary.BigEndian.Uint32(data[4+keyLen:])
	if uint32(len(data)) < keyLen+valueLen+8 {
		return damagedPacket
	}
	pair.Value = data[8+keyLen : 8+keyLen+valueLen]
	return nil
}

func (pair *Pair) Len() int {
	return 8 + len(pair.Key) + len(pair.Value)
}
