package internal

import (
	"encoding/binary"
	"github.com/xiaobogaga/fakedb2/util"
)

var recoveryLog = util.GetLog("recovery")

func Recovery(logManager *LogManager, bufManager *BufferManager) *Oracle {
	activeTransActionTable, dirtyPageRecordsTable, commitStatusTable := analysisForRecovery(logManager)
	go bufManager.FlushDirtyPagesRegularly(logManager)
	// We use the maximum transactionId from the current trans table as the next transId.
	// nextUsefulTransactionId = NextUsefulTransId(activeTransActionTable)
	err := redoForRecovery(logManager, bufManager, dirtyPageRecordsTable)
	if err != nil {
		panic(err)
	}
	err = undoForRecovery(logManager, bufManager, activeTransActionTable, commitStatusTable)
	if err != nil {
		panic(err)
	}
	cleanFinishTrans(logManager, activeTransActionTable)
	err = logManager.WAL.Sync()
	if err != nil {
		panic(err)
	}
	oracle := NewOracle(commitStatusTable)
	go logManager.FlushPeriodly()
	go logManager.CheckPoint(bufManager, oracle)
	return oracle
}

func cleanFinishTrans(logManager *LogManager, activeTransActionTable map[uint64]*TransactionTableEntry) {
	for txnId, txn := range activeTransActionTable {
		if txn.State == TransactionC || txn.State == TransactionP {
			log := &LogRecord{
				PrevLsn:       txn.Lsn,
				TransactionId: txn.TransactionId,
				TP:            TransEndLogType,
			}
			logManager.AppendRecoveryLog(log)
			delete(activeTransActionTable, txnId)
		}
		if txn.State == TransactionE {
			delete(activeTransActionTable, txnId)
		}
		if txn.State == TransactionU && txn.UndoNextLsn == InvalidLsn {
			log := &LogRecord{
				PrevLsn:       txn.Lsn,
				TransactionId: txn.TransactionId,
				TP:            TransEndLogType,
			}
			logManager.AppendRecoveryLog(log)
			delete(activeTransActionTable, txnId)
		}
	}
}

func SerializeTransactionTableEntryAndPageCommitTable(transTable []*TransactionTableEntry, pageCommitTable []*CommitStatus) []byte {
	transTableBytes := make([]byte, len(transTable)*transTableSize+4)
	binary.BigEndian.PutUint32(transTableBytes, uint32(len(transTable)))
	for i, entry := range transTable {
		copy(transTableBytes[4+i*transTableSize:], entry.Serialize())
	}

	pageCommitTableBytes := make([]byte, len(pageCommitTable)*pageCommitSize+4)
	binary.BigEndian.PutUint32(pageCommitTableBytes, uint32(len(pageCommitTable)))
	for i, entry := range pageCommitTable {
		copy(pageCommitTableBytes[4+pageCommitSize*i:], entry.Serialize())
	}

	return append(transTableBytes, pageCommitTableBytes...)
}

func SerializeDirtyPageRecord(dirtyTable []*DirtyPageRecord) []byte {
	dirtyTableBytes := make([]byte, len(dirtyTable)*dirtyPageTableSize+4)
	binary.BigEndian.PutUint32(dirtyTableBytes, uint32(len(dirtyTable)))
	for i, entry := range dirtyTable {
		copy(dirtyTableBytes[4+i*dirtyPageTableSize:], entry.Serialize())
	}
	return dirtyTableBytes
}

func DeserializeTransactionTableEntryAndPageCommitTable(data []byte) ([]*TransactionTableEntry, []*CommitStatus) {
	transTableLen := binary.BigEndian.Uint32(data)
	transTable := make([]*TransactionTableEntry, transTableLen)
	for i := uint32(0); i < transTableLen; i++ {
		transTable[i] = &TransactionTableEntry{}
		err := transTable[i].Deserialize(data[4+25*i:])
		if err != nil {
			panic(err)
		}
	}
	commitTableLen := binary.BigEndian.Uint32(data[4+transTableLen*25:])
	commitStatusTable := make([]*CommitStatus, commitTableLen)
	for i := uint32(0); i < commitTableLen; i++ {
		commitStatusTable[i] = &CommitStatus{}
		err := commitStatusTable[i].Deserialize(data[8+transTableLen*25+i*36:])
		if err != nil {
			panic(err)
		}
	}
	return transTable, commitStatusTable
}

func DeserializeDirtyPageRecord(data []byte) []*DirtyPageRecord {
	dirtyTableLen := binary.BigEndian.Uint32(data)
	dirtyTable := make([]*DirtyPageRecord, dirtyTableLen)
	for i := uint32(0); i < dirtyTableLen; i++ {
		dirtyTable[i] = &DirtyPageRecord{}
		err := dirtyTable[i].Deserialize(data[4+uint32(dirtyPageTableSize)*i:])
		if err != nil {
			panic(err)
		}
	}
	return dirtyTable
}

func handleCheckPointLogDuringRecovery(checkPointEndLog *LogRecord, activeTransActionTable map[uint64]*TransactionTableEntry,
	dirtyPageTables map[int32]*DirtyPageRecord, commitStatusTable map[int32]*CommitStatus) {
	transTable, commitStatus := DeserializeTransactionTableEntryAndPageCommitTable(checkPointEndLog.BeforeValue)
	dirtyTable := DeserializeDirtyPageRecord(checkPointEndLog.AfterValue)
	for _, entry := range transTable {
		_, ok := activeTransActionTable[entry.TransactionId]
		if ok {
			continue
		}
		activeTransActionTable[entry.TransactionId] = &TransactionTableEntry{
			TransactionId: entry.TransactionId,
			State:         entry.State,
			Lsn:           entry.Lsn,
			UndoNextLsn:   entry.UndoNextLsn,
		}
	}

	for _, entry := range dirtyTable {
		pageId := entry.PageId
		_, ok := dirtyPageTables[pageId]
		if !ok {
			dirtyPageTables[pageId] = &DirtyPageRecord{PageId: pageId, RevLSN: entry.RevLSN, StartTs: entry.StartTs, CommitTs: entry.CommitTs}
			continue
		}
		dirtyPageTables[pageId].RevLSN = entry.RevLSN // entry.RevLSN must be less than the lsn in dirty page table.
	}

	for _, status := range commitStatus {
		commitStatusTable[status.PageId] = status
	}
}

func analysisForRecovery(logManager *LogManager) (activeTransActionTable map[uint64]*TransactionTableEntry,
	dirtyPageTables map[int32]*DirtyPageRecord, commitStatusTable map[int32]*CommitStatus) {
	activeTransActionTable = map[uint64]*TransactionTableEntry{}
	dirtyPageTables = map[int32]*DirtyPageRecord{}
	commitStatusTable = map[int32]*CommitStatus{}
	lsn := logManager.GetBeginCheckPointLSN()
	logIte := logManager.LogIterator(lsn)
	for logIte.HasNext() {
		log := logIte.Next()
		logManager.UpdateMaxTimeStampAndMaxTransactionId(log)
		recoveryLog.InfoF("analysis log: %s", log)
		tp := log.TP
		_, ok := activeTransActionTable[log.TransactionId]
		if IsTransLog(log) && !ok {
			activeTransActionTable[log.TransactionId] = &TransactionTableEntry{
				TransactionId: log.TransactionId,
				State:         TransactionU,
				Lsn:           log.LSN,
				UndoNextLsn:   log.PrevLsn,
			}
		}
		switch tp {
		case SetLogType:
			activeTransActionTable[log.TransactionId].Lsn = log.LSN
			activeTransActionTable[log.TransactionId].UndoNextLsn = log.LSN
			_, ok := dirtyPageTables[log.PageId]
			if !ok {
				dirtyPageTables[log.PageId] = &DirtyPageRecord{PageId: log.PageId, RevLSN: log.LSN}
			}
			activeTransActionTable[log.TransactionId].State = TransactionU
		case CompensationLogType:
			activeTransActionTable[log.TransactionId].Lsn = log.LSN
			activeTransActionTable[log.TransactionId].UndoNextLsn = log.UndoNextLsn
			_, ok := dirtyPageTables[log.PageId]
			if !ok {
				dirtyPageTables[log.PageId] = &DirtyPageRecord{PageId: log.PageId, RevLSN: log.LSN}
			}
		case TransBeginLogType, TransAbortLogType:
			if tp == TransBeginLogType {
				activeTransActionTable[log.TransactionId].State = TransactionP
			} else {
				activeTransActionTable[log.TransactionId].State = TransactionU
			}
			activeTransActionTable[log.TransactionId].Lsn = log.LSN
		case TransCommitLogType:
			activeTransActionTable[log.TransactionId].State = TransactionC
			rows := DeserializeRows(log.BeforeValue)
			for _, r := range rows {
				status, ok := commitStatusTable[r]
				if !ok {
					commitStatusTable[r] = &CommitStatus{StartTs: log.StartTs, CommitTs: log.CommitTs, PageId: r, TransId: log.TransactionId, Lsn: log.PrevLsn}
					continue
				}
				if status.CommitTs < log.CommitTs {
					commitStatusTable[r] = &CommitStatus{StartTs: log.StartTs, CommitTs: log.CommitTs, PageId: r, TransId: log.TransactionId, Lsn: log.PrevLsn}
				}
			}
		case TransEndLogType:
			delete(activeTransActionTable, log.TransactionId)
		case CheckPointBeginLogType:
		case CheckPointEndLogType:
			handleCheckPointLogDuringRecovery(log, activeTransActionTable, dirtyPageTables, commitStatusTable)
		}
		lsn += int64(log.Len())
	}
	// Note: in case packet is broken, we redirect the lsn and flushLsn here.
	logManager.Lsn = lsn
	logManager.FlushedLsn = lsn
	for _, entry := range activeTransActionTable {
		if entry.State == TransactionU && entry.UndoNextLsn == InvalidLsn {
			entry.State = TransactionC
		}
	}
	return
}

func redoForRecovery(logManager *LogManager, bufManager *BufferManager, dirtyPageTable map[int32]*DirtyPageRecord) error {
	recLsn := int64(-1)
	for _, record := range dirtyPageTable {
		if recLsn == -1 {
			recLsn = record.RevLSN
			continue
		}
		if recLsn > record.RevLSN {
			recLsn = record.RevLSN
		}
	}
	logIte := logManager.LogIterator(recLsn)
	for logIte.HasNext() {
		log := logIte.Next()
		logManager.updateActiveTransactionTable(log)
		if log.TP != SetLogType && log.TP != CompensationLogType && log.TP != TransCommitLogType {
			continue
		}
		if log.TP == TransCommitLogType {
			bufManager.MarkCommit(log.TransactionId, log.StartTs, log.CommitTs, DeserializeRows(log.BeforeValue))
			continue
		}
		_, ok := dirtyPageTable[log.PageId]
		if !ok || log.LSN < dirtyPageTable[log.PageId].RevLSN {
			continue
		}
		page, err := bufManager.GetPage(log.PageId, log.StartTs)
		if err != nil {
			return err
		}
		if page == nil {
			err = redoLog(bufManager, log)
			if err != nil {
				return err
			}
			page, err = bufManager.GetPage(log.PageId, log.StartTs)
			if err != nil {
				return err
			}
		}
		if page.LSN < log.LSN {
			redoLog(bufManager, log)
			page.LSN = log.LSN
			continue
		}
		dirtyPageTable[log.PageId].RevLSN = page.LSN
	}
	return nil
}

// log must be a compensation log or update log.
func redoLog(bufManager *BufferManager, log *LogRecord) error {
	recoveryLog.InfoF("redoLog: %s", log)
	switch log.ActionTP {
	case AddAction, SetAction:
		pair := &Pair{}
		pair.Deserialize(log.AfterValue)
		return bufManager.Set(log.PageId, pair.Key, pair.Value, log.LSN, log.StartTs, log.CommitTs)
	case DelAction:
		bufManager.Del(log.PageId, log.LSN, log.StartTs, log.CommitTs)
		return nil
	default:
		panic("unknown action type")
	}
}

func redoLog2(logManager *LogManager, bufManager *BufferManager, log *LogRecord, row int32) error {
	for log != nil && log.PageId != row {
		var err error
		log, err = logManager.ReadLog(log.PrevLsn)
		if err != nil {
			panic(err)
		}
	}
	if log == nil {
		panic("cannot find such log")
	}
	recoveryLog.InfoF("redoLog: %s", log)
	switch log.ActionTP {
	case AddAction, SetAction:
		pair := &Pair{}
		pair.Deserialize(log.AfterValue)
		return bufManager.Set(log.PageId, pair.Key, pair.Value, log.LSN, log.StartTs, log.CommitTs)
	case DelAction:
		bufManager.Del(log.PageId, log.LSN, log.StartTs, log.CommitTs)
		return nil
	default:
		panic("unknown action type")
	}
}

func dropTransaction(log *LogRecord, page *Page, commitStatusTable map[int32]*CommitStatus) bool {
	if page == nil {
		return false
	}
	status, ok := commitStatusTable[log.PageId]
	if !ok {
		return false
	}
	if page.CommitTS >= status.CommitTs {
		return true
	}
	return false
}

func undoForRecovery(logManager *LogManager, bufManager *BufferManager, activeTransactionTable map[uint64]*TransactionTableEntry, commitStatusTable map[int32]*CommitStatus) error {
	for {
		entry, maxUndoLsn := maximumUndoLsn(activeTransactionTable)
		if maxUndoLsn == InvalidLsn {
			return nil
		}
		log, err := logManager.ReadLog(maxUndoLsn)
		if err != nil {
			return err
		}
		if log.TP == CompensationLogType || log.TP == SetLogType {
			page, err := bufManager.Disk.Read(log.PageId)
			if err != nil {
				return err
			}
			if page != nil && dropTransaction(log, page, commitStatusTable) {
				recoveryLog.InfoF("cancel a undo log because it has been overwritten. transId: %d")
				delete(activeTransactionTable, page.TransId)
				endLog := &LogRecord{
					PrevLsn:       log.LSN,
					TransactionId: log.TransactionId,
					TP:            TransEndLogType,
				}
				logManager.AppendRecoveryLog(endLog)
				activeTransactionTable[log.TransactionId].State = TransactionC
				continue
			}
			// We roll it to the status we get from the commitStatusTable.
			status, ok := commitStatusTable[log.PageId]
			if ok && page != nil {
				lastCommitLog, err := logManager.ReadLog(status.Lsn)
				if err != nil {
					return err
				}
				recoveryLog.InfoF("rollback to previous commit status: log: %v, status: %v", lastCommitLog, status)
				lastCommitLog.CommitTs = status.CommitTs
				err = redoLog2(logManager, bufManager, lastCommitLog, log.PageId)
				if err != nil {
					return err
				}
				delete(activeTransactionTable, page.TransId)
				endLog := &LogRecord{
					PrevLsn:       log.LSN,
					TransactionId: log.TransactionId,
					TP:            TransEndLogType,
				}
				logManager.AppendRecoveryLog(endLog)
				activeTransactionTable[log.TransactionId].State = TransactionC
				continue
			}
		}
		switch log.TP {
		case TransAbortLogType, TransBeginLogType:
			activeTransactionTable[log.TransactionId].UndoNextLsn = log.PrevLsn
		case CompensationLogType:
			activeTransactionTable[log.TransactionId].UndoNextLsn = log.UndoNextLsn
		case SetLogType:
			l := undoLog(log, bufManager, logManager, activeTransactionTable)
			activeTransactionTable[log.TransactionId].Lsn = l.LSN
			activeTransactionTable[log.TransactionId].UndoNextLsn = log.PrevLsn
			if log.PrevLsn == InvalidLsn {
				endLog := &LogRecord{
					PrevLsn:       log.LSN,
					TransactionId: log.TransactionId,
					TP:            TransEndLogType,
				}
				logManager.AppendRecoveryLog(endLog)
				activeTransactionTable[log.TransactionId].State = TransactionC
			}
		default:
			recoveryLog.InfoF("an abnormal log type during undo. log: %v", log)
			delete(activeTransactionTable, entry.TransactionId)
		}
	}
	return nil
}

func maximumUndoLsn(activeTransactionTable map[uint64]*TransactionTableEntry) (*TransactionTableEntry, int64) {
	maxLsn := int64(InvalidLsn)
	var r *TransactionTableEntry
	for _, entry := range activeTransactionTable {
		if entry.State == TransactionP || entry.State == TransactionC || entry.State == TransactionE {
			continue
		}
		if entry.UndoNextLsn > maxLsn {
			r = entry
			maxLsn = entry.UndoNextLsn
		}
	}
	return r, maxLsn
}

func undoLog(undoLog *LogRecord, bufManager *BufferManager, logManager *LogManager,
	activeTransactionTable map[uint64]*TransactionTableEntry) *LogRecord {
	l := &LogRecord{
		TP:            CompensationLogType,
		TransactionId: undoLog.TransactionId,
		PageId:        undoLog.PageId,
		PrevLsn:       activeTransactionTable[undoLog.TransactionId].Lsn,
		UndoNextLsn:   undoLog.PrevLsn,
		StartTs:       undoLog.StartTs,
		CommitTs:      InvalidTimeStamp,
	}
	recoveryLog.InfoF("undoLog: %s", undoLog)
	var err error
	switch undoLog.ActionTP {
	case AddAction:
		l.ActionTP = DelAction
		logManager.AppendRecoveryLog(l)
		err = bufManager.Del(undoLog.PageId, l.LSN, l.StartTs, l.CommitTs)
	case SetAction:
		l.ActionTP = SetAction
		l.BeforeValue = undoLog.AfterValue
		l.AfterValue = undoLog.BeforeValue
		logManager.AppendRecoveryLog(l)
		pair := &Pair{}
		err = pair.Deserialize(l.AfterValue)
		if err != nil {
			panic(err)
		}
		err = bufManager.Set(undoLog.PageId, pair.Key, pair.Value, l.LSN, l.StartTs, l.CommitTs)
	case DelAction:
		l.ActionTP = AddAction
		l.BeforeValue = nil
		l.AfterValue = undoLog.BeforeValue
		logManager.AppendRecoveryLog(l)
		pair := &Pair{}
		err = pair.Deserialize(l.AfterValue)
		if err != nil {
			panic(err)
		}
		err = bufManager.Set(undoLog.PageId, pair.Key, pair.Value, l.LSN, l.StartTs, l.CommitTs)
	}
	if err != nil {
		panic(err)
	}
	return l
}
