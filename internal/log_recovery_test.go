package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDeserializeTransactionTableEntryAndPageCommitTable(t *testing.T) {
	transTable := []*TransactionTableEntry{
		{TransactionId: 1, UndoNextLsn: 3},
		{TransactionId: 2, UndoNextLsn: 4},
	}
	commitStatus := []*CommitStatus{
		{PageId: 1, TransId: 2, Lsn: 4},
		{PageId: 2, TransId: 3, Lsn: 5},
	}

	data := SerializeTransactionTableEntryAndPageCommitTable(transTable, commitStatus)

	dirtyPages := []*DirtyPageRecord{
		{PageId: 1, CommitTs: 10},
		{PageId: 2, CommitTs: 11},
	}
	data1 := SerializeDirtyPageRecord(dirtyPages)

	a1, a2 := DeserializeTransactionTableEntryAndPageCommitTable(data)
	a3 := DeserializeDirtyPageRecord(data1)

	assert.Equal(t, transTable, a1)
	assert.Equal(t, commitStatus, a2)

	assert.Equal(t, dirtyPages, a3)

}
