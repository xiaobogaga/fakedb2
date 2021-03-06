package internal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/xiaobogaga/fakedb2/util"
	"sync"
	"time"
)

var bufManagerLog = util.GetLog("bufManager")

type BufferManager struct {
	Data          map[int32]*MultiVersionPage
	Lock          sync.Mutex
	Disk          *DiskManager
	Ctx           context.Context
	FlushDuration time.Duration
}

func NewBufferManager(ctx context.Context, dataFile string, flushDuration time.Duration) (*BufferManager, error) {
	diskManager, err := NewDiskManager(dataFile)
	if err != nil {
		return nil, err
	}
	return &BufferManager{
		Data:          map[int32]*MultiVersionPage{},
		Lock:          sync.Mutex{},
		Disk:          diskManager,
		Ctx:           ctx,
		FlushDuration: flushDuration,
	}, nil
}

type MultiVersionPage struct {
	Pages map[int64]*Page
}

func (pages *MultiVersionPage) Get(startTs int64) *Page {
	largestCommitTs := int64(-1)
	var latestPage *Page
	for _, page := range pages.Pages {
		// latest page.
		if page.CommitTS > largestCommitTs && page.CommitTS < startTs {
			latestPage = page
			largestCommitTs = page.CommitTS
		}
		if page.StartTS == startTs {
			latestPage = page
			break
		}
	}
	return latestPage
}

// Return the page identified by the startTs.
func (pages *MultiVersionPage) GetByTs(startTs int64) *Page {
	for _, page := range pages.Pages {
		if page.StartTS == startTs {
			return page
		}
	}
	return nil
}

type Page struct {
	Dirty   bool
	Key     []byte
	Value   []byte
	LSN     int64
	RecvLsn int64 // recovery lsn. won't save to Disk.

	TransId  uint64 // the transId modified this page.
	CommitTS int64
	StartTS  int64
}

// +----------+------------+
// + page len + page bytes +
// +----------+------------+
//            + lsn + key len + key bytes + value len + value bytes
func (page *Page) Serialize() []byte {
	ret := make([]byte, page.Len())
	binary.BigEndian.PutUint64(ret, uint64(page.LSN))
	binary.BigEndian.PutUint64(ret[8:], uint64(page.StartTS))
	binary.BigEndian.PutUint64(ret[16:], uint64(page.CommitTS))
	binary.BigEndian.PutUint32(ret[24:], uint32(len(page.Key)))
	copy(ret[28:], page.Key)
	binary.BigEndian.PutUint32(ret[28+len(page.Key):], uint32(len(page.Value)))
	copy(ret[32+len(page.Key):], page.Value)
	return ret
}

var damagedPacket = errors.New("damaged packet")

func (page *Page) Deserialize(data []byte) error {
	len := len(data)
	if len < 32 {
		return damagedPacket
	}
	page.LSN = int64(binary.BigEndian.Uint64(data))
	page.StartTS = int64(binary.BigEndian.Uint64(data[8:]))
	page.CommitTS = int64(binary.BigEndian.Uint64(data[16:]))
	keyLen := binary.BigEndian.Uint32(data[24:])
	if uint32(len) < 28+keyLen {
		return damagedPacket
	}
	page.Key = data[28 : keyLen+28]
	valueLen := binary.BigEndian.Uint32(data[28+keyLen:])
	if uint32(len) < 32+keyLen+valueLen {
		return damagedPacket
	}
	page.Value = data[32+keyLen : 32+keyLen+valueLen]
	return nil
}

func (page *Page) Len() uint32 {
	return uint32(8*3 + 4 + len(page.Key) + 4 + len(page.Value)) // page len + key len + key bytes + value len + value bytes + lsn len
}

// Return whether found the key, the value of the key if found. error.
func (buffer *BufferManager) Get(id int32, key []byte, startTs int64) (bool, []byte, error) {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	page, err := buffer.getOrLoadPage(id, startTs)
	if err != nil {
		return false, nil, err
	}
	if page == nil {
		return false, nil, nil
	}
	return bytes.Compare(page.Key, key) == 0, page.Value, nil
}

func (buffer *BufferManager) Set(id int32, key, value []byte, lsn int64, startTs, commitTs int64) error {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	multiVersionPage, ok := buffer.Data[id]
	if !ok {
		page := &Page{Dirty: true, Key: key, Value: value, LSN: lsn, RecvLsn: InvalidLsn, StartTS: startTs, CommitTS: commitTs}
		multiPage := &MultiVersionPage{Pages: map[int64]*Page{}}
		multiPage.Pages[page.StartTS] = page
		buffer.Data[id] = multiPage
		return nil
	}
	page := multiVersionPage.GetByTs(startTs)
	if page == nil {
		page = &Page{Key: key, Value: value, LSN: lsn, RecvLsn: lsn, StartTS: startTs, CommitTS: commitTs}
		multiVersionPage.Pages[page.StartTS] = page
	}
	if page.RecvLsn == InvalidLsn {
		page.RecvLsn = lsn
	}
	page.Dirty = true
	page.LSN = lsn
	page.Key = key
	page.Value = value
	return nil
}

func (buffer *BufferManager) Undo(id int32, key, value []byte, lsn int64, startTs, commitTs int64) {
	buffer.Set(id, key, value, lsn, startTs, commitTs)
}

func (buffer *BufferManager) Del(id int32, lsn int64, startTs, commitTs int64) (err error) {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	multiVersionPage, ok := buffer.Data[id]
	if !ok {
		page := &Page{Dirty: true, Key: nil, Value: nil, LSN: lsn, RecvLsn: InvalidLsn, StartTS: startTs, CommitTS: commitTs}
		multiPage := &MultiVersionPage{Pages: map[int64]*Page{}}
		multiPage.Pages[page.StartTS] = page
		buffer.Data[id] = multiPage
		return nil
	}
	page := multiVersionPage.GetByTs(startTs)
	if page == nil {
		page = &Page{Key: nil, Value: nil, LSN: lsn, RecvLsn: lsn, StartTS: startTs, CommitTS: commitTs}
		multiVersionPage.Pages[page.StartTS] = page
	}
	if page.RecvLsn == InvalidLsn {
		page.RecvLsn = lsn
	}
	page.Dirty = true
	page.LSN = lsn
	page.Key = nil
	page.Value = nil
	return nil
}

func (buffer *BufferManager) GetPage(id int32, startTs int64) (*Page, error) {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	page, err := buffer.getOrLoadPage(id, startTs)
	if err != nil {
		return nil, err
	}
	return page, nil
}

func (buffer *BufferManager) Size(startTs int64) int {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	ret := 0
	for i := 1; i <= 2; i++ {
		page, _ := buffer.getOrLoadPage(int32(i), startTs)
		if page == nil || (len(page.Key) == 0 && len(page.Value) == 0) {
			continue
		}
		ret++
	}
	return ret
}

func (buffer *BufferManager) getOrLoadPage(id int32, startTs int64) (*Page, error) {
	var err error
	pages, ok := buffer.Data[id]
	if !ok {
		page, err := buffer.Disk.Read(id)
		if err != nil {
			return nil, err
		}
		if page == nil {
			return nil, nil
		}
		pages = &MultiVersionPage{Pages: map[int64]*Page{
			page.StartTS: page,
		}}
		buffer.Data[id] = pages
		return pages.Get(startTs), nil
	}
	page := pages.Get(startTs)
	if page == nil {
		page, err = buffer.Disk.Read(id)
		if err != nil {
			return nil, err
		}
		if page != nil && pages.Pages[page.StartTS] == nil {
			pages.Pages[page.StartTS] = page
		}
		page = pages.Get(startTs)
	}
	return page, nil
}

func (buffer *BufferManager) IsEmpty(id int32, startTs int64) bool {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	page, _ := buffer.getOrLoadPage(id, startTs)
	return page == nil || (len(page.Key) == 0 && len(page.Value) == 0)
}

func (buffer *BufferManager) FlushDirtyPagesRegularly(logManager *LogManager) {
	bufManagerLog.InfoF("start flush dirty page goroutine")
	defer bufManagerLog.InfoF("end flush dirty page goroutine")
	for {
		select {
		case <-time.After(buffer.FlushDuration):
		case <-buffer.Ctx.Done():
			return
		}
		buffer.Lock.Lock()
		for id, pages := range buffer.Data {
			for _, page := range pages.Pages {
				if page.LSN <= logManager.GetFlushedLsn() {
					bufManagerLog.InfoF("write dirty page: lsn: %d, key: %s, value: %s", page.LSN, string(page.Key), string(page.Value))
					err := buffer.Disk.Write(id, page)
					if err != nil {
						panic(err)
					}
					// delete(pages.Pages, page.StartTS)
				}
			}
		}
		buffer.Lock.Unlock()
	}
}

var dirtyPageTableSize = 28

type DirtyPageRecord struct {
	PageId   int32
	RevLSN   int64
	StartTs  int64
	CommitTs int64
}

func (record *DirtyPageRecord) Serialize() []byte {
	ret := make([]byte, dirtyPageTableSize)
	binary.BigEndian.PutUint32(ret, uint32(record.PageId))
	binary.BigEndian.PutUint64(ret[4:], uint64(record.RevLSN))
	binary.BigEndian.PutUint64(ret[12:], uint64(record.StartTs))
	binary.BigEndian.PutUint64(ret[20:], uint64(record.CommitTs))
	return ret
}

func (record *DirtyPageRecord) Deserialize(data []byte) error {
	if len(data) < dirtyPageTableSize {
		return damagedPacket
	}
	record.PageId = int32(binary.BigEndian.Uint32(data))
	record.RevLSN = int64(binary.BigEndian.Uint64(data[4:]))
	record.StartTs = int64(binary.BigEndian.Uint64(data[12:]))
	record.CommitTs = int64(binary.BigEndian.Uint64(data[20:]))
	return nil
}

func (record *DirtyPageRecord) Len() int {
	return dirtyPageTableSize
}

func (record *DirtyPageRecord) String() string {
	return fmt.Sprintf("[pageId: %d, recLSN: %d, startTs: %d, commitTs: %d]", record.PageId, record.RevLSN,
		record.StartTs, record.CommitTs)
}

func (buffer *BufferManager) DirtyPageRecordTable() []*DirtyPageRecord {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	var ret []*DirtyPageRecord
	for id, pages := range buffer.Data {
		for _, page := range pages.Pages {
			if !page.Dirty {
				continue
			}
			ret = append(ret, &DirtyPageRecord{PageId: id, RevLSN: page.RecvLsn, StartTs: page.StartTS, CommitTs: page.CommitTS})
		}
	}
	return ret
}

func (buffer *BufferManager) FlushDirtyPage(logManager *LogManager) {
	buffer.Lock.Lock()
	for id, pages := range buffer.Data {
		for _, page := range pages.Pages {
			if page.LSN <= logManager.GetFlushedLsn() {
				bufManagerLog.InfoF("write dirty page: lsn: %d, key: %s, value: %s", page.LSN, string(page.Key), string(page.Value))
				err := buffer.Disk.Write(id, page)
				if err != nil {
					panic(err)
				}
				// delete(pages.Pages, page.StartTS)
			}
		}
	}
	buffer.Lock.Unlock()
}

func (buffer *BufferManager) MarkCommit(transId uint64, startTs, commitTs int64, rows []int32) {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	bufManagerLog.InfoF("mark commit: trans: %d, start timestamp: %d, commit timestamp: %d, rows: %v", transId, startTs, commitTs, rows)
	for _, row := range rows {
		pages, ok := buffer.Data[row]
		if !ok {
			continue
		}
		for _, page := range pages.Pages {
			if page.StartTS == startTs {
				page.CommitTS = commitTs
				page.Dirty = true
			}
		}
	}
}
