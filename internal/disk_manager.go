package internal

import (
	"github.com/xiaobogaga/fakedb2/util"
	"io"
	"os"
	"sync"
)

var diskLog = util.GetLog("disk")

type DiskManager struct {
	Lock   sync.Mutex
	Writer *os.File
}

const PageSize = 16 * 1024 // 16K

func NewDiskManager(dataFile string) (*DiskManager, error) {
	writer, err := os.OpenFile(dataFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &DiskManager{Writer: writer}, nil
}

func (disk *DiskManager) Write(index int32, page *Page) error {
	disk.Lock.Lock()
	defer disk.Lock.Unlock()
	_, err := disk.Writer.Seek(int64(index*PageSize), 0)
	if err != nil {
		return err
	}
	data := page.Serialize()
	if len(data) < PageSize {
		padding := make([]byte, PageSize-len(data))
		data = append(data, padding...)
	}
	for i := uint32(0); i < page.Len(); {
		size, err := disk.Writer.Write(data[i:])
		if err != nil {
			return err
		}
		i += uint32(size)
	}
	return nil
}

func (disk *DiskManager) Read(index int32) (page *Page, err error) {
	disk.Lock.Lock()
	defer disk.Lock.Unlock()
	_, err = disk.Writer.Seek(int64(index*PageSize), 0)
	if err != nil {
		return
	}
	data := make([]byte, PageSize)
	i := 0
	for i = 0; i < PageSize; {
		size, err := disk.Writer.Read(data[i:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF {
			err = nil
			break
		}
		i += size
	}
	page = &Page{}
	err1 := page.Deserialize(data[:i])
	if err1 != nil {
		// possible a broken packet.
		diskLog.InfoF("possible a broken packet on data. now ignore it.")
		return nil, nil
	}
	return
}
