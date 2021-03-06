package internal

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestDiskManager_Read(t *testing.T) {
	f, err := ioutil.TempFile("/tmp", "fakedb2.")
	assert.Nil(t, err)
	page := &Page{
		Key:   []byte{1},
		Value: []byte{2},
		LSN:   0,
	}
	diskManager, err := NewDiskManager(f.Name())
	assert.Nil(t, err)
	err = diskManager.Write(0, page)
	assert.Nil(t, err)
	another, err := diskManager.Read(0)
	assert.Nil(t, err)
	assert.Equal(t, page, another)
	err = os.Remove(f.Name())
	assert.Nil(t, err)
}
