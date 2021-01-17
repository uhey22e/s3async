package s3async

import (
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// DeliveryStream .
type DeliveryStream interface {
	Put([]byte) (int, error)
	Flush() (int, error)
}

// FSDeliveryStream .
type FSDeliveryStream struct {
	DestDir     string
	Prefix      string
	asyncBuffer *AsyncBuffer
}

// NewFSDeliveryStream .
func NewFSDeliveryStream(destDir, prefix string) *FSDeliveryStream {
	d := &FSDeliveryStream{
		DestDir:     destDir,
		Prefix:      prefix,
		asyncBuffer: NewAsyncBuffer(nil),
	}
	return d
}

// Put .
func (d *FSDeliveryStream) Put(data []byte) (int, error) {
	return d.asyncBuffer.Write(data)
}

// Flush .
func (d *FSDeliveryStream) Flush() (int, error) {
	ts := time.Now().UnixNano()
	fpath := filepath.Join(d.DestDir, d.Prefix+strconv.FormatInt(ts, 10))

	dir, _ := filepath.Split(fpath)
	err := os.MkdirAll(dir, os.ModeDir|os.ModePerm)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE, os.FileMode(0644))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	return f.Write(d.asyncBuffer.Bytes())
}
