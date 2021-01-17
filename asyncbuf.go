package s3async

import (
	"bytes"
	"sync"
)

// AsyncBuffer provides thread-safe *bytes.Buffer
type AsyncBuffer struct {
	buffer *bytes.Buffer
	mutex  *sync.Mutex
}

var (
	initialBufferSize = 1 * 1024 * 1024
)

// NewAsyncBuffer .
func NewAsyncBuffer(p []byte) *AsyncBuffer {
	b := &AsyncBuffer{
		buffer: bytes.NewBuffer(p),
		mutex:  &sync.Mutex{},
	}
	b.buffer.Grow(initialBufferSize)
	return b
}

// Write is the thread-safe version of (*bytes.Buffer).Write()
func (b *AsyncBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

// Bytes is the thread-safe version of (*bytes.Buffer).Write()
func (b *AsyncBuffer) Bytes() []byte {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Bytes()
}
