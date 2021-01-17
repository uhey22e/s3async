package s3async

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"
)

func TestAsyncBuffer_Write(t *testing.T) {
	b := NewAsyncBuffer(nil)

	data := bytes.NewBuffer(nil)
	dummyJSON(t, data)

	n, err := b.Write(data.Bytes())
	if err != nil {
		t.Errorf("AsyncBuffer.Write() error = %v", err)
		return
	}
	if n != data.Len() {
		t.Errorf("AsyncBuffer.Write() got = %v , expect = %v", n, data.Len())
	}

	t.Log(b.buffer.String())
}

func BenchmarkAsyncBuffer_SerialWrite(b *testing.B) {
	const nReq = 1000
	const dataSize = 100 * 1024

	// prepare data
	data := make([][]byte, nReq)
	for i := range data {
		data[i] = make([]byte, dataSize)
		rand.Read(data[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := NewAsyncBuffer(nil)

		for j := 0; j < nReq; j++ {
			buf.buffer.Write(data[j])
		}

		if buf.buffer.Len() != nReq*dataSize {
			b.Fatalf("Size unmatch %v, %v", buf.buffer.Len(), nReq*dataSize)
		}
	}
}

func BenchmarkAsyncBuffer_ConcurrentWrite(b *testing.B) {
	const nReq = 1000
	const dataSize = 100 * 1024

	// prepare data
	data := make([][]byte, nReq)
	for i := range data {
		data[i] = make([]byte, dataSize)
		rand.Read(data[i])
	}

	child := func(wg *sync.WaitGroup, buf *AsyncBuffer, data []byte) {
		defer wg.Done()
		_, err := buf.Write(data)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := NewAsyncBuffer(nil)

		wg := &sync.WaitGroup{}
		wg.Add(nReq)
		for j := 0; j < nReq; j++ {
			go child(wg, buf, data[j])
		}
		wg.Wait()

		if buf.buffer.Len() != nReq*dataSize {
			b.Fatalf("Size unmatch %v, %v", buf.buffer.Len(), nReq*dataSize)
		}
	}
}

func BenchmarkAsyncBuffer_ConcurrentWriteWithWorker(b *testing.B) {
	const nReq = 1000
	const dataSize = 100 * 1024
	const nWorkers = 8

	// prepare data
	data := make([][]byte, nReq)
	for i := range data {
		data[i] = make([]byte, dataSize)
		rand.Read(data[i])
	}

	worker := func(wg *sync.WaitGroup, buf *AsyncBuffer, queue chan []byte) {
		defer wg.Done()
		for {
			b, ok := <-queue
			if !ok {
				return
			}
			_, err := buf.Write(b)
			if err != nil {
				panic(err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := NewAsyncBuffer(nil)
		q := make(chan []byte, nReq)

		wg := &sync.WaitGroup{}
		wg.Add(nWorkers)
		for j := 0; j < nWorkers; j++ {
			go worker(wg, buf, q)
		}
		for j := 0; j < nReq; j++ {
			q <- data[j]
		}
		close(q)
		wg.Wait()

		if buf.buffer.Len() != nReq*dataSize {
			b.Fatalf("Size unmatch %v, %v", buf.buffer.Len(), nReq*dataSize)
		}
	}
}
