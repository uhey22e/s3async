package s3async

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// truncateDir
func truncateDir(t testing.TB, path string) {
	t.Helper()

	err := os.Mkdir(path, os.ModeDir|os.ModePerm)
	if err != nil {
		if !os.IsExist(err) {
			t.Fatal(err)
		}
	}

	fis, err := ioutil.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, fi := range fis {
		err := os.RemoveAll(filepath.Join(path, fi.Name()))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func dummyJSON(t testing.TB, w io.Writer) {
	t.Helper()

	rb := make([]byte, 32)
	rand.Read(rb)

	type object map[string]interface{}
	v := object{
		"key":        "value",
		"timestamp":  time.Now().Format(time.RFC3339Nano),
		"request_id": uuid.Must(uuid.NewRandom()).String(),
		"data":       base64.StdEncoding.EncodeToString(rb),
		"nested":     dummyNestedObject(t, 5),
	}

	buf := bytes.NewBuffer(nil)
	err := json.NewEncoder(buf).Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	b2 := bytes.Trim(buf.Bytes(), "\n")
	if _, err := w.Write(b2); err != nil {
		t.Fatal(err)
	}
}

type object map[string]interface{}

func dummyNestedObject(t testing.TB, depth int) object {
	t.Helper()
	if depth == 0 {
		return object{
			"level": depth,
			"x":     "last",
		}
	}
	return object{
		"level": depth,
		"x":     dummyNestedObject(t, depth-1),
	}
}

func TestFSDeliveryStream_PutFlush(t *testing.T) {
	const nReqs = 100

	destDirName := "dest"
	truncateDir(t, destDirName)

	// prepare data
	data := make([][]byte, nReqs)
	for i := range data {
		buf := bytes.NewBuffer(nil)
		dummyJSON(t, buf)
		data[i] = buf.Bytes()
	}

	d := NewFSDeliveryStream("dest", "testPutFlush-")

	wg := &sync.WaitGroup{}
	wg.Add(nReqs)
	for i := 0; i < nReqs; i++ {
		go func(wg *sync.WaitGroup, data []byte) {
			d.Put(data)
			wg.Done()
		}(wg, data[i])
	}
	wg.Wait()
	d.Flush()
}

func TestFSDeliveryStream_MultiplePutFlush(t *testing.T) {
	// Test using multiple streams concurrently
	const destDirName = "dest"
	const nTopics = 10
	const nRecords = 10000

	truncateDir(t, destDirName)

	records := make([][]byte, nRecords)
	for i := range records {
		buf := bytes.NewBuffer(nil)
		dummyJSON(t, buf)
		records[i] = buf.Bytes()
	}

	streams := make([]*FSDeliveryStream, nTopics)
	for i := 0; i < nTopics; i++ {
		streams[i] = NewFSDeliveryStream(destDirName, "topic"+strconv.Itoa(i)+"/")
	}

	proc := func(wg *sync.WaitGroup, record []byte) {
		i := mrand.Intn(nTopics)
		s := streams[i]
		s.Put(record)
		wg.Done()
	}

	wg := &sync.WaitGroup{}
	wg.Add(nRecords)
	for _, record := range records {
		go proc(wg, record)
	}
	wg.Wait()

	for _, s := range streams {
		s.Flush()
	}
}
