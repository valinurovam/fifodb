package fifodb_test

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/valinurovam/fifodb"
)

const (
	defaultBytesPerSegment = 1 << 10 // 1kb
	defaultMessageCount    = 100
)

func TestMain(m *testing.M) {
	cleanTestDBPath := func() {
		err := os.RemoveAll("test")
		if err != nil {
			panic("unexpected error on remove test folder")
		}
	}
	cleanTestDBPath()

	test := func() (code int) {
		defer cleanTestDBPath()

		return m.Run()
	}

	os.Exit(test())
}

func TestDb_Open_Close_Empty(t *testing.T) {
	db, msgLeft, err := openDB(0, t)

	assert.Nil(t, err)
	assert.EqualValues(t, 0, msgLeft)

	defer func() {
		if err := os.RemoveAll(db.GetPath()); err != nil {
			t.Fatal("Unexpected error on remove db", err)
		}
	}()

	assert.Nil(t, db.Close())
}

func TestDb_Open_Close_Non_Empty(t *testing.T) {
	msgCount := defaultMessageCount
	db, _, err := openDB(msgCount, t)

	defer func() {
		if err := os.RemoveAll(db.GetPath()); err != nil {
			t.Fatal("Unexpected error on remove db", err)
		}
	}()

	assert.Nil(t, err)
	assert.Nil(t, db.Close())

	db, msgLeft, err := openDB(0, t)
	assert.Nil(t, err)
	assert.EqualValues(t, msgCount, msgLeft)
}

func TestDb_Pop_WithOrderCheck(t *testing.T) {
	msgCount := defaultMessageCount
	db, _, _ := openDB(msgCount, t)

	defer func() {
		if err := os.RemoveAll(db.GetPath()); err != nil {
			t.Fatal("Unexpected error on remove db", err)
		}
	}()

	for i := 0; i < msgCount; i++ {
		expectedData := make([]byte, 8)
		binary.BigEndian.PutUint64(expectedData, uint64(i))

		data, _, _, err := db.Pop()
		assert.Nil(t, err)
		assert.Equal(t, data, expectedData)
	}

	// check pop on empty db
	data, _, _, err := db.Pop()
	assert.Nil(t, data)
	assert.Nil(t, err)
}

func TestDb_Pop_No_Ack_Safe(t *testing.T) {
	msgCount := defaultMessageCount
	db, _, _ := openDB(msgCount, t)

	defer func() {
		if err := os.RemoveAll(db.GetPath()); err != nil {
			t.Fatal("Unexpected error on remove db", err)
		}
	}()

	for i := 0; i < msgCount; i++ {
		_, _, _, err := db.Pop()
		assert.Nil(t, err)
	}

	assert.Nil(t, db.Close())

	db, msgLeft, _ := openDB(0, t)
	assert.EqualValues(t, msgCount, msgLeft)
}

func TestDb_NoPanicAfterClose(t *testing.T) {
	db, _, err := openDB(0, t)

	assert.Nil(t, err)
	assert.Nil(t, db.Close())
	_, _, err = db.Push([]byte{'b'})
	assert.NotNil(t, err)

	defer func() {
		if err := os.RemoveAll(db.GetPath()); err != nil {
			t.Fatal("Unexpected error on remove db", err)
		}
	}()

	assert.Error(t, db.Close())
}

func TestDb_Pop_Ack_All(t *testing.T) {
	msgCount := defaultMessageCount
	db, _, _ := openDB(msgCount, t)

	defer func() {
		if err := os.RemoveAll(db.GetPath()); err != nil {
			t.Fatal("Unexpected error on remove db", err)
		}
	}()

	for i := 0; i < msgCount; i++ {
		_, segID, offset, err := db.Pop()
		assert.Nil(t, err)

		assert.Nil(t, db.Ack(segID, offset))

		assert.Nil(t, err)
	}

	assert.Nil(t, db.Close())

	db, msgLeft, _ := openDB(0, t)
	assert.EqualValues(t, 0, msgLeft)
}

func TestDb_Pop_Ack_Half_Order(t *testing.T) {
	msgCount := defaultMessageCount
	db, _, _ := openDB(msgCount, t)

	defer func() {
		if err := os.RemoveAll(db.GetPath()); err != nil {
			t.Fatal("Unexpected error on remove db", err)
		}
	}()

	for i := 0; i < msgCount; i++ {
		_, segID, offset, err := db.Pop()
		assert.Nil(t, err)

		if i%2 == 0 {
			assert.Nil(t, db.Ack(segID, offset))
		}
	}

	assert.Nil(t, db.Close())
	db, msgLeft, _ := openDB(0, t)
	assert.EqualValues(t, msgCount/2, msgLeft)

	for i := 0; i < msgCount/2; i++ {
		expectedData := make([]byte, 8)
		binary.BigEndian.PutUint64(expectedData, uint64(i*2+1))

		data, _, _, err := db.Pop()
		assert.Nil(t, err)

		assert.Equal(t, data, expectedData)
	}
}

func TestDb_Clean(t *testing.T) {
	msgCount := defaultMessageCount
	db, _, _ := openDB(msgCount, t)
	assert.Nil(t, db.Close())

	db, msgLeft, err := openDB(0, t)

	assert.Nil(t, err)
	assert.EqualValues(t, msgCount, msgLeft)

	assert.Nil(t, db.Clean())
	assert.Nil(t, db.Close())

	_, msgLeft, err = openDB(0, t)

	assert.Nil(t, err)
	assert.EqualValues(t, 0, msgLeft)
}

func TestDb_Push_Pop_Concurrently(t *testing.T) {
	db, _, _ := openDB(0, t)
	defer db.Close()

	var (
		g       errgroup.Group
		pushCnt uint32
		popCnt  uint32
	)

	for i := 0; i < runtime.NumCPU(); i++ {
		g.Go(func() error {
			data := make([]byte, 8)
			for i := 0; i < defaultMessageCount; i++ {
				binary.BigEndian.PutUint64(data, uint64(i))

				if _, _, err := db.Push(data); err != nil {
					return err
				}
				atomic.AddUint32(&pushCnt, 1)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatal("Unexpected error on push data", err)
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		g.Go(func() error {
			for {
				data, _, _, err := db.Pop()
				if err != nil {
					return err
				}

				if data != nil {
					atomic.AddUint32(&popCnt, 1)
				} else {
					break
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatal("Unexpected error on pop data", err)
	}

	assert.EqualValues(t, pushCnt, popCnt)
}

func TestCrashRecovery_SingleSegment(t *testing.T) {
	db, _, err := openDB(0, t)
	assert.NoError(t, err)

	msg1 := []byte("message-1")
	msg2 := []byte("message-2")
	msg3 := []byte("message-3")

	segId1, offsetId1, err := db.Push(msg1)
	assert.NoError(t, err)
	_, _, err = db.Push(msg2)
	assert.NoError(t, err)
	_, _, err = db.Push(msg3)
	assert.NoError(t, err)

	err = db.Ack(segId1, offsetId1)
	assert.NoError(t, err)

	// reopen db - kill -9 imitation
	db2, _, err := openDB(0, t)
	defer db2.Close()

	// expect msg2 and msg3
	data, _, _, err := db2.Pop()
	assert.NoError(t, err)
	assert.Equal(t, msg2, data)

	data, _, _, err = db2.Pop()
	assert.NoError(t, err)
	assert.Equal(t, msg3, data)

	// Очередь должна быть пуста
	data, _, _, err = db2.Pop()
	assert.NoError(t, err)
	assert.Nil(t, data)
}

func TestCrashRecovery_PushNotAcked_WALBuffered(t *testing.T) {
	dir := t.TempDir()

	// Open DB with large WAL buffer (64KB) so Push operation stays in memory,
	// and small segment buffer (1 byte) to ensure message is written to disk immediately.
	db, _, err := fifodb.Open(fifodb.Options{
		Path:               dir,
		MaxBytesPerSegment: 1024,  // small segment size
		WriteBufferSize:    1,     // forces immediate write to segment file
		WALWriteBufferSize: 65536, // large WAL buffer — data stays in memory
	})
	assert.NoError(t, err)

	// Push a message. It will be written to the segment file (due to WriteBufferSize=1),
	// but the corresponding Push op will remain in WAL buffer (not flushed to disk).
	msg := []byte("important message")
	segID, offset, err := db.Push(msg)
	assert.NoError(t, err)
	_ = segID
	_ = offset

	// Simulate crash: do NOT call db.Close(), so WAL buffer is never flushed.
	// This mimics a kill -9 scenario.

	// Reopen the database
	db2, _, err := fifodb.Open(fifodb.Options{
		Path:               dir,
		MaxBytesPerSegment: 1024,
		WriteBufferSize:    1,
		ReadBufferSize:     1,
		WALWriteBufferSize: 1, // buffer size doesn't matter on read
	})
	assert.NoError(t, err)
	defer db2.Close()

	// The message must still be present — even though WAL was empty on disk.
	data, _, _, err := db2.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, msg, data)

	// Queue should be empty after popping the message
	data, _, _, err = db2.Pop()
	assert.NoError(t, err)
	assert.Nil(t, data)
}

func openDB(msgCount int, t *testing.T) (db *fifodb.DB, msgLeft uint64, err error) {
	opt := fifodb.DefaultOptions
	opt.WriteBufferSize = 1    // to avoid bufferization
	opt.WALWriteBufferSize = 1 // to avoid bufferization
	opt.MaxBytesPerSegment = defaultBytesPerSegment
	opt.Path = fmt.Sprintf("test/%s", t.Name())
	db, msgLeft, err = fifodb.Open(opt)

	if err != nil {
		t.Fatal("Unexpected error on openDB open", err)
	}

	for i := 0; i < msgCount; i++ {
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, uint64(i))

		if _, _, err := db.Push(data); err != nil {
			t.Fatal("Unexpected error on openDB push data", err)
		}
	}

	return
}
