package fifodb

import (
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
)

const (
	segmentsName        = "segment"
	dataSegmentsPattern = segmentsName + "_%020d.dat"
	walSegmentsSuffix   = "wal"
	walSegmentsPattern  = segmentsName + "_%020d." + walSegmentsSuffix
)

// DB represents thread-safe FIFO disk queue with WAL of queue operations.
type DB struct {
	path string
	sc   *segmentsController
	wal  *walController

	lock   sync.RWMutex
	closed bool

	log Logger
}

func Open(opt Options) (db *DB, msgLeft uint64, err error) {
	log := opt.Logger
	if log == nil {
		log = &noopLogger{}
	}

	opt.Path = path.Clean(opt.Path)

	if err = os.MkdirAll(opt.Path, 0700); err != nil {
		log.Printf("failed to create directory %s", opt.Path)
		return nil, 0, errors.Wrapf(err, "failed to create directory %s", opt.Path)
	}

	db = &DB{
		path: opt.Path,
		log:  log,
	}

	db.sc = newSegmentsController(opt)
	db.wal = newWalController(opt.Path, opt.WALWriteBufferSize, db.log)

	if msgLeft, err = db.compactOnStartUp(); err != nil {
		return
	}

	if err = db.sc.load(); err != nil {
		return
	}

	return
}

func (db *DB) SetReadPos(fileID uint32, offset uint32) error {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.sc.setReadPos(fileID, offset)
}

// Sync flushes all pending data to stable storage.
// It is NOT safe to call Sync concurrently with Push, Ack, or Pop.
// The caller must ensure external synchronization if concurrent access is needed.
func (db *DB) Sync() error {
	if err := db.sc.sync(); err != nil {
		db.log.Printf("db: failed to sync data segments")
		return errors.Wrap(err, "db: failed to sync data segments")
	}

	if err := db.wal.sync(); err != nil {
		db.log.Printf("db: failed to sync ops log")
		return errors.Wrap(err, "db: failed to sync ops log")
	}

	return nil
}

func (db *DB) Push(data []byte) (segID uint32, offset uint32, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if err = db.errOnClosed(); err != nil {
		return
	}

	if segID, offset, err = db.sc.write(data); err != nil {
		return
	}

	err = db.wal.Write(segID, offset)
	if err != nil {
		return 0, 0, err
	}

	return
}

func (db *DB) Pop() (data []byte, segID uint32, offset uint32, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if err = db.errOnClosed(); err != nil {
		return
	}

	return db.sc.read()
}

func (db *DB) Ack(segID uint32, offset uint32) (err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if err = db.errOnClosed(); err != nil {
		return
	}

	return db.wal.Delete(segID, offset)
}

func (db *DB) Close() (err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if err = db.errOnClosed(); err != nil {
		return
	}

	db.closed = true

	if db.sc != nil {
		if err = db.sc.Close(); err != nil {
			return err
		}
	}

	db.sc = nil

	if db.wal != nil {
		if err = db.wal.Close(); err != nil {
			return err
		}
	}

	db.wal = nil

	return
}

// Clean deletes all db files
// Thread-safe.
func (db *DB) Clean() (err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if err = db.errOnClosed(); err != nil {
		return
	}

	if err := db.wal.Clean(); err != nil {
		return err
	}

	if err := db.sc.Clean(); err != nil {
		return err
	}

	if err := db.sc.load(); err != nil {
		return err
	}

	return nil
}

func (db *DB) GetPath() string {
	return db.path
}

func (db *DB) errOnClosed() error {
	if db.closed {
		return errors.New("db is closed")
	}

	return nil
}

func (db *DB) compactOnStartUp() (totalMsgLeft uint64, err error) {
	var (
		maxSegID, segID uint32
		segErr          error
		msgLeft         uint64
	)

	files, err := os.ReadDir(db.path)
	if err != nil {
		return
	}

	for _, f := range files {
		if !db.wal.isCorrectFileName(f.Name()) {
			continue
		}

		if segID, segErr = db.wal.getSegIDByFileName(f.Name()); segErr != nil {
			db.log.Printf("error on compactOnStartUp: %v", segErr)
			return 0, errors.Wrap(segErr, "compactOnStartUp error")
		}

		maxSegID = segID
	}

	for segID := 0; segID <= int(maxSegID); segID++ {
		if msgLeft, err = db.compactSegment(true, uint32(segID)); err != nil {
			db.log.Printf("compactSegment failed on segId %d: %v", segID, err)
			return 0, errors.Wrap(err, "compactOnStartUp error, compactSegment failed")
		}

		totalMsgLeft += msgLeft
	}

	return
}

func (db *DB) compactSegment(startUp bool, segID uint32) (msgLeft uint64, err error) {
	var meta *compactionMeta

	if meta, err = db.wal.getDataForCompaction(segID); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}

		return 0, err
	}

	if meta.full() {
		if err = db.wal.release(segID); err != nil {
			return 0, err
		}

		if err = os.Remove(db.sc.fileNameBySegID(segID)); err != nil {
			return 0, err
		}
	} else if startUp {
		if meta.needCompactLog() {
			var offsets []uint32
			if offsets, err = db.sc.compactSegment(segID, meta); err != nil {
				return 0, err
			}
			if err = db.wal.release(segID); err != nil {
				return 0, err
			}

			if len(offsets) > 0 {
				if err = db.wal.createAfterCompaction(offsets, segID); err != nil {
					return 0, err
				}
			}
			msgLeft = uint64(len(offsets))
		} else {
			msgLeft = meta.wrCnt
		}
	}

	return
}
