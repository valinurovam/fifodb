package fifodb

import (
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
)

const (
	dataSegmentsName      = "data"
	dataSegmentsPattern   = dataSegmentsName + "_%020d.dsf"
	opsLogSegmentsName    = "ops"
	opsLogSegmentsPattern = opsLogSegmentsName + "_%020d.osf"
)

// DB represents thread-safe FIFO disk queue with WAL of queue operations.
type DB struct {
	path   string
	sc     *segmentsController
	opsLog *opsLog

	lock   sync.RWMutex
	closed bool
}

func Open(opt Options) (db *DB, msgLeft uint64, err error) {
	opt.Path = path.Clean(opt.Path)

	if err = os.MkdirAll(opt.Path, 0700); err != nil {
		return nil, 0, errors.Wrapf(err, "create directory %s", opt.Path)
	}

	db = &DB{
		path: opt.Path,
	}

	db.sc = newSegmentsController(opt)
	db.opsLog = newOperationsLog(opt.Path)

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

func (db *DB) Sync() error {
	if err := db.sc.sync(); err != nil {
		return errors.Wrap(err, "db logSegments sync")
	}

	if err := db.opsLog.sync(); err != nil {
		return errors.Wrap(err, "db ops log sync")
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

	err = db.opsLog.Write(segID, offset)
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

	return db.opsLog.Delete(segID, offset)
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

	if db.opsLog != nil {
		if err = db.opsLog.Close(); err != nil {
			return err
		}
	}

	db.opsLog = nil

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

	if err := db.opsLog.Clean(); err != nil {
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

	files, err := ioutil.ReadDir(db.path)
	if err != nil {
		return
	}

	for _, f := range files {
		if !db.opsLog.isCorrectFileName(f.Name()) {
			continue
		}

		if segID, segErr = db.opsLog.getSegIDByFileName(f.Name()); segErr != nil {
			return 0, errors.Wrap(segErr, "compactOnStartUp error")
		}

		maxSegID = segID
	}

	for segID := 0; segID <= int(maxSegID); segID++ {
		if msgLeft, err = db.compactSegment(true, uint32(segID)); err != nil {
			return 0, errors.Wrap(err, "compactOnStartUp error")
		}

		totalMsgLeft += msgLeft
	}

	return
}

func (db *DB) compactSegment(startUp bool, segID uint32) (msgLeft uint64, err error) {
	var meta *compactionMeta

	if meta, err = db.opsLog.getDataForCompaction(segID); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}

		return 0, err
	}

	if meta.full() {
		if err = db.opsLog.release(segID); err != nil {
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
			if err = db.opsLog.release(segID); err != nil {
				return 0, err
			}

			if len(offsets) > 0 {
				if err = db.opsLog.createAfterCompaction(offsets, segID); err != nil {
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
