package fifodb

import (
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path"
)

const dataSegmentsPattern = "data_%020d.dsf"
const opsLogSegmentsPattern = "ops_%020d.osf"

type Entry struct {
	Data []byte

	// internal fields
	segID  uint32
	offset uint32
}

// Db represents thread-safe FIFO disk queue with WAL of queue operations
type Db struct {
	path   string
	sc     *segmentsController
	opsLog *opsLog
}

func Open(opt Options) (db *Db, msgLeft uint64, err error) {
	opt.Path = path.Clean(opt.Path)

	if err = os.MkdirAll(opt.Path, 0700); err != nil {
		return nil, 0, errors.Wrapf(err, "create directory %s", opt.Path)
	}

	db = &Db{
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

func (db *Db) SetReadPos(fileID uint32, offset uint32) error {
	return db.sc.setReadPos(fileID, offset)
}

func (db *Db) Push(data []byte) (segID uint32, offset uint32, err error) {
	if segID, offset, err = db.sc.write(data); err != nil {
		return
	}

	err = db.opsLog.Write(segID, offset)
	if err != nil {
		return 0, 0, err
	}

	return
}

func (db *Db) Sync() {
	db.sc.sync()
	db.opsLog.sync()
}

func (db *Db) Pop() (data []byte, segID uint32, offset uint32, err error) {
	return db.sc.read()
}

func (db *Db) Ack(segID uint32, offset uint32) error {
	return db.opsLog.Delete(segID, offset)
}

func (db *Db) Nack(segID uint32, offset uint32) error {
	return db.opsLog.Requeue(segID, offset)
}

func (db *Db) Close() (err error) {
	err = db.sc.Close()
	if err != nil {
		return err
	}
	return db.opsLog.Close()
}

func (db *Db) Delete() error {
	return db.sc.Delete()
}

func (db *Db) compactOnStartUp() (totalMsgLeft uint64, err error) {
	var maxSegID, segID uint32
	var segErr error
	files, err := ioutil.ReadDir(db.path)
	if err != nil {
		return
	}

	for _, f := range files {
		if segID, segErr = db.opsLog.getSegIDByFileName(f.Name()); segErr != nil {
			continue
		}
		maxSegID = segID + 1
	}

	for segID := 0; segID < int(maxSegID); segID++ {
		if msgLeft, err := db.compactSegment(true, uint32(segID)); err != nil {
			return 0, err
		} else {
			totalMsgLeft = totalMsgLeft + msgLeft
		}

	}

	return
}

func (db *Db) compactSegment(startUp bool, segID uint32) (msgLeft uint64, err error) {
	var (
		meta *compactionMeta
	)
	if meta, err = db.opsLog.getDataForCompaction(segID); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if meta.full() {
		if _, err = db.sc.compactSegment(segID, meta); err != nil {
			return 0, err
		}
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
