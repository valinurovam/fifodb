package fifodb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/golang-collections/go-datastructures/bitarray"
	"github.com/pkg/errors"
)

const (
	opWr = iota + 1
	opDel
)

// walController represents controller of WALs
//
// We need walController to handle messages that we should remove or requeue after storage (server) restart
// Each segmentsController segment has its own WAL segment.
type walController struct {
	sync.RWMutex
	path     string
	segments map[uint32]*segmentWAL
	bufSize  uint32

	log Logger
}

// segmentsWAL represents single WAL file for data segment for operations like
//   - write message (push)
//   - remove message (ack/reject)
type segmentWAL struct {
	sync.Mutex
	segID uint32
	fd    *os.File
	wr    *bufio.Writer
	// At first view that 8 bytes looks like overhead, because we really need just 5 bytes for operation
	// 1 to opId and 4 to offset. With 8 byte length we try to avoid data corruption on separate writes inside
	// buffered writer
	entryBuffer [8]byte
}

func (seg *segmentWAL) sync() error {
	if err := seg.wr.Flush(); err != nil {
		return err
	}

	return seg.fd.Sync()
}

func (seg *segmentWAL) close() error {
	if err := seg.sync(); err != nil {
		return err
	}

	return seg.fd.Close()
}

func newWalController(path string, bufSize uint32, log Logger) *walController {
	return &walController{
		path:     path,
		segments: make(map[uint32]*segmentWAL),
		bufSize:  bufSize,
		log:      log,
	}
}

// Write writes write operation into WAL.
func (wal *walController) Write(segID uint32, offset uint32) error {
	return wal.writeOp(opWr, segID, offset)
}

// Delete writes remove operation into WAL.
func (wal *walController) Delete(segID uint32, offset uint32) error {
	return wal.writeOp(opDel, segID, offset)
}

func (wal *walController) writeOp(op byte, segID uint32, offset uint32) error {
	var (
		seg *segmentWAL
		err error
	)
	if seg, err = wal.getSegment(segID); err != nil {
		return err
	}

	seg.Lock()
	seg.entryBuffer[3] = op
	binary.BigEndian.PutUint32(seg.entryBuffer[4:], offset)
	_, err = seg.wr.Write(seg.entryBuffer[:])
	seg.Unlock()

	return err
}

func (wal *walController) getSegment(segID uint32) (seg *segmentWAL, err error) {
	wal.RLock()
	if seg, ok := wal.segments[segID]; ok && seg != nil {
		wal.RUnlock()
		return seg, nil
	}
	wal.RUnlock()

	wal.Lock()
	if seg, err = wal.openWalSegment(segID); err != nil {
		wal.Unlock()
		return nil, err
	}
	wal.segments[segID] = seg
	wal.Unlock()

	return seg, nil
}

func (wal *walController) openWalSegment(segID uint32) (seg *segmentWAL, err error) {
	seg = &segmentWAL{}
	fileName := wal.fileNameBySegID(segID)
	if seg.fd, err = os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600); err != nil {
		return nil, errors.Wrapf(err, "failed to open WAL segment: %s", fileName)
	}

	// @todo 64<<10 can that bufferSize be configurable?
	seg.wr = bufio.NewWriterSize(seg.fd, int(wal.bufSize))
	seg.segID = segID
	return seg, nil
}

func (wal *walController) fileNameBySegID(segID uint32) string {
	return path.Join(wal.path, fmt.Sprintf(walSegmentsPattern, segID))
}

func (wal *walController) getSegIDByFileName(name string) (segID uint32, err error) {
	if _, err = fmt.Sscanf(name, walSegmentsPattern, &segID); err != nil {
		return 0, errors.Wrapf(err, "get segId by file name '%s'", name)
	}
	return
}

func (wal *walController) isCorrectFileName(name string) bool {
	return strings.HasSuffix(name, walSegmentsSuffix)
}

func (wal *walController) release(segID uint32) error {
	return os.Remove(wal.fileNameBySegID(segID))
}

func (wal *walController) createAfterCompaction(offsets []uint32, segID uint32) (err error) {
	for _, offset := range offsets {
		if err = wal.writeOp(opWr, segID, offset); err != nil {
			return err
		}
	}
	logSeg, err := wal.getSegment(segID)
	if err != nil {
		return err
	}
	return logSeg.sync()
}

func (wal *walController) getDataForCompaction(segID uint32) (meta *compactionMeta, err error) {
	// we accumulate only requeue offset, cause if we have empty reqOffsets at the end
	// we can remove wal file
	var (
		segFd  *os.File
		data   [8]byte
		op     byte
		offset uint64
		n      int
	)

	walSegFileName := wal.fileNameBySegID(segID)
	if _, err = os.Stat(walSegFileName); os.IsNotExist(err) {
		return
	}

	if segFd, err = os.OpenFile(walSegFileName, os.O_RDONLY, 0600); err != nil {
		return
	}

	fileStat, err := os.Stat(path.Join(wal.path, fmt.Sprintf(dataSegmentsPattern, segID)))
	if err != nil {
		return nil, err
	}
	fileSize := uint64(fileStat.Size())
	bitArraySize := roundUp(fileSize)

	meta = &compactionMeta{
		delOffsets: bitarray.NewBitArray(bitArraySize),
	}

	defer func() {
		if closeErr := segFd.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	reader := bufio.NewReaderSize(segFd, 4<<20)

	_, err = io.ReadFull(reader, data[:])
	for err != io.EOF || err == nil {
		op = data[3]
		offset = uint64(binary.BigEndian.Uint32(data[4:]))

		switch op {
		case opWr:
			meta.wrCnt++
		case opDel:
			if ok, _ := meta.delOffsets.GetBit(offset); !ok {
				if err = meta.delOffsets.SetBit(offset); err != nil {
					break
				}
				meta.delCnt++
			}
		}
		if n, err = io.ReadFull(reader, data[:]); n != 8 {
			break
		}
	}

	if err != nil && err != io.EOF {
		if err != io.ErrUnexpectedEOF {
			return
		}
	}

	return meta, nil
}

func (wal *walController) sync() error {
	wal.Lock()
	defer wal.Unlock()
	for _, seg := range wal.segments {
		if seg != nil {
			if err := seg.sync(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (wal *walController) syncSeg(segID uint32) error {
	var (
		seg *segmentWAL
		ok  bool
	)
	wal.RLock()
	defer wal.RUnlock()
	if seg, ok = wal.segments[segID]; !ok || seg == nil {
		return fmt.Errorf("segment [%d] has not been opened yet", segID)
	}

	return seg.sync()
}

func (wal *walController) Close() error {
	wal.Lock()
	defer wal.Unlock()
	for idx, seg := range wal.segments {
		if seg != nil {
			if err := seg.close(); err != nil {
				return err
			}

			delete(wal.segments, idx)
		}
	}

	return nil
}

// Clean
func (wal *walController) Clean() error {
	wal.Lock()
	defer wal.Unlock()

	for idx, seg := range wal.segments {
		if seg != nil {
			if err := seg.close(); err != nil {
				return err
			}

			delete(wal.segments, idx)
		}
	}

	files, err := os.ReadDir(wal.path)
	if err != nil {
		return errors.Wrapf(err, "failed to read opslog's directory: %s", wal.path)
	}

	var segID uint32
	for _, f := range files {
		if segID, err = wal.getSegIDByFileName(f.Name()); err != nil {
			continue
		}

		if err := os.Remove(wal.fileNameBySegID(segID)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

type compactionMeta struct {
	wrCnt  uint64
	reqCnt uint64
	delCnt uint64

	delOffsets bitarray.BitArray
}

// Write writes write operation into WAL
func (m *compactionMeta) full() bool {
	return m.wrCnt == m.delCnt
}

// Write writes write operation into WAL
func (m *compactionMeta) needCompactLog() bool {
	return !m.full() && m.reqCnt != 0 || m.delCnt != 0
}
