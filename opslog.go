package fifodb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/golang-collections/go-datastructures/bitarray"
)

const (
	opWr = iota + 1
	opDel
	opReq
)

// opsLog represents log of operations of operations like
// 	- write message (push)
//	- delete message (ack/reject)
//	- requeue message (nack)
//
// We need that opsLog to handle messages that we should delete or requeue after storage (server) restart
// Each segmentsController segment has it's own WAL segment
type opsLog struct {
	sync.RWMutex
	path          string
	segments      map[uint32]*logSegment
}

type logSegment struct {
	sync.Mutex
	segID uint32
	fd    *os.File
	wr    *bufio.Writer
	// At first view that 8 bytes looks like overhead, cause we really need just 5 bytes for operation
	// 1 to opId and 4 to offset. With 8 byte length we try to avoid data corruption on separate writes inside
	// buffered writer
	entryData [8]byte
}

func (seg *logSegment) sync() error {
	if err := seg.wr.Flush(); err != nil {
		return err
	}
	return seg.fd.Sync()
}

func (seg *logSegment) close() error {
	if err := seg.sync(); err != nil {
		return err
	}
	return seg.fd.Close()
}

func newOperationsLog(path string) *opsLog {
	wal := &opsLog{
		path:     path,
		segments: make(map[uint32]*logSegment),
	}

	return wal
}

// Write writes write operation into WAL
func (log *opsLog) Write(segId uint32, offset uint32) error {
	return log.writeOp(opWr, segId, offset)
}

// Delete writes delete operation into WAL
func (log *opsLog) Delete(segId uint32, offset uint32) error {
	return log.writeOp(opDel, segId, offset)
}

// Requeue writes requeue operation into WAL
func (log *opsLog) Requeue(segId uint32, offset uint32) error {
	return log.writeOp(opReq, segId, offset)
}

func (log *opsLog) writeOp(op byte, segId uint32, offset uint32) error {
	var (
		seg *logSegment
		err error
	)
	if seg, err = log.getSegment(segId); err != nil {
		return err
	}

	seg.Lock()
	seg.entryData[3] = op
	binary.BigEndian.PutUint32(seg.entryData[4:], offset)
	_, err = seg.wr.Write(seg.entryData[:])
	seg.Unlock()

	return err
}

func (log *opsLog) getSegment(segID uint32) (seg *logSegment, err error) {
	log.RLock()
	if seg, ok := log.segments[segID]; ok && seg != nil {
		log.RUnlock()
		return seg, nil
	}
	log.RUnlock()

	log.Lock()
	if seg, err = log.openWalSegment(segID); err != nil {
		log.Unlock()
		return nil, err
	}
	log.segments[segID] = seg
	log.Unlock()

	return seg, nil
}

func (log *opsLog) openWalSegment(segID uint32) (seg *logSegment, err error) {
	seg = &logSegment{}
	if seg.fd, err = os.OpenFile(log.fileNameBySegID(segID), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600); err != nil {
		return nil, err
	}

	// @todo 64<<10 can that bufferSize be configurable?
	seg.wr = bufio.NewWriterSize(seg.fd, 64<<10)
	seg.segID = segID
	return seg, nil
}

func (log *opsLog) fileNameBySegID(segID uint32) string {
	return path.Join(log.path, fmt.Sprintf(opsLogSegmentsPattern, segID))
}

func (log *opsLog) getSegIDByFileName(name string) (segID uint32, err error) {
	if _, err = fmt.Sscanf(name, opsLogSegmentsPattern, &segID); err != nil {
		return
	}
	return
}

func (log *opsLog) release(segID uint32) error {
	return os.Remove(log.fileNameBySegID(segID))
}

func (log *opsLog) createAfterCompaction(offsets []uint32, segID uint32) (err error) {
	for _, offset := range offsets {
		if err = log.writeOp(opWr, segID, offset); err != nil {
			return err
		}
	}
	logSeg, err := log.getSegment(segID)
	if err != nil {
		return err
	}
	return logSeg.sync()
}

func (log *opsLog) getDataForCompaction(segID uint32) (meta *compactionMeta, err error) {
	// we accumulate only requeue offset, cause if we have empty reqOffsets at the end
	// we can delete log file
	var (
		segFd     *os.File
		data      [8]byte
		op        byte
		offset    uint64
		n         int
	)

	segFileName := log.fileNameBySegID(segID)
	if _, err = os.Stat(segFileName); os.IsNotExist(err) {
		return
	}

	if segFd, err = os.OpenFile(segFileName, os.O_RDONLY, 0600); err != nil {
		return
	}


	fileStat, err := os.Stat(path.Join(log.path, fmt.Sprintf(dataSegmentsPattern, segID)))
	fileSize := uint64(fileStat.Size())
	bitArraySize := roundUp(fileSize)


	meta = &compactionMeta{
		reqOffsets: bitarray.NewBitArray(bitArraySize),
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
		case opReq:
			if ok, _ := meta.reqOffsets.GetBit(offset); !ok {
				if err = meta.reqOffsets.SetBit(offset); err != nil {
					break
				}
				meta.reqCnt++
			}
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

func (log *opsLog) sync() error {
	log.Lock()
	defer log.Unlock()
	for _, seg := range log.segments {
		if seg != nil {
			if err := seg.sync(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (log *opsLog) syncSeg(segID uint32) error {
	var (
		seg *logSegment
		ok  bool
	)
	log.RLock()
	defer log.RUnlock()
	if seg, ok = log.segments[segID]; !ok || seg == nil {
		return fmt.Errorf("segment [%d] has not been opened yet", segID)
	}

	return seg.sync()
}

func (log *opsLog) Close() error {
	log.Lock()
	defer log.Unlock()
	for _, seg := range log.segments {
		if seg != nil {
			if err := seg.close(); err != nil {
				return err
			}
		}
	}

	return nil
}

type compactionMeta struct {
	wrCnt  uint64
	reqCnt uint64
	delCnt uint64

	delOffsets bitarray.BitArray
	reqOffsets bitarray.BitArray

	needCompact bool
}

// Write writes write operation into WAL
func (m *compactionMeta) full() bool {
	return m.wrCnt == m.delCnt
}

// Write writes write operation into WAL
func (m *compactionMeta) needCompactLog() bool {
	return !m.full() && m.reqCnt != 0 || m.delCnt != 0
}
