package fifodb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

// meta represents current read or write file index and offset inside
type meta struct {
	segID  uint32
	offset uint32
}

// segmentsController is a FIFO disk storage
// It is just abstraction and don't know about entities that it handle
type segmentsController struct {
	// filename base filesPattern
	path         string
	filesPattern string

	// current read and write sc
	readFd  *os.File
	writeFd *os.File

	reader *bufio.Reader
	writer *bufio.Writer

	readLock  sync.Mutex
	writeLock sync.Mutex

	metaRead  *meta
	metaWrite *meta

	writeBufferSize    uint32
	readBufferSize     uint32
	maxBytesPerSegment uint32

	fsync           bool
	writeDataLength [4]byte
	readDataLength  [4]byte

	crc32Table  *crc32.Table
	writeCrcBuf [4]byte
	readCrcBuf  [4]byte
}

func newSegmentsController(opt Options) *segmentsController {

	return &segmentsController{
		path:               opt.Path,
		filesPattern:       opt.Path + dataSegmentsPattern,
		writeBufferSize:    opt.WriteBufferSize,
		readBufferSize:     opt.ReadBufferSize,
		fsync:              opt.Fsync,
		maxBytesPerSegment: opt.MaxBytesPerSegment,
		metaRead:           &meta{},
		metaWrite:          &meta{},
		crc32Table:         crc32.MakeTable(crc32.Castagnoli),
	}
}

// load loads data
//
// First, we read directory for existing data-files and customize read & write meta-data
// Then just open files for read and write with metadata properties
// Write file always opens in append-only mode with O_CREATE flag to create file if not exists
// Also we start sync ioLoop if needed
func (sc *segmentsController) load() error {
	var err error
	var segID, minSegID, maxSegID uint32
	files, err := ioutil.ReadDir(sc.path)
	if err != nil {
		return err
	}

	// find closest files to read and write
	for i, f := range files {
		if segID, err = sc.getSegIDByName(f.Name()); err != nil {
			continue
		}

		if i == 0 || segID < minSegID {
			minSegID = segID
		}

		// last segment will be current writer, so track segID and offset
		maxSegID = segID
		sc.metaWrite.offset = uint32(f.Size())
	}

	sc.metaWrite.segID = maxSegID
	sc.metaRead.segID = minSegID

	if err = sc.openWriter(sc.metaWrite.segID); err != nil {
		return err
	}

	if err = sc.openReader(sc.metaRead.segID); err != nil {
		return err
	}

	sc.writer = bufio.NewWriterSize(sc.writeFd, int(sc.writeBufferSize))
	sc.reader = bufio.NewReaderSize(sc.readFd, int(sc.readBufferSize))

	return nil
}

func (sc *segmentsController) openReader(segID uint32) (err error) {
	fileName := sc.fileNameBySegID(segID)
	if sc.readFd, err = os.OpenFile(fileName, os.O_RDONLY, 0600); err != nil {
		return errors.Wrapf(err, "open read segment failed: %s", fileName)
	}

	return nil
}

func (sc *segmentsController) openWriter(segID uint32) (err error) {
	fileName := sc.fileNameBySegID(segID)
	if sc.writeFd, err = os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600); err != nil {
		return errors.Wrapf(err, "open write segment failed: %s", fileName)
	}

	return nil
}

// sync fully flush all buffered data and
// commits the current contents of the file to stable storage.
// that is not thread-safe
func (sc *segmentsController) sync() error {
	if err := sc.writer.Flush(); err != nil {
		return errors.Wrap(err, "writer flush failed")
	}
	if err := sc.writeFd.Sync(); err != nil {
		return errors.Wrap(err, "sync segment failed")
	}

	return nil
}

func (sc *segmentsController) setReadPos(segID uint32, offset uint32) error {
	sc.readLock.Lock()
	defer sc.readLock.Unlock()

	var (
		err         error
		readChanged bool
	)

	if sc.metaRead.segID != segID {
		sc.metaRead.segID = segID
		if err = sc.openReader(sc.metaRead.segID); err != nil {
			return err
		}
		readChanged = true
	}

	if sc.metaRead.offset != offset {
		sc.metaRead.offset = offset
		if _, err = sc.readFd.Seek(int64(sc.metaRead.offset), io.SeekStart); err != nil {
			return err
		}
		readChanged = true

	}
	if readChanged {
		sc.reader.Reset(sc.readFd)
	}

	return nil
}

func (sc *segmentsController) read() (data []byte, segID uint32, offset uint32, err error) {
	sc.readLock.Lock()
	data, segID, offset, err = sc.readOne()
	sc.readLock.Unlock()
	return
}

func (sc *segmentsController) write(data []byte) (segID uint32, offset uint32, err error) {
	sc.writeLock.Lock()
	segID, offset, err = sc.writeOne(data)
	sc.writeLock.Unlock()
	return
}

func (sc *segmentsController) fileNameBySegID(segID uint32) string {
	return path.Join(sc.path, fmt.Sprintf(dataSegmentsPattern, segID))
}

func (sc *segmentsController) getSegIDByName(name string) (uint32, error) {
	var segID uint32
	if _, err := fmt.Sscanf(name, dataSegmentsPattern, &segID); err != nil {
		return 0, errors.Wrap(err, "scanning segment pattern failed")
	}
	return segID, nil
}

func (sc *segmentsController) nonEOFOnly(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}

func (sc *segmentsController) flushWriter() (err error) {
	sc.writeLock.Lock()
	if err = sc.writer.Flush(); err != nil {
		sc.writeLock.Unlock()
		return errors.Wrap(err, "writer flush failed")
	}
	sc.writeLock.Unlock()
	return nil
}

// readOne tries to read one entry from disk
// if take an EOF error check writer and flush it if needed or go to next read file
// if end offset will be over than maxBytesPerSegment go to next read file too
func (sc *segmentsController) readOne() (data []byte, segID uint32, offset uint32, err error) {
	var (
		size uint32
		// to once goto startRead
		once bool
	)

startRead:
	if _, err := io.ReadFull(sc.reader, sc.readDataLength[:]); err != nil {
		if err == io.EOF {
			// if it happens when writer buffered but not already flushed?
			// lets check and try to flush it
			if !once && sc.metaWrite.segID == sc.metaRead.segID {
				if err = sc.flushWriter(); err != nil {
					return nil, 0, 0, sc.nonEOFOnly(err)
				}
				once = true

				goto startRead
			}

			// check for existing next file, cause after compaction size of data files
			// less than maxBytesPerSegment

			if err = sc.openNextReadSegment(); err != nil {
				// next segment does not exists or any error happens
				if os.IsNotExist(err) {
					err = nil
				}
				return nil, 0, 0, sc.nonEOFOnly(err)
			}

			// next file exists, try to read again
			goto startRead
		}

		return nil, 0, 0, sc.nonEOFOnly(err)
	}

	size = binary.BigEndian.Uint32(sc.readDataLength[:])
	data = make([]byte, size)

	if _, err := io.ReadFull(sc.reader, data); err != nil {
		return nil, 0, 0, sc.nonEOFOnly(err)
	}
	if _, err := io.ReadFull(sc.reader, sc.readCrcBuf[:]); err != nil {
		return nil, 0, 0, sc.nonEOFOnly(err)
	}

	checkSum := binary.BigEndian.Uint32(sc.readCrcBuf[:])
	expectedSum := crc32.Checksum(data, sc.crc32Table)

	if checkSum != expectedSum {
		return nil, 0, 0, errors.Wrapf(
			err,
			"check crc32 failed, expected %d, actual %d", expectedSum, checkSum,
		)
	}

	segID = sc.metaRead.segID
	offset = sc.metaRead.offset

	sc.metaRead.offset = sc.metaRead.offset + uint32(size) + 4 + 4

	if err := sc.mayBeNextReadSegment(); err != nil {
		return nil, 0, 0, sc.nonEOFOnly(err)
	}

	return
}

func (sc *segmentsController) mayBeNextReadSegment() error {
	if sc.metaRead.offset > sc.maxBytesPerSegment {
		if err := sc.openNextReadSegment(); os.IsNotExist(err) {
			return nil
		}
	}

	return nil
}

func (sc *segmentsController) openNextReadSegment() error {
	nextFileName := sc.fileNameBySegID(sc.metaRead.segID + 1)
	if _, err := os.Stat(nextFileName); os.IsNotExist(err) {
		return err
	}

	// ok, next file exists, close current and process next
	if err := sc.readFd.Close(); err != nil {
		return err
	}

	var err error
	sc.metaRead.segID++
	if err = sc.openReader(sc.metaRead.segID); err != nil {
		return err
	}

	sc.reader.Reset(sc.readFd)
	sc.metaRead.offset = 0

	return nil
}

func (sc *segmentsController) writeOne(data []byte) (segID uint32, offset uint32, err error) {
	length := uint32(len(data))
	dataLength := int(length) + 4

	if dataLength > sc.writer.Available() {
		if err := sc.writer.Flush(); err != nil {
			return 0, 0, errors.Wrap(err, "Unable to flush buffered data")
		}
	}

	binary.BigEndian.PutUint32(sc.writeDataLength[:], length)
	if _, err = sc.writer.Write(sc.writeDataLength[:]); err != nil {
		return 0, 0, errors.Wrap(err, "Unable to write length")
	}

	if _, err = sc.writer.Write(data); err != nil {
		return 0, 0, errors.Wrap(err, "Unable to write data")
	}

	binary.BigEndian.PutUint32(sc.writeCrcBuf[:], crc32.Checksum(data, sc.crc32Table))

	if _, err = sc.writer.Write(sc.writeCrcBuf[:]); err != nil {
		return 0, 0, errors.Wrap(err, "Unable to write checksum")
	}

	segID = sc.metaWrite.segID
	offset = sc.metaWrite.offset

	sc.metaWrite.offset = sc.metaWrite.offset + uint32(length) + 4 + 4

	if err = sc.mayBeNextWriteSegment(); err != nil {
		return 0, 0, err
	}

	return
}

func (sc *segmentsController) mayBeNextWriteSegment() error {
	if sc.metaWrite.offset > sc.maxBytesPerSegment {
		// Flush any buffered data and Sync for strong consistency
		if err := sc.sync(); err != nil {
			return err
		}
		if err := sc.writeFd.Close(); err != nil {
			return err
		}

		// Initialize new segment
		var err error
		sc.metaWrite.segID++
		if err = sc.openWriter(sc.metaWrite.segID); err != nil {
			return err
		}

		sc.writer.Reset(sc.writeFd)
		sc.metaWrite.offset = 0
	}
	return nil
}

func (sc *segmentsController) compactSegment(segID uint32, meta *compactionMeta) (newOffsets []uint32, err error) {
	var (
		fd, fdTmp *os.File
	)
	fileName := sc.fileNameBySegID(segID)
	fileNameTmp := fileName + ".tmp"
	if fd, err = os.OpenFile(fileName, os.O_RDONLY, 0600); err != nil {
		return newOffsets, err
	}
	defer func() {
		if closeErr := fd.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	if fdTmp, err = os.OpenFile(fileNameTmp, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600); err != nil {
		return newOffsets, err
	}

	reader := bufio.NewReaderSize(fd, int(sc.readBufferSize))
	writer := bufio.NewWriterSize(fdTmp, int(sc.writeBufferSize))

	var (
		size      uint32
		sizeBytes [4]byte
		newOffset uint32
		readerPos uint32
	)

	for err != io.EOF || err == nil {
		_, err = io.ReadFull(reader, sizeBytes[:])
		if err != nil {
			break
		}
		size = binary.BigEndian.Uint32(sizeBytes[:])

		if ok, _ := meta.delOffsets.GetBit(uint64(readerPos)); ok {
			// discard data + crc32
			if _, err = reader.Discard(int(size) + 4); err != nil {
				break
			}
			readerPos += 4 + size + 4
			continue
		}

		if _, err = writer.Write(sizeBytes[:]); err != nil {
			break
		}
		// copy data + crc32
		if _, err = io.CopyN(writer, reader, int64(size)+4); err != nil {
			break
		}
		newOffsets = append(newOffsets, newOffset)
		newOffset = newOffset + 4 + size + 4
		readerPos += 4 + size + 4
	}

	if err != nil && err != io.EOF {
		return nil, err
	}

	if err = writer.Flush(); err != nil {
		return newOffsets, err
	}

	if err = fdTmp.Sync(); err != nil {
		return nil, err
	}

	if err = fdTmp.Close(); err != nil {
		return nil, err
	}

	return newOffsets, os.Rename(fileNameTmp, fileName)
}

// Delete deletes all data
func (sc *segmentsController) Delete() error {
	sc.writeLock.Lock()
	sc.readLock.Lock()
	defer sc.writeLock.Unlock()
	defer sc.readLock.Unlock()

	if err := sc.close(); err != nil {
		return err
	}

	max := sc.metaRead.segID
	if sc.metaWrite.segID > sc.metaRead.segID {
		max = sc.metaWrite.segID
	}

	for i := 0; i <= int(max); i++ {
		fileName := sc.fileNameBySegID(uint32(i))
		err := os.Remove(fileName)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

// Close properly closes files with fsync data
func (sc *segmentsController) Close() (err error) {
	sc.writeLock.Lock()
	sc.readLock.Lock()
	defer sc.writeLock.Unlock()
	defer sc.readLock.Unlock()

	return sc.close()
}

func (sc *segmentsController) close() (err error) {
	// Flush any buffered data and Sync for strong consistency
	if err = sc.sync(); err != nil {
		return err
	}

	if err = sc.readFd.Close(); err != nil {
		return err
	}

	if err = sc.writeFd.Close(); err != nil {
		return err
	}

	sc.writeFd = nil
	sc.readFd = nil

	return nil
}
