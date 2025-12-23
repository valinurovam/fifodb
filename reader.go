package fifodb

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"io"
)

// messageReader reads messages from an io.Reader without allocating.
// It reuses internal buffers for headers.
type messageReader struct {
	headerBuf [8]byte // 4 (size) + 4 (crc)
}

// ReadMessage reads one message from r into msg.
// Returns io.EOF if no more messages.
func (mr *messageReader) ReadMessage(r io.Reader) (size uint32, crc uint32, data []byte, err error) {
	if _, err = io.ReadFull(r, mr.headerBuf[:]); err != nil {
		return 0, 0, nil, err
	}
	size = binary.BigEndian.Uint32(mr.headerBuf[:4])
	crc = binary.BigEndian.Uint32(mr.headerBuf[4:])

	if size == 0 {
		return 0, 0, nil, errors.New("zero-size message")
	}

	data = make([]byte, size)
	if _, err = io.ReadFull(r, data); err != nil {
		return 0, 0, nil, err
	}

	return size, crc, data, nil
}
