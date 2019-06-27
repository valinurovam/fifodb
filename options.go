package fifodb

// Options are params for creating DB object.
type Options struct {
	// Directory to store the data. Would be created if not exists.
	Path string

	// Size of bytes to keep in memory before persisting to avoid much write syscalls.
	WriteBufferSize uint32

	// Size of bytes buffered from file to avoid much read syscalls.
	ReadBufferSize uint32

	// Maximum size of each segment file.
	MaxBytesPerSegment uint32

	// Fsync each write
	Fsync bool
}

// DefaultOptions contains options that should work for most applications
var DefaultOptions = Options{
	WriteBufferSize:    128 << 10,
	ReadBufferSize:     128 << 10,
	MaxBytesPerSegment: 128 << 20,
	Fsync:              false,
}
