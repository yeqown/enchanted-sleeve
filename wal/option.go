package wal

type Config struct {
	// root // represents the root directory of the WAL, e.g. /var/lib/myapp/wal.
	// It contains the following files:
	// 0000000000.wal (segment 0)
	// 0000000000.wal.meta (meta file for segment 0)
	// 0000000001.wal (segment 1)
	// 0000000001.wal.meta (meta file for segment 1)
	// 0000000002.wal (segment 2)
	// 0000000002.wal.meta (meta file for segment 2)
	// ...
	Root string

	MaxSegmentSize int64 // represents the maximum size of a segment file in bytes, 0 means unlimited
	MaxSegments    int   // represents the maximum number of segments to keep, 0 means unlimited

	Logger __loggerSpec // represents the __loggerSpec to use for logging
}

type OptionWAL interface {
	apply(*Config)
}

type optionFunc func(*Config)

func (f optionFunc) apply(o *Config) {
	f(o)
}

func DefaultConfig() *Config {
	return &Config{
		Root:           "./wal",
		MaxSegmentSize: 20 * 1024 * 1024, // 20MB
		MaxSegments:    5,                // 5 segments
	}
}

func WithRoot(root string) OptionWAL {
	return optionFunc(func(o *Config) {
		o.Root = root
	})
}

func WithMaxSegmentSize(maxSegmentSize int64) OptionWAL {
	return optionFunc(func(o *Config) {
		o.MaxSegmentSize = maxSegmentSize
	})
}

func WithMaxSegments(maxSegments int) OptionWAL {
	return optionFunc(func(o *Config) {
		o.MaxSegments = maxSegments
	})
}
