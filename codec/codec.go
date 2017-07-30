package codec

// SegmentWriter allows various implementations of 'writers' to write segments
// to output streams
type SegmentWriter interface {
	Write(seg []byte, id uint64, seen bool) error
	Close()
}
