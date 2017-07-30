package codec

// SegmentWriter allows various implementations of 'writers' to write segments
// to output streams
type SegmentWriter interface {
	Write(seg []byte, id uint64, seen bool) error
	Close()
}

const (
	// GobMessageRef indicates this is a Ref message
	GobMessageRef = 1
	// GobMessageDef indicates this is a Def message
	GobMessageDef = 2
)

// GobMessage is the message that we write to the output stream.
type GobMessage struct {
	Type     uint16
	RefID    uint64
	DefID    uint64
	DefBytes []byte
}
