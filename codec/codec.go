package codec

// Writer interface allows callers to write the protocol messages to a sink
type Writer interface {
	Write(*Message) error
}

// Reader interface allows callers to read a stream of protocol messages
type Reader interface {
	Read() (Message, error)
}

const (
	// MessageRef indicates this is a Ref message
	MessageRef = 1
	// MessageDef indicates this is a Def message
	MessageDef = 2
)

// Message is the message that we write to the output stream
type Message struct {
	Type     uint16
	RefID    uint64
	DefID    uint64
	DefBytes []byte
}
