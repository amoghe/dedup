package dedup

import (
	"io"
	"log"

	"github.com/amoghe/dedup/codec"
	"github.com/pkg/errors"
)

// Reduplicator performs reduplication of the specified file
type Reduplicator struct {
	tracker map[uint64][]byte
}

// NewReduplicator returns a Reduplicator
func NewReduplicator() *Reduplicator {
	d := Reduplicator{
		tracker: map[uint64][]byte{},
	}
	return &d
}

// Do runs the reduplication writing the output to the output stream
func (r *Reduplicator) Do(input io.Reader, output io.Writer) error {
	reader := codec.NewGobReader(input)

	for {
		msg, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		switch msg.Type {
		case codec.MessageDef:
			r.handleSegmentDef(&msg, output)
		case codec.MessageRef:
			r.handleSegmentRef(&msg, output)
		default:
			return errors.Errorf("Unexpected type in input stream: %d", msg.Type)
		}
		//r.msgsProcessed++
	}
	return nil
}

func (r *Reduplicator) handleSegmentDef(msg *codec.Message, out io.Writer) {
	r.tracker[msg.DefID] = msg.DefBytes
	// receipt of def is implicit ref, so output the bytes
	out.Write(msg.DefBytes)
}

func (r *Reduplicator) handleSegmentRef(msg *codec.Message, out io.Writer) {
	bytes, there := r.tracker[msg.RefID]
	if !there {
		log.Panicln("Got Ref for previously unseed ID:", msg.RefID)
	}
	out.Write(bytes)
}
