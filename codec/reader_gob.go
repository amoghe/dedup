package codec

import (
	"encoding/gob"
	"io"

	"github.com/pkg/errors"
)

// GobReader allows us to read gob encoded streams
type GobReader struct {
	output   io.WriteCloser
	rawInput io.ReadCloser
	gobInput *gob.Decoder
	segMap   map[uint64][]byte
}

func NewGobReader(input io.ReadCloser, output io.WriteCloser) *GobReader {
	return &GobReader{
		output:   output,
		rawInput: input,
		gobInput: gob.NewDecoder(input),
		segMap:   make(map[uint64][]byte),
	}
}

// Reduplicate reads from the input and recreates the original stream
func (r *GobReader) Reduplicate() error {
	for {
		dec := GobMessage{}
		err := r.gobInput.Decode(&dec)
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrapf(err, "Failed to read gob from input stream")
		}

		switch dec.Type {
		case GobMessageDef:
			r.handleSegmentDef(&dec)
		case GobMessageRef:
			r.handleSegmentRef(&dec)
		default:
			return errors.Errorf("Unexpected type in input stream: %d", dec.Type)
		}
	}
	return nil
}

func (r *GobReader) handleSegmentDef(msg *GobMessage) error {
	r.segMap[msg.DefID] = msg.DefBytes
	return nil
}

func (r *GobReader) handleSegmentRef(dec *GobMessage) error {
	bytes, there := r.segMap[dec.RefID]
	if !there {
		return errors.Errorf("Received a REF with a previously unseen ID %d", dec.RefID)
	}
	r.output.Write(bytes)
	return nil
}
