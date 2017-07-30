package codec

import (
	"encoding/gob"
	"io"

	"github.com/pkg/errors"
)

// GobWriter implements SegmentWriter. It writes segments that are gob encoded
// to a given output stream
type GobWriter struct {
	output  io.WriteCloser
	encoder *gob.Encoder
}

// NewGobWriter returns a writer to write golang/gobs to the specified output
func NewGobWriter(output io.WriteCloser) *GobWriter {
	return &GobWriter{
		output:  output,
		encoder: gob.NewEncoder(output),
	}
}

// Write outputs the segment instruction (ref, def) to the output stream
func (g *GobWriter) Write(seg []byte, id uint64, seen bool) error {

	if !seen {
		err := g.emit(&GobMessage{
			Type:     GobMessageDef,
			DefID:    id,
			DefBytes: seg,
		})
		if err != nil {
			return err
		}
	}

	err := g.emit(&GobMessage{
		Type:  GobMessageRef,
		RefID: id,
	})
	if err != nil {
		return err
	}

	return nil
}

// Close allows GobWriter to satisfy SegmentWriter interface
func (g *GobWriter) Close() {
	// TODO: w.output.Flush()
	g.output.Close()
}

// actually emit the message to the output stream
func (g *GobWriter) emit(msg *GobMessage) error {
	if err := g.encoder.Encode(msg); err != nil {
		return errors.Wrapf(err, "Failed to encode msg: %v", err)
	}
	return nil
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
