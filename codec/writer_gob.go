package codec

import (
	"encoding/gob"
	"io"

	"github.com/pkg/errors"
)

// GobWriter implements SegmentWriter. It writes segments that are gob encoded
// to a given output stream
type GobWriter struct {
	output  io.Writer
	encoder *gob.Encoder
}

// NewGobWriter returns a codec.writer capable of emitting golang/gob encoded
// messages to the specified output
func NewGobWriter(output io.Writer) Writer {
	return &GobWriter{
		output:  output,
		encoder: gob.NewEncoder(output),
	}
}

// Write emits the message to the output stream
func (g *GobWriter) Write(msg *Message) error {
	if err := g.encoder.Encode(msg); err != nil {
		return errors.Wrapf(err, "Failed to encode msg: %v", err)
	}
	return nil
}
