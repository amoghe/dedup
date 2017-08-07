package codec

import (
	"encoding/gob"
	"io"
)

// GobReader allows us to read gob encoded streams
type GobReader struct {
	input         io.Reader
	decoder       *gob.Decoder
	msgsProcessed uint64
}

// NewGobReader returns a codec.Reader capable of reading golang/gob encoded
// messages from input streams
func NewGobReader(input io.Reader) Reader {
	return &GobReader{
		input:   input,
		decoder: gob.NewDecoder(input),
	}
}

// Read returns the next message available in the input stream
func (r *GobReader) Read() (Message, error) {
	dec := Message{}
	err := r.decoder.Decode(&dec)
	return dec, err
}
