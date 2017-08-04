package dedup

import (
	"bufio"
	"io"

	"github.com/kch42/buzhash"
	"github.com/pkg/errors"
)

// SegmentHandler is something capable of processing the segments handed to it
type SegmentHandler interface {
	Handle([]byte) error
}

// Segmenter segments a file or stream
type Segmenter struct {
	WindowSize       uint64
	Mask             uint64
	MaxSegmentLength uint64
	SegHandler       SegmentHandler
}

// SegmentFile does the actual work of segmenting the specified file as per the
// params configure in the Segmenter struct
func (s Segmenter) SegmentFile(file io.ReadCloser) error {

	if s.SegHandler == nil {
		return errors.Errorf("No segment handler specified")
	}

	if s.Mask == 0 {
		return errors.Errorf("Invalid mask specified (0)")
	}

	if s.WindowSize <= 0 {
		return errors.Errorf("Invalid windows size specified")
	}

	if s.MaxSegmentLength <= 0 {
		s.MaxSegmentLength = (s.Mask + 1) * 8 // arbitrary :-)
	}

	var (
		reader     = bufio.NewReader(file)
		roller     = buzhash.NewBuzHash(uint32(s.WindowSize))
		curSegment = make([]byte, 0, s.MaxSegmentLength)
		bytesRead  = uint64(0)
		minSegLen  = s.WindowSize
	)

	// Loop over input stream one byte at a time
	for {
		b, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		curSegment = append(curSegment, b)
		sum := roller.HashByte(b)
		bytesRead++

		// dont accept segments smaller than minSegLen
		if uint64(len(curSegment)) < minSegLen {
			continue
		}

		// If this is a cutpoint, process the curSegment
		if (uint64(sum) & s.Mask) == 0 {
			if err := s.SegHandler.Handle(curSegment); err != nil {
				return err
			}
			curSegment = curSegment[:0] // reset the curSegment accumulator
		}
		if uint64(len(curSegment)) >= s.MaxSegmentLength {
			if err := s.SegHandler.Handle(curSegment); err != nil {
				return err
			}
			curSegment = curSegment[:0] // reset the curSegment accumulator
		}
	}

	// Deal with any remaining bytes in curSegment
	if err := s.SegHandler.Handle(curSegment); err != nil {
		return err
	}

	return nil
}
