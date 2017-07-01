package main

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
	WindowSize uint64
	Mask       uint64
	SegHandler SegmentHandler
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

	var (
		reader           = bufio.NewReader(file)
		roller           = buzhash.NewBuzHash(uint32(s.WindowSize))
		maxSegmentLength = int(2048)
		curSegment       = make([]byte, 0, maxSegmentLength)
		bytesRead        = uint64(0) // uint64(f.WindowSize)
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

		// Process at least WindowSize bytes since last cutpoint
		if uint64(len(curSegment)) <= s.WindowSize {
			continue
		}

		// If this is a cutpoint, process the curSegment
		if (len(curSegment) >= maxSegmentLength) || ((uint64(sum) & s.Mask) == 0) {
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
