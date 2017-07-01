package main

import (
	"crypto/sha512"
	"io"

	"github.com/amoghe/dedup/codec"
)

type SigMaker struct {
	segmenter Segmenter
	stats     *ParseStats
	writer    codec.SignatureWriter
}

func NewSigMaker(winsz, mask uint64, output io.WriteCloser) *SigMaker {
	sm := SigMaker{
		stats:     NewParseStats(sha512.New()),
		segmenter: Segmenter{WindowSize: *windowSize, Mask: mask},
		writer:    codec.NewGobWriter(output),
	}
	sm.segmenter.SegHandler = &sm
	return &sm
}

// Do generates the signatures
func (d *SigMaker) Do(f io.ReadCloser) error {
	err := d.segmenter.SegmentFile(f)
	return err
}

// Handle allows the Deduplicator to be a SegmentHandler (satisfies interface)
func (s *SigMaker) Handle(segment []byte) error {
	segHash := s.stats.UpdateStats(segment)
	return s.writer.WriteSignature(segHash)
}
