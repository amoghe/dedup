package main

import (
	"crypto/sha512"
	"hash"
	"io"
	"sync/atomic"

	"github.com/amoghe/dedup/codec"
)

// Deduplicator performs deduplication of the specified file
type Deduplicator struct {
	segmenter  Segmenter
	seghasher  hash.Hash
	writer     codec.SegmentWriter
	stats      *ParseStats
	segmentNum uint64 // used to issue unique seq numbers
}

// NewDeduplicator returns a Deduplicator
func NewDeduplicator(winsz, mask uint64, output io.WriteCloser) *Deduplicator {
	d := Deduplicator{
		writer:     codec.NewGobWriter(output),
		segmenter:  Segmenter{WindowSize: winsz, Mask: mask},
		seghasher:  sha512.New(),
		stats:      NewParseStats(),
		segmentNum: uint64(0),
	}
	d.segmenter.SegHandler = &d
	return &d
}

// Do runs the deduplication
func (d *Deduplicator) Do(f io.ReadCloser) error {
	err := d.segmenter.SegmentFile(f)
	return err
}

// Handle allows the Deduplicator to be a SegmentHandler (satisfies interface)
func (d *Deduplicator) Handle(seg []byte) error {

	segSig := d.seghasher.Sum(seg)
	segNum := atomic.AddUint64(&d.segmentNum, 1)
	segNew := true

	d.stats.UpdateStats(seg, segSig)
	if s, _ := d.stats.SegHashes[string(segSig)]; s.Freq > 1 {
		segNew = false
	}
	return d.writer.Write(seg, segNum, !segNew)
}
