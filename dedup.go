package main

import (
	"crypto/sha512"
	"hash"
	"io"

	"github.com/amoghe/dedup/codec"
)

// Deduplicator performs deduplication of the specified file
type Deduplicator struct {
	writer    codec.SegmentWriter
	segmenter Segmenter
	seghasher hash.Hash
	stats     *ParseStats
}

// NewDeduplicator returns a Deduplicator
func NewDeduplicator(winsz, mask uint64, output io.WriteCloser) *Deduplicator {
	d := Deduplicator{
		writer:    codec.NewGobWriter(output),
		segmenter: Segmenter{WindowSize: winsz, Mask: mask},
		seghasher: sha512.New(),
		stats:     NewParseStats(),
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
	segHash := d.stats.UpdateStats(seg, d.seghasher.Sum(seg))
	segStat := d.stats.SegHashes[segHash]
	return d.writer.Write(seg, segStat.SeqNum, segStat.Freq > 1)
}
