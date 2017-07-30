package main

import (
	"crypto/sha512"
	"hash"
	"io"

	"github.com/amoghe/dedup/codec"
)

// Deduplicator performs deduplication of the specified file
type Deduplicator struct {
	segmenter Segmenter
	seghasher hash.Hash
	writer    codec.SegmentWriter
	tracker   *SegmentTracker
}

// NewDeduplicator returns a Deduplicator
func NewDeduplicator(winsz, mask uint64, output io.WriteCloser) *Deduplicator {
	d := Deduplicator{
		writer:    codec.NewGobWriter(output),
		segmenter: Segmenter{WindowSize: winsz, Mask: mask},
		seghasher: sha512.New(),
		tracker:   NewSegmentTracker(),
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
	segStat := d.tracker.Track(seg, d.seghasher.Sum(seg))
	return d.writer.Write(seg, segStat.ID, segStat.Freq > 1)
}

// MessagesForSegment returns one or more messages we emit (to the wire) for the
// segment specified (presumably just encountered)
func MessagesForSegment(seg []byte, segNum uint64, new bool) []WireMessage {
	ret := []WireMessage{}

	if new {
		ret = append(ret, WireMessage{
			Type:     WireMessageDef,
			DefID:    segNum,
			DefBytes: seg,
		})
	}

	ret = append(ret, WireMessage{
		Type:  WireMessageRef,
		RefID: segNum,
	})

	return ret
}
