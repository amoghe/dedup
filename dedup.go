package dedup

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
	writer    codec.Writer
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

// Do runs the deduplication of the specified input stream
func (d *Deduplicator) Do(input io.ReadCloser) error {
	defer d.writer.Close()
	return d.segmenter.SegmentFile(input)
}

// Handle allows the Deduplicator to be a SegmentHandler (satisfies interface)
func (d *Deduplicator) Handle(seg []byte) error {
	segStat := d.tracker.Track(seg, d.seghasher.Sum(seg))

	var msg codec.Message
	if segStat.Freq <= 1 {
		msg = codec.Message{
			Type:     codec.MessageDef,
			DefID:    segStat.ID,
			DefBytes: seg,
		}
	} else {
		msg = codec.Message{
			Type:  codec.MessageRef,
			RefID: segStat.ID,
		}
	}

	return d.writer.Write(&msg)
}

// PrintStats prints stats to the given writer
func (d *Deduplicator) PrintStats(out io.Writer) error {
	return d.tracker.PrintStats(out)
}
