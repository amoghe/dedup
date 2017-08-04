package dedup

import (
	"crypto/sha512"
	"hash"
	"io"

	"github.com/amoghe/dedup/codec"
)

// Deduplicator performs deduplication of the specified file
type Deduplicator struct {
	segmenter *Segmenter
	tracker   *SegmentTracker
	seghasher hash.Hash
}

// NewDeduplicator returns a Deduplicator
func NewDeduplicator(winsz, mask uint64) *Deduplicator {
	d := Deduplicator{
		//writer:    codec.NewGobWriter(output),
		segmenter: &Segmenter{WindowSize: winsz, Mask: mask},
		tracker:   NewSegmentTracker(),
		seghasher: sha512.New(),
	}

	return &d
}

// Do runs the deduplication of the specified input stream
func (d *Deduplicator) Do(input io.ReadCloser, output io.WriteCloser) error {
	writer := codec.NewGobWriter(output)
	defer writer.Close()

	handler := func(seg []byte) error {
		stat := d.tracker.Track(seg, d.seghasher.Sum(seg))
		cmsg := codec.Message{}
		if stat.Freq <= 1 {
			cmsg = codec.Message{Type: codec.MessageDef, DefID: stat.ID, DefBytes: seg}
		} else {
			cmsg = codec.Message{Type: codec.MessageRef, RefID: stat.ID}
		}
		return writer.Write(&cmsg)
	}

	return d.segmenter.SegmentFile(input, handler)
}

// PrintStats prints stats to the given writer
func (d *Deduplicator) PrintStats(out io.Writer) error {
	return d.tracker.PrintStats(out)
}
