package main

import (
	"crypto/sha512"
	"io"
	"log"
	"os"

	"github.com/amoghe/dedup/codec"
	"github.com/pkg/profile"

	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	// DefaultWindowSize is the window over which to compute rolling fingerprint
	DefaultWindowSize = "64"
	// DefaultZeroBits specifies the bitbank which must be 0 to identify a segment boundary
	DefaultZeroBits = "16"
)

var (
	zeroBits = kingpin.Flag("zerobits", "Number of 0 bits to identify segment").
			Default(DefaultZeroBits).
			Uint64()
	windowSize = kingpin.Flag("window", "Fingerprint window size (bytes)").
			Default(DefaultWindowSize).
			Uint64()
	reduplicate = kingpin.Flag("decompress", "Recover original file (redup)").
			Short('d').
			Bool()
	memProfile = kingpin.Flag("memprofile", "Enable memory profiling").
			Bool()
	// outFile = kingpin.Flag("stdout", "Write to stdout, keep original").
	// 	Short('c').
	// 	Bool()
	// inputFile = kingpin.Arg("infile", "File to be deduplicated").
	// 		File()
)

func main() {
	kingpin.Parse()

	if *memProfile {
		defer profile.Start(profile.MemProfile).Stop()
	}

	doRedup := func() {
		r := codec.NewGobReader(os.Stdin, os.Stdout)
		if err := r.Reduplicate(); err != nil {
			log.Fatalln("Failed to redup", err)
		}
	}

	if *reduplicate {
		doRedup()
	} else {
		doDedup()
	}
}

//
// Performs deduplication (compression)
//
func doDedup() {

	// Setup window size
	if *windowSize <= 1 {
		log.Fatalln("Window too small (<=1)")
	}

	// Setup bitmask
	if *zeroBits <= 1 {
		log.Fatalln("Mask size too small (<=1)")
	}
	mask := uint64((1 << *zeroBits) - 1)

	// Setup the Segmenter
	dedup := NewDeduplicator(*windowSize, mask, os.Stdout)

	// Segment the file
	if err := dedup.Do(os.Stdin); err != nil {
		log.Fatalln("Failed to parse file:", err)
	}

	// Print stats (TODO: make this optional)
	_ = dedup.stats.Print(os.Stderr)
}

// Deduplicator performs deduplication of the specified file
type Deduplicator struct {
	writer    codec.SegmentWriter
	segmenter Segmenter
	stats     *ParseStats
}

// NewDeduplicator returns a Deduplicator
func NewDeduplicator(winsz, mask uint64, output io.WriteCloser) *Deduplicator {
	segmenter := Segmenter{
		WindowSize: winsz,
		Mask:       mask,
	}

	d := Deduplicator{
		writer:    codec.NewGobWriter(output),
		segmenter: segmenter,
		stats:     NewParseStats(sha512.New()),
	}

	d.segmenter.SegHandler = &d
	return &d
}

// Do runs the deduplication
func (d *Deduplicator) Do(f *os.File) error {
	err := d.segmenter.SegmentFile(f)
	return err
}

func (d *Deduplicator) Handle(seg []byte) error {
	segHash := d.stats.UpdateStats(seg)
	segStat := d.stats.SegHashes[segHash]
	return d.writer.Write(seg, segStat.SeqNum, segStat.Freq > 1)
}
