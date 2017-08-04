package main

import (
	"io"
	"log"
	"os"

	"github.com/amoghe/dedup"
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
	toStdout = kingpin.Flag("stdout", "Write to stdout").
			Short('c').
			Bool()
	inputFile = kingpin.Arg("infile", "File to be deduplicated").
			File()

	source io.ReadCloser  = os.Stdin
	sink   io.WriteCloser = os.Stdout
)

// main entrypoint
func main() {

	kingpin.Parse()

	if *windowSize <= 1 {
		log.Fatalln("Window too small (<=1)")
	}

	if *zeroBits <= 1 {
		log.Fatalln("Mask size too small (<=1)")
	}

	if *memProfile {
		defer profile.Start(profile.MemProfile).Stop()
	}

	if *reduplicate {
		doReduplication(source, sink)
	} else {
		doDeduplication(source, sink)
	}
}

// set source/sink (input/output) streams
// if input is file, output can be file or stdout
// if input is stdin, output can only be stdout
func setInputOutpuStreams() {
	// trivial case
	if *inputFile == nil {
		source = os.Stdin
		sink = os.Stdout
		return
	}

	source = *inputFile

	// change what sink points to
	if *toStdout == false {
		inFileName := (*inputFile).Name()
		outFileName := inFileName + ".dd"
		// TODO: if decompression, strip the 'dd'

		inStat, err := (*inputFile).Stat()
		if err != nil {
			log.Fatalln("Failed to stat input file:", err)
		}
		out, err := os.OpenFile(outFileName, os.O_CREATE|os.O_RDWR, inStat.Mode())
		if err != nil {
			log.Fatalln("Failed to open output file:", err)
		}
		sink = out
	}
}

// Performs deduplication (compression)
func doDeduplication(in io.ReadCloser, out io.WriteCloser) {
	dedup := dedup.NewDeduplicator(*windowSize, uint64((1<<*zeroBits)-1))
	if err := dedup.Do(in, out); err != nil {
		log.Fatalln("Failed to parse file:", err)
	}
	// Print stats (TODO: make this optional)
	dedup.PrintStats(os.Stderr)
}

// Performs reduplication (decompression)
func doReduplication(in io.ReadCloser, out io.WriteCloser) {
	redup := dedup.NewReduplicator()
	if err := redup.Do(in, out); err != nil {
		log.Fatalln("Failed to redup", err)
	}
	// Print stats (TODO: make this optional)
	// redup.PrintStats()
}
