package main

import (
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
	toStdout = kingpin.Flag("stdout", "Write to stdout").
			Short('c').
			Bool()
	// TODO: Add positional arg, then deal appropriately with toStdout
	// inputFile = kingpin.Arg("infile", "File to be deduplicated").
	// 		File()
	makeSig = kingpin.Flag("signature", "Make a signature file").
		Short('s').
		Bool()

	inFile  io.ReadCloser
	outFile io.WriteCloser
)

func main() {
	parseArgsOrDie()
	if *memProfile {
		defer profile.Start(profile.MemProfile).Stop()
	}
	if *makeSig {
		doSignatures(os.Stdin, os.Stdout)
	} else if *reduplicate {
		doRedup(os.Stdin, outFile)
	} else {
		doDedup(os.Stdin, outFile)
	}
}

// Parse and validate command line arguments, fail on bad args
func parseArgsOrDie() {
	kingpin.Parse()

	if *windowSize <= 1 {
		log.Fatalln("Window too small (<=1)")
	}

	if *zeroBits <= 1 {
		log.Fatalln("Mask size too small (<=1)")
	}
}

// Performs deduplication (compression)
func doDedup(in io.ReadCloser, out io.WriteCloser) {
	dedup := NewDeduplicator(*windowSize, uint64((1<<*zeroBits)-1), out)
	if err := dedup.Do(in); err != nil {
		log.Fatalln("Failed to parse file:", err)
	}
	// Print stats (TODO: make this optional)
	_ = dedup.stats.Print(os.Stderr)
}

// Performs reduplication (decompression)
func doRedup(in io.ReadCloser, out io.WriteCloser) {
	if err := codec.NewGobReader(in, out).Reduplicate(); err != nil {
		log.Fatalln("Failed to redup", err)
	}
}

// Makes a signature file
func doSignatures(in io.ReadCloser, out io.WriteCloser) {
	sm := NewSigMaker(*windowSize, uint64((1<<*zeroBits)-1), out)
	if err := sm.Do(in); err != nil {
		log.Fatalln("Failed to generate signatures:", err)
	}
}
