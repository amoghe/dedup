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
	inputFile = kingpin.Arg("infile", "File to be deduplicated").
			File()
	makeSig = kingpin.Flag("signature", "Make a signature file").
		Short('s').
		Bool()

	source io.ReadCloser  = os.Stdin
	sink   io.WriteCloser = os.Stdout
)

func main() {
	parseArgsOrDie()
	if *memProfile {
		defer profile.Start(profile.MemProfile).Stop()
	}
	if *makeSig {
		doSignatures(os.Stdin, os.Stdout)
	} else if *reduplicate {
		doRedup(os.Stdin, os.Stdout)
	} else {
		doDedup(source, sink)
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

	// change what source points to (if specified)
	if *inputFile != nil {
		source = *inputFile
	}

	// change what sink points to
	if *inputFile != nil && *toStdout == false {
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
func doDedup(in io.ReadCloser, out io.WriteCloser) {
	dedup := NewDeduplicator(*windowSize, uint64((1<<*zeroBits)-1), out)
	if err := dedup.Do(in); err != nil {
		log.Fatalln("Failed to parse file:", err)
	}
	// Print stats (TODO: make this optional)
	dedup.stats.Print(os.Stderr)
	dedup.stats.PrintMostFrequentSegStats(os.Stderr, 10)
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
