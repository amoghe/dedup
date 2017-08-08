package main

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/amoghe/dedup"
	"github.com/pkg/profile"

	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	// DefaultWindowSize is the window over which to compute rolling fingerprint
	DefaultWindowSize = "64"
	// DefaultZeroBits is the bitbank which must be 0 to identify a segment boundary
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
	inputFile = kingpin.Arg("infile", "File to be {de|re}duplicated").
			File()
)

func main() {
	kingpin.Parse()

	if *windowSize <= 1 {
		log.Fatalln("Window too small (<=1)")
	}

	if *zeroBits <= 1 {
		log.Fatalln("Mask size too small (<=1)")
	}

	source, err := getInputStream()
	if err != nil {
		log.Fatalln("Failed to setup input stream:", err)
	}

	sink, err := getOutputStream()
	if err != nil {
		log.Fatalln("Failed to setup input stream:", err)
	}

	defer source.Close()
	defer sink.Close()
	if *memProfile {
		defer profile.Start(profile.MemProfile).Stop()
	}

	if *reduplicate {
		doReduplication(source, sink)
	} else {
		doDeduplication(source, sink)
	}
}

func getInputStream() (io.ReadCloser, error) {
	if *inputFile == nil {
		return os.Stdin, nil
	}
	return *inputFile, nil
}

func getOutputStream() (io.WriteCloser, error) {
	if *inputFile == nil || *toStdout == true {
		return os.Stdout, nil
	}

	var (
		inFileName   = (*inputFile).Name()
		dedupFileExt = ".dd"
		outFileName  = inFileName + dedupFileExt
	)
	if *reduplicate == true {
		if filepath.Ext(inFileName) != dedupFileExt {
			return nil, errors.New("Input has unknown ext, can't infer output name")
		}
		outFileName = strings.TrimSuffix(inFileName, dedupFileExt)
	}

	inStat, err := (*inputFile).Stat()
	if err != nil {
		return nil, err
	}
	return os.OpenFile(outFileName, os.O_CREATE|os.O_RDWR, inStat.Mode())
}

func doDeduplication(in io.Reader, out io.Writer) {
	dedup := dedup.NewDeduplicator(*windowSize, uint64((1<<*zeroBits)-1))
	if err := dedup.Do(in, out); err != nil {
		log.Fatalln("Failed to deduplicate:", err)
	}
	// Print stats (TODO: make this optional)
	dedup.PrintStats(os.Stderr)
}

func doReduplication(in io.Reader, out io.Writer) {
	redup := dedup.NewReduplicator()
	if err := redup.Do(in, out); err != nil {
		log.Fatalln("Failed to reduplicate:", err)
	}
	// Print stats (TODO: make this optional)
	// redup.PrintStats()
}
