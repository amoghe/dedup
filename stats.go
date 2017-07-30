package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/codahale/hdrhistogram"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
)

// SegmentStat holds stats for a single segment
type SegmentStat struct {
	Length int // Length of segment
	Freq   int // How many times this segment occurred in the file
}

// ParseStats holds stats about the parsed file
type ParseStats struct {
	SegHashes   map[string]SegmentStat // map[crypto hash of seg] -> SegmentStat
	BytesParsed uint64                 // number of bytes parsed
}

// NewParseStats returns an initialized ParseStats struct
func NewParseStats() *ParseStats {
	return &ParseStats{
		SegHashes: make(map[string]SegmentStat),
	}
}

// UpdateStats updates the ParseStats with stats for the specified chunk
func (s *ParseStats) UpdateStats(segment, seghash []byte) {

	// Sprint'ing the hash sum causes an allocation that is unnecessary and
	// is completely avoidable.
	//segHash := fmt.Sprintf("%X", s.segHasher.Sum(segment))
	segHash := string(seghash)
	segStat, there := s.SegHashes[segHash]
	if there {
		segStat.Freq++
	} else {
		segStat.Freq = 1
		segStat.Length = len(segment)
	}
	s.SegHashes[segHash] = segStat
	s.BytesParsed += uint64(len(segment))
	return
}

// Print prints the specified ParseStats on the given output (io.Writer)
//
func (s ParseStats) Print(out io.Writer) error {

	segLens := make([]float64, 0, len(s.SegHashes))
	for _, stat := range s.SegHashes {
		for i := 0; i < stat.Freq; i++ {
			segLens = append(segLens, float64(stat.Length))
		}
	}

	med, err := stats.Median(segLens)
	if err != nil {
		return errors.Wrapf(err, "Failed compute median")
	}
	max, err := stats.Max(segLens)
	if err != nil {
		return errors.Wrapf(err, "Failed to compute max")
	}
	min, err := stats.Min(segLens)
	if err != nil {
		return errors.Wrapf(err, "Failed to compute min")
	}
	mea, err := stats.Mean(segLens)
	if err != nil {
		return errors.Wrapf(err, "Failed to compute mean")
	}

	mostFreq := 0
	dupCount := 0
	dupBytes := 0
	lenUnique := uint64(0)
	for _, segStat := range s.SegHashes {
		if segStat.Freq > 1 {
			dupCount += (segStat.Freq - 1)
			dupBytes += (segStat.Length * (segStat.Freq - 1))
		}
		if segStat.Freq > mostFreq {
			mostFreq = segStat.Freq
		}
		lenUnique += uint64(segStat.Length)
	}

	output := struct {
		NumSegments     int
		MeanSegLength   float64
		MedianSegLength float64
		MaxSegLength    float64
		MinSegLength    float64
		DupSegCount     int
		DupBytes        int
		MaxSegFreq      int
		UniqueBytes     uint64
		TotalBytes      uint64
	}{
		NumSegments:     len(segLens),
		MeanSegLength:   mea,
		MedianSegLength: med,
		MaxSegLength:    max,
		MinSegLength:    min,
		DupSegCount:     dupCount,
		DupBytes:        dupBytes,
		MaxSegFreq:      mostFreq,
		UniqueBytes:     lenUnique,
		TotalBytes:      s.BytesParsed,
	}

	marshalled, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal stats into JSON output")
	}
	fmt.Fprintln(out, string(marshalled))
	return nil
}

// PrintSegLengths prints segment lengths to the specified output separated by
// the specified separator
func (s ParseStats) PrintSegLengths(out io.Writer, sep string) error {

	lenStrings := []string{}
	for _, stat := range s.SegHashes {
		for i := 0; i < stat.Freq; i++ {
			lenStrings = append(lenStrings, strconv.Itoa(int(stat.Length)))
		}
	}

	// Join our string slice.
	result := strings.Join(lenStrings, sep)
	_, err := fmt.Fprint(out, result)
	return err
}

// PrintMostFrequentSegStats prints 'n' "hottest" segments (SegmentStat)
func (s ParseStats) PrintMostFrequentSegStats(out io.Writer, n int) error {
	ss := []SegmentStat{}
	for _, s := range s.SegHashes {
		ss = append(ss, s)
	}

	sort.Sort(sort.Reverse(bySegFreq(ss)))

	for i := 0; i < n; i++ {
		marshalled, err := json.MarshalIndent(ss[i], "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(out, string(marshalled))
	}

	return nil
}

// PrintSegLengthHistogram prints histogram (bars in csv) to out
func (s ParseStats) PrintSegLengthHistogram(out io.Writer) error {
	ss := []SegmentStat{}
	for _, s := range s.SegHashes {
		ss = append(ss, s)
	}
	sort.Sort(sort.Reverse(bySegFreq(ss)))

	hist := hdrhistogram.New(int64(ss[0].Length), int64(ss[len(ss)-1].Length), 1)
	for _, s := range ss {
		hist.RecordValue(int64(s.Length))
	}
	for _, bar := range hist.Distribution() {
		fmt.Fprintf(out, "%s\n", bar.String())
	}
	return nil
}

// SegmentStat sorted by segment frequencies
type bySegFreq []SegmentStat

func (a bySegFreq) Len() int           { return len(a) }
func (a bySegFreq) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bySegFreq) Less(i, j int) bool { return a[i].Freq < a[j].Freq }
