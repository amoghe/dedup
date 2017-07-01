package main

import (
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log"
	"sync/atomic"

	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
)

// SegmentStat holds stats for a single segment
type SegmentStat struct {
	SeqNum uint64 // First occurrence of this segment in the file
	Length int    // Length of segment
	Freq   int    // How many times this segment occurred in the file
}

// ParseStats holds stats about the parsed file
type ParseStats struct {
	Cutpoints   []uint64               // indices at which we have cutpoints
	SegLengths  []uint64               // lengths of segments (between cutpoints)
	SegHashes   map[string]SegmentStat // map[crypto hash of seg] -> SegmentStat
	BytesParsed uint64                 // number of bytes parsed

	// internal:
	segNum    uint64    // tracks the segment numbers we've issued
	segHasher hash.Hash // used to generate the crypto hash of segments
}

// NewParseStats returns an initialized ParseStats struct
func NewParseStats(hasher hash.Hash) *ParseStats {
	return &ParseStats{
		SegHashes: make(map[string]SegmentStat),
		segNum:    uint64(0),
		segHasher: hasher,
	}
}

// UpdateStats updates the ParseStats with stats for the specified chunk
func (s *ParseStats) UpdateStats(segment []byte) string {

	// Sprint'ing the hash sum causes an allocation that is unnecessary and
	// is completely avoidable.
	//segHash := fmt.Sprintf("%X", s.segHasher.Sum(segment))
	segHash := string(s.segHasher.Sum(segment))
	segStat, there := s.SegHashes[segHash]
	if there {
		segStat.Freq++
	} else {
		//segStat = SegmentStat{SeqNum: s.segNum, Length: len(segment), Freq: 1}
		segStat.Freq = 1
		segStat.Length = len(segment)
		segStat.SeqNum = atomic.AddUint64(&s.segNum, 1)
	}
	s.SegHashes[segHash] = segStat

	// Additional book keeping (TODO: allow this to be disabled)
	s.Cutpoints = append(s.Cutpoints, s.BytesParsed+uint64(len(segment)))
	s.SegLengths = append(s.SegLengths, uint64(len(segment)))
	s.BytesParsed += uint64(len(segment))

	return segHash
}

// Print prints the specified ParseStats on the given output (io.Writer)
//
func (s ParseStats) Print(out io.Writer) error {
	segLens := make([]float64, 0, len(s.SegLengths))
	for _, s := range s.SegLengths {
		segLens = append(segLens, float64(s))
	}
	log.Println("len seglens:", len(segLens))

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

	dupCount := 0
	dupBytes := 0
	lenUnique := uint64(0)
	for _, segStat := range s.SegHashes {
		if segStat.Freq > 1 {
			dupCount += (segStat.Freq - 1)
			dupBytes += (segStat.Length * (segStat.Freq - 1))
		}
		lenUnique += uint64(segStat.Length)
	}

	output := struct {
		NumCutpoints    int
		MeanSegLength   float64
		MedianSegLength float64
		MaxSegLength    float64
		MinSegLength    float64
		DupSegCount     int
		DupBytes        int
		UniqueBytes     uint64
		TotalBytes      uint64
	}{
		NumCutpoints:    len(s.Cutpoints),
		MeanSegLength:   mea,
		MedianSegLength: med,
		MaxSegLength:    max,
		MinSegLength:    min,
		DupSegCount:     dupCount,
		DupBytes:        dupBytes,
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
