package dedup

import (
	"crypto/sha512"
	"hash"
	"io"
	"log"
	"sync"

	"github.com/amoghe/dedup/codec"
	"github.com/pkg/errors"
)

// Differ performs diff computation (and resuscitation)
type Differ struct {
	dedup     *Deduplicator
	seghasher hash.Hash
	segmenter Segmenter

	newSegmentNum uint64 // from where we can start issuing new segment IDs
}

type devnull struct{}

func (d devnull) Write(p []byte) (n int, err error) { return len(p), nil }
func (d devnull) Close() error                      { return nil }

// NewDiffer returns a Differ
func NewDiffer(winsz, mask uint64) *Differ {
	return &Differ{
		dedup:     NewDeduplicator(winsz, mask),
		segmenter: Segmenter{WindowSize: winsz, Mask: mask},
		seghasher: sha512.New(),
	}
}

// MakePatch writes a "patch" file (betweem "old" and "new") to the specified
// output WriteCloser
func (d *Differ) MakePatch(old, new io.Reader, out io.Writer) error {

	// First parse old file and build up the segment state
	if err := d.dedup.Do(old, devnull{}); err != nil {
		return errors.Wrapf(err, "Failed to parse old file")
	}

	// Now parse the new file (with the state we've built)
	if err := d.dedup.Do(new, out); err != nil {
		return errors.Wrapf(err, "Failed to segment new file")
	}

	return nil
}

// ApplyPatch applies the patch file to the 'old' and writes the result to 'new'
func (d *Differ) ApplyPatch(old, patch io.Reader, new io.Writer) error {

	r, w := io.Pipe()
	redup := NewReduplicator()
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		redup.Do(r, devnull{})
		wg.Done()
	}()

	// First parse the 'old' file and build up segment state (in the redup)
	if err := d.dedup.Do(old, w); err != nil {
		return errors.Wrapf(err, "Failed to parse old file")
	}
	w.Close() // close the dummy writer
	wg.Wait() // wait for dummy redup to be done

	// Next parse the 'patch' file and recreate 'new' using the messages
	cpatch := codec.NewGobReader(patch)

	handleDef := func(msg *codec.Message) {
		redup.tracker[msg.DefID] = msg.DefBytes
		// receipt of def is implicit ref, so output the bytes
		new.Write(msg.DefBytes)
	}
	handleRef := func(msg *codec.Message) {
		if b, ok := redup.tracker[msg.RefID]; ok {
			new.Write(b)
		} else {
			log.Panicln("Previously unseen Ref", msg.RefID)
		}
	}
	for {
		msg, err := cpatch.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		switch msg.Type {
		case codec.MessageDef:
			handleDef(&msg)
		case codec.MessageRef:
			handleRef(&msg)
		default:
			return errors.Errorf("Unexpected type in input stream: %d", msg.Type)
		}

	}
	return nil
}
