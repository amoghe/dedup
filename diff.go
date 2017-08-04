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
func NewDiffer(winsz, mask uint64, output io.WriteCloser) *Differ {
	return &Differ{
		dedup:     NewDeduplicator(winsz, mask, devnull{}),
		segmenter: Segmenter{WindowSize: winsz, Mask: mask},
		seghasher: sha512.New(),
	}
}

// MakePatch writes a "patch" file to the specified WriteCloser
func (d *Differ) MakePatch(old, new io.ReadCloser, out io.WriteCloser) error {
	defer old.Close()
	defer new.Close()
	defer out.Close()

	// First parse old file and build up the segment state
	if err := d.dedup.Do(old); err != nil {
		return errors.Wrapf(err, "Failed to parse old file")
	}

	// Now attach the output and parse the new file (with the state we've built)
	d.dedup.writer = codec.NewGobWriter(out)
	if err := d.dedup.Do(new); err != nil {
		return errors.Wrapf(err, "Failed to segment new file")
	}

	return nil
}

// ApplyPatch applies the patch file to the 'old' and writes the result to 'new'
func (d *Differ) ApplyPatch(old, patch io.ReadCloser, new io.WriteCloser) error {

	defer old.Close()
	defer patch.Close()
	defer new.Close()

	r, w := io.Pipe()
	redup := NewReduplicator(r)
	d.dedup.writer = codec.NewGobWriter(w)
	// First parse the 'old' file and build up segment state (in the redup)
	if err := d.dedup.Do(old); err != nil {
		return errors.Wrapf(err, "Failed to parse old file")
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		redup.Do(devnull{})
		wg.Done()
	}()

	wg.Wait()
	log.Println("finished parsing original file")

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
