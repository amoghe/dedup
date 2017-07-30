package main

const (
	// WireMessageRef indicates this is a Ref message
	WireMessageRef = 1
	// WireMessageDef indicates this is a Def message
	WireMessageDef = 2
)

// WireMessage is the message that we write to the output stream (wire).
type WireMessage struct {
	Type     uint16
	RefID    uint64
	DefID    uint64
	DefBytes []byte
}
