package flowfile

import (
	"io"
)

type FlowFile interface {
	GetAttribute(string) (string, bool)
	GetAttributes() map[string]string
	SetAttributes(map[string]string) FlowFile
	GetContent() []byte
	SetContent([]byte) FlowFile
	Serialize(io.Writer) error
	Deserialize(io.Reader) error
}
