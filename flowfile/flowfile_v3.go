package flowfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

var magicHeaderV3 []byte = []byte{'N', 'i', 'F', 'i', 'F', 'F', '3'}
var badHeaderV3 error = errors.New("Not a NiFi v3 FlowFile")

type FlowFileV3 struct {
	attributes map[string]string
	content    []byte
}

func NewFlowFileV3() *FlowFileV3 {
	return &FlowFileV3{
		attributes: make(map[string]string),
		content:    make([]byte, 0),
	}
}

func (ff *FlowFileV3) Deserialize(in io.Reader) error {
	if err := validateHeader(in); err != nil {
		return err
	}
	if attributes, err := readAttributes(in); err != nil {
		return err
	} else {
		ff.attributes = attributes
	}
	if content, err := readContent(in); err != nil {
		return err
	} else {
		ff.content = content
	}
	return nil
}

func (ff FlowFileV3) GetAttribute(key string) (string, bool) {
	value, ok := ff.attributes[key]
	return value, ok
}

func (ff FlowFileV3) GetAttributes() map[string]string {
	return ff.attributes
}

func (ff *FlowFileV3) SetAttributes(attributes map[string]string) FlowFile {
	ff.attributes = attributes
	return ff
}

func (ff *FlowFileV3) GetContent() []byte {
	return ff.content
}

func (ff *FlowFileV3) SetContent(content []byte) FlowFile {
	ff.content = content
	return ff
}

func (ff FlowFileV3) Serialize(out io.Writer) error {
	if err := writeHeader(out); err != nil {
		return err
	}
	if err := writeAttributes(out, ff.attributes); err != nil {
		return err
	}
	if err := writeContent(out, ff.content); err != nil {
		return err
	}
	return nil
}

func validateHeader(in io.Reader) error {
	header := make([]byte, len(magicHeaderV3))
	if bytesRead, err := in.Read(header); err != nil {
		return err
	} else if bytesRead != len(magicHeaderV3) {
		return io.EOF
	}
	if !bytes.Equal(header, magicHeaderV3) {
		return badHeaderV3
	}
	return nil
}

func writeHeader(out io.Writer) error {
	if _, err := out.Write(magicHeaderV3); err != nil {
		return err
	}
	return nil
}

func readAttributes(in io.Reader) (map[string]string, error) {
	numAttributes, err := readLength(in)
	if err != nil {
		return nil, err
	}
	attributes := make(map[string]string)
	for i := 0; i < numAttributes; i++ {
		key, err := readString(in)
		if err != nil {
			return nil, err
		}
		value, err := readString(in)
		if err != nil {
			return nil, err
		}
		attributes[key] = value
	}
	return attributes, nil
}

func writeAttributes(out io.Writer, attributes map[string]string) error {
	if err := writeLength(out, len(attributes)); err != nil {
		return err
	}
	for key, value := range attributes {
		if err := writeString(out, key); err != nil {
			return err
		}
		if err := writeString(out, value); err != nil {
			return err
		}
	}
	return nil
}

func readContent(in io.Reader) ([]byte, error) {
	var length uint64
	if err := binary.Read(in, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	val := make([]byte, length)
	if bytesRead, err := in.Read(val); err != nil {
		return nil, err
	} else if uint64(bytesRead) != length {
		return nil, io.EOF
	}
	return val, nil
}

func writeContent(out io.Writer, content []byte) error {
	if err := binary.Write(out, binary.BigEndian, uint64(len(content))); err != nil {
		return err
	}
	if _, err := out.Write(content); err != nil {
		return err
	}
	return nil
}

func readLength(in io.Reader) (int, error) {
	var length int = 0
	var len16 uint16
	if err := binary.Read(in, binary.BigEndian, &len16); err != nil {
		return -1, err
	}
	if len16 < math.MaxUint16 {
		length = int(len16)
	} else {
		var len32 uint32
		if err := binary.Read(in, binary.BigEndian, &len32); err != nil {
			return -1, err
		}
		length = int(len32)
	}
	return length, nil
}

func writeLength(out io.Writer, length int) error {
	if length < math.MaxUint16 {
		if err := binary.Write(out, binary.BigEndian, uint16(length)); err != nil {
			return err
		}
	} else {
		if err := binary.Write(out, binary.BigEndian, uint16(0xffff)); err != nil {
			return err
		}
		if err := binary.Write(out, binary.BigEndian, uint32(length)); err != nil {
			return err
		}
	}
	return nil
}

func readString(in io.Reader) (string, error) {
	numBytes, err := readLength(in)
	if err != nil {
		return "", err
	}
	val := make([]byte, numBytes)
	if bytesRead, err := in.Read(val); err != nil {
		return "", err
	} else if bytesRead != numBytes {
		return "", io.EOF
	}
	return string(val), nil
}

func writeString(out io.Writer, val string) error {
	if err := writeLength(out, len(val)); err != nil {
		return err
	}
	if _, err := out.Write([]byte(val)); err != nil {
		return err
	}
	return nil
}
