package flowfile

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func createByteBuffer() *bytes.Buffer {
	val := make([]byte, 0)
	val = append(val, magicHeaderV3...)
	val = append(val, []byte{0, 2}...)
	val = append(val, []byte{0, 4}...)
	val = append(val, []byte("key1")...)
	val = append(val, []byte{0, 6}...)
	val = append(val, []byte("value1")...)
	val = append(val, []byte{0, 4}...)
	val = append(val, []byte("key2")...)
	val = append(val, []byte{0, 6}...)
	val = append(val, []byte("value2")...)
	val = append(val, []byte{0, 0, 0, 0, 0, 0, 0, 7}...)
	val = append(val, []byte("content")...)
	return bytes.NewBuffer(val)
}

func createFlowFileV3() FlowFile {
	attributes := map[string]string{"key1": "value1", "key2": "value2"}
	content := []byte("content")
	return NewFlowFileV3().SetAttributes(attributes).SetContent(content)
}

func TestFlowfileV3SerializeDeserialize(t *testing.T) {
	ffBytes1 := createByteBuffer()
	ff1 := NewFlowFileV3()
	err1 := ff1.Deserialize(ffBytes1)

	ff2 := createFlowFileV3()
	ffBytes2 := new(bytes.Buffer)
	err2 := ff2.Serialize(ffBytes2)
	ff3 := NewFlowFileV3()
	err3 := ff3.Deserialize(ffBytes2)

	ffs := []FlowFile{ff1, ff2, ff3}

	assert.Nil(t, err1)
	assert.Nil(t, err2)
	assert.Nil(t, err3)

	assert.Equal(t, ffs[0], ffs[1])
	assert.Equal(t, ffs[1], ffs[2])
}
