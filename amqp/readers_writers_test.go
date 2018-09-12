package amqp

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
	"time"
)

func TestReadFrame_Success(t *testing.T) {
	var frameType byte = 1
	var channelID uint16 = 1
	payload := []byte("some_test_data")

	wr := bytes.NewBuffer(make([]byte, 0))
	// type
	binary.Write(wr, binary.BigEndian, frameType)
	// channelID
	binary.Write(wr, binary.BigEndian, channelID)
	// size
	binary.Write(wr, binary.BigEndian, uint32(len(payload)))
	// payload
	binary.Write(wr, binary.BigEndian, payload)
	// end
	binary.Write(wr, binary.BigEndian, byte(FrameEnd))

	frame, err := ReadFrame(wr)
	if err != nil {
		t.Fatal(err)
	}

	if frame.ChannelID != channelID {
		t.Fatalf("Excpected ChannelID %d, actual %d", channelID, frame.ChannelID)
	}

	if frame.Type != frameType {
		t.Fatalf("Excpected Type %d, actual %d", frameType, frame.Type)
	}

	if !bytes.Equal(frame.Payload, payload) {
		t.Fatal("Payload not equal test data")
	}
}

func TestReadFrame_Failed_WrongFrameEnd(t *testing.T) {
	var frameType byte = 1
	var channelID uint16 = 1
	payload := []byte("some_test_data")

	wr := bytes.NewBuffer(make([]byte, 0))
	// type
	binary.Write(wr, binary.BigEndian, frameType)
	// channelID
	binary.Write(wr, binary.BigEndian, channelID)
	// size
	binary.Write(wr, binary.BigEndian, uint32(len(payload)))
	// payload
	binary.Write(wr, binary.BigEndian, payload)
	// end
	binary.Write(wr, binary.BigEndian, byte('t'))

	_, err := ReadFrame(wr)
	if err == nil {
		t.Fatal("Expected error about frame end")
	}
}

func TestWriteFrame(t *testing.T) {
	frame := &Frame{
		Type:       1,
		ChannelID:  2,
		Payload:    []byte("some_test_data"),
		CloseAfter: false,
	}
	wr := bytes.NewBuffer(make([]byte, 0))
	err := WriteFrame(wr, frame)
	if err != nil {
		t.Fatal(err)
	}

	readFrame, err := ReadFrame(wr)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(frame, readFrame) {
		t.Fatal("Read and write frames not equal")
	}
}

func TestReadOctet(t *testing.T) {
	var data byte = 10
	r := bytes.NewReader([]byte{data})
	rData, err := ReadOctet(r)
	if err != nil {
		t.Fatal(err)
	}

	if rData != data {
		t.Fatalf("Expected %d, actual %d", data, rData)
	}
}

func TestWriteOctet(t *testing.T) {
	var data byte = 10
	wr := bytes.NewBuffer(make([]byte, 0))
	err := WriteOctet(wr, data)
	if err != nil {
		t.Fatal(err)
	}

	if rData, _ := ReadOctet(wr); rData != data {
		t.Fatalf("Expected %d, actual %d", data, rData)
	}
}

func TestReadShort(t *testing.T) {
	var data uint16 = 10
	wr := bytes.NewBuffer(make([]byte, 0))
	binary.Write(wr, binary.BigEndian, data)
	rData, err := ReadShort(wr)
	if err != nil {
		t.Fatal(err)
	}

	if rData != data {
		t.Fatalf("Expected %d, actual %d", data, rData)
	}
}

func TestWriteShort(t *testing.T) {
	var data uint16 = 10
	wr := bytes.NewBuffer(make([]byte, 0))
	err := WriteShort(wr, data)
	if err != nil {
		t.Fatal(err)
	}

	if rData, _ := ReadShort(wr); rData != data {
		t.Fatalf("Expected %d, actual %d", data, rData)
	}
}

func TestReadLong(t *testing.T) {
	var data uint32 = 10
	wr := bytes.NewBuffer(make([]byte, 0))
	binary.Write(wr, binary.BigEndian, data)
	rData, err := ReadLong(wr)
	if err != nil {
		t.Fatal(err)
	}

	if rData != data {
		t.Fatalf("Expected %d, actual %d", data, rData)
	}
}

func TestWriteLong(t *testing.T) {
	var data uint32 = 10
	wr := bytes.NewBuffer(make([]byte, 0))
	err := WriteLong(wr, data)
	if err != nil {
		t.Fatal(err)
	}

	if rData, _ := ReadLong(wr); rData != data {
		t.Fatalf("Expected %d, actual %d", data, rData)
	}
}

func TestReadLonglong(t *testing.T) {
	var data uint64 = 10
	wr := bytes.NewBuffer(make([]byte, 0))
	binary.Write(wr, binary.BigEndian, data)
	rData, err := ReadLonglong(wr)
	if err != nil {
		t.Fatal(err)
	}

	if rData != data {
		t.Fatalf("Expected %d, actual %d", data, rData)
	}
}

func TestWriteLonglong(t *testing.T) {
	var data uint64 = 10
	wr := bytes.NewBuffer(make([]byte, 0))
	err := WriteLonglong(wr, data)
	if err != nil {
		t.Fatal(err)
	}

	if rData, _ := ReadLonglong(wr); rData != data {
		t.Fatalf("Expected %d, actual %d", data, rData)
	}
}

func TestReadShortstr(t *testing.T) {
	var data = "someteststring"
	wr := bytes.NewBuffer(make([]byte, 0))
	binary.Write(wr, binary.BigEndian, byte(len(data)))
	binary.Write(wr, binary.BigEndian, []byte(data))
	rData, err := ReadShortstr(wr)
	if err != nil {
		t.Fatal(err)
	}

	if rData != data {
		t.Fatalf("Expected '%s', actual '%s'", data, rData)
	}
}

func TestWriteShortstr(t *testing.T) {
	var data = "someteststring"
	wr := bytes.NewBuffer(make([]byte, 0))
	err := WriteShortstr(wr, data)
	if err != nil {
		t.Fatal(err)
	}

	if rData, _ := ReadShortstr(wr); rData != data {
		t.Fatalf("Expected '%s', actual '%s'", data, rData)
	}
}

func TestReadLongstr(t *testing.T) {
	var data = []byte("someteststring")
	wr := bytes.NewBuffer(make([]byte, 0))
	binary.Write(wr, binary.BigEndian, uint32(len(data)))
	binary.Write(wr, binary.BigEndian, data)
	rData, err := ReadLongstr(wr)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(rData, data) {
		t.Fatalf("Expected '%v', actual '%v'", data, rData)
	}
}

func TestWriteLongstr(t *testing.T) {
	var data = []byte("someteststring")
	wr := bytes.NewBuffer(make([]byte, 0))
	err := WriteLongstr(wr, data)
	if err != nil {
		t.Fatal(err)
	}

	if rData, _ := ReadLongstr(wr); !bytes.Equal(rData, data) {
		t.Fatalf("Expected '%v', actual '%v'", data, rData)
	}
}

/*
Rabbitmq table fields

't' bool			boolean
'b' int8			short-short-int
's'	int16			short-int
'I' int32			long-int
'l' int64			long-long-int
'f' float			float
'd' double			double
'D' Decimal			decimal-value
'S'	[]byte			long-string
'T' time.Time		timestamp
'F' Table			field-table
'V' nil				no-field
'x' []interface{} 	field-array
*/
func TestReadWriteTable(t *testing.T) {

	table := Table{}
	table["bool_true"] = true
	table["bool_false"] = false
	table["int8"] = int8(16)
	table["uint8"] = uint8(16)
	table["int16"] = int16(16)
	table["uint16"] = uint16(16)
	table["int32"] = int32(32)
	table["uint32"] = uint32(32)
	table["int64"] = int64(64)
	table["uint64"] = uint64(64)
	table["float32"] = float32(32.32)
	table["float64"] = float64(64.64)
	table["decimal"] = Decimal{Scale: 1, Value: 10}
	table["byte_array"] = []byte{'a', 'r', 'r', 'a', 'y'}
	table["string"] = "string"
	table["time"] = time.Now()
	table["array_of_data"] = []interface{}{int8(16), Decimal{Scale: 1, Value: 10}, "string"}
	table["nil"] = nil
	table["table"] = Table{}

	wr := bytes.NewBuffer(make([]byte, 0))
	err := WriteTable(wr, &table, ProtoRabbit)
	if err != nil {
		t.Fatal(err)
	}

	err = WriteTable(wr, &table, Proto091)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ReadTable(wr, ProtoRabbit)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ReadTable(wr, Proto091)
	if err != nil {
		t.Fatal(err)
	}
}
