package amqp

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestReadFrame_Success(t *testing.T) {
	var frameType byte = 1
	var channelId uint16 = 1
	payload := []byte("some_test_data")

	wr := bytes.NewBuffer(make([]byte, 0))
	// type
	binary.Write(wr, binary.BigEndian, frameType)
	// channelId
	binary.Write(wr, binary.BigEndian, channelId)
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

	if frame.ChannelID != channelId {
		t.Fatalf("Excpected ChannelID %d, actual %d", channelId, frame.ChannelID)
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
	var channelId uint16 = 1
	payload := []byte("some_test_data")

	wr := bytes.NewBuffer(make([]byte, 0))
	// type
	binary.Write(wr, binary.BigEndian, frameType)
	// channelId
	binary.Write(wr, binary.BigEndian, channelId)
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