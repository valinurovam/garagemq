package amqp

import (
	"encoding/binary"
	"io"
	"bytes"
	"errors"
	"fmt"
)

func ReadFrame(r io.Reader) (*Frame, error) {
	// 7 bytes for type, channel and size
	var header = make([]byte, 7)
	if err := binary.Read(r, binary.BigEndian, header); err != nil {
		return nil, err
	}

	frame := &Frame{}
	headerBuf := bytes.NewBuffer(header)

	frame.Type, _ = ReadOctet(headerBuf)
	frame.ChannelId, _ = ReadShort(headerBuf)

	payloadSize, _ := ReadLong(headerBuf)
	var payload = make([]byte, payloadSize+1)
	if err := binary.Read(r, binary.BigEndian, payload); err != nil {
		return nil, err
	}
	frame.Payload = payload[0:payloadSize]

	// check frame end
	if payload[payloadSize] != FrameEnd {
		return nil, errors.New(
			fmt.Sprintf(
				"The frame-end octet MUST always be the hexadecimal value 'xCE', %x given",
				payload[payloadSize]))
	}

	return frame, nil
}

func WriteFrame(writer io.Writer, frame *Frame) error {
	var rawFrame = make([]byte, 0, 7+len(frame.Payload)+1)
	frameBuffer := bytes.NewBuffer(rawFrame)
	WriteOctet(frameBuffer, frame.Type)
	WriteShort(frameBuffer, frame.ChannelId)
	// size + payload
	WriteLongstr(frameBuffer, frame.Payload)
	WriteOctet(frameBuffer, FrameEnd)

	fmt.Println(frameBuffer.Bytes())
	return binary.Write(writer, binary.BigEndian, frameBuffer.Bytes())
}

func ReadOctet(r io.Reader) (data byte, err error) {
	err = binary.Read(r, binary.BigEndian, &data)
	return
}

func WriteOctet(wr io.Writer, data byte) error {
	return binary.Write(wr, binary.BigEndian, data)
}

func ReadShort(r io.Reader) (data uint16, err error) {
	err = binary.Read(r, binary.BigEndian, &data)
	return
}

func WriteShort(wr io.Writer, data uint16) error {
	return binary.Write(wr, binary.BigEndian, &data)
}

func ReadLong(r io.Reader) (data uint32, err error) {
	err = binary.Read(r, binary.BigEndian, &data)
	return
}

func WriteLong(wr io.Writer, data uint32) error {
	return binary.Write(wr, binary.BigEndian, &data)
}

func ReadLonglong(r io.Reader) (data uint64, err error) {
	err = binary.Read(r, binary.BigEndian, &data)
	return
}

func WriteLonglong(wr io.Writer, data uint64) error {
	return binary.Write(wr, binary.BigEndian, &data)
}

var ReadTimestamp = ReadLonglong
var WriteTimestamp = WriteLonglong

func ReadShortstr(r io.Reader) (data string, err error) {
	var length byte

	length, err = ReadOctet(r)
	if err != nil {
		return "", err
	}

	bytes := make([]byte, length)

	err = binary.Read(r, binary.BigEndian, &bytes)
	if err != nil {
		return "", err
	}
	data = string(bytes)
	return
}

func WriteShortstr(wr io.Writer, data string) error {
	err := binary.Write(wr, binary.BigEndian, byte(len(data)))
	if err != nil {
		return err
	}
	return binary.Write(wr, binary.BigEndian, []byte(data))
}

func ReadLongstr(r io.Reader) (data []byte, err error) {
	var length uint32

	length, err = ReadLong(r)
	if err != nil {
		return nil, err
	}

	data = make([]byte, length)

	err = binary.Read(r, binary.BigEndian, &data)
	if err != nil {
		return nil, err
	}
	return
}

func WriteLongstr(wr io.Writer, data []byte) error {
	err := binary.Write(wr, binary.BigEndian, uint32(len(data)))
	if err != nil {
		return err
	}
	return binary.Write(wr, binary.BigEndian, data)
}

func ReadTable(r io.Reader) (data *Table, err error) {
	data = &Table{}
	// @todo implement ReadTable
	return
}

func WriteTable(writer io.Writer, data *Table) (err error) {
	var buf = bytes.NewBuffer(make([]byte, 0))
	for key, v := range data.Data {
		if err := WriteShortstr(buf, key); err != nil {
			return err
		}
		switch value := v.(type) {
		case string:
			if err = WriteOctet(buf, byte('s')); err == nil {
				err = WriteShortstr(buf, value)
			}
		}
	}
	return WriteLongstr(writer, buf.Bytes())
}
