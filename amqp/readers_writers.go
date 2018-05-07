package amqp

import (
	"encoding/binary"
	"io"
)

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

func WriteTable(wr io.Writer, data *Table) error {
	// @todo implement WriteTable
	return binary.Write(wr, binary.BigEndian, &data)
}
