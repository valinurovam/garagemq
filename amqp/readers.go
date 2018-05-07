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

func ReadLong(r io.Reader) (data uint32, err error) {
	err = binary.Read(r, binary.BigEndian, &data)
	return
}

func ReadLonglong(r io.Reader) (data uint64, err error) {
	err = binary.Read(r, binary.BigEndian, &data)
	return
}

var ReadTimestamp = ReadLonglong

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

func ReadTable(r io.Reader) (data *Table, err error) {
	data = &Table{}
	return
}
