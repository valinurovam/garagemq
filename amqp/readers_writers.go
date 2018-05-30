package amqp

import (
	"encoding/binary"
	"io"
	"bytes"
	"errors"
	"fmt"
	"time"
)

const (
	Proto091    = "amqp-0-9-1"
	ProtoRabbit = "amqp-rabbit"
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

func ReadTimestamp(r io.Reader) (data time.Time, err error) {
	var seconds uint64
	if seconds, err = ReadLonglong(r); err != nil {
		return
	}
	return time.Unix(int64(seconds), 0), nil
}

func WriteTimestamp(wr io.Writer, data time.Time) error {
	return binary.Write(wr, binary.BigEndian, uint64(data.Unix()))
}

func ReadShortstr(r io.Reader) (data string, err error) {
	var length byte

	length, err = ReadOctet(r)
	if err != nil {
		return "", err
	}

	strBytes := make([]byte, length)

	err = binary.Read(r, binary.BigEndian, &strBytes)
	if err != nil {
		return "", err
	}
	data = string(strBytes)
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

func ReadTable(r io.Reader, protoVersion string) (data *Table, err error) {
	tmpData := Table{}
	tableData, err := ReadLongstr(r)
	if err != nil {
		return nil, err
	}

	tableReader := bytes.NewReader(tableData)
	for tableReader.Len() > 0 {
		var key string
		var value interface{}
		if key, err = ReadShortstr(tableReader); err != nil {
			return nil, errors.New("Unable to read key from table: " + err.Error())
		}

		if value, err = readV(tableReader, protoVersion); err != nil {
			return nil, errors.New("Unable to read value from table: " + err.Error())
		}

		tmpData[key] = value
	}

	return &tmpData, nil
}

func readV(r io.Reader, protoVersion string) (data interface{}, err error) {
	switch protoVersion {
	case Proto091:
		return readValue091(r)
	case ProtoRabbit:
		return readValueRabbit(r)
	}

	return nil, errors.New(fmt.Sprintf("Unknown proto version [%s]", protoVersion))
}

/*
Standard amqp-0-9-1 table fields

't' bool			boolean
'b' int8			short-short-int
'B' uint8			short-short-uint
'U' int16			short-int
'u' uint16			short-uint
'I' int32			long-int
'i' uint32			long-uint
'L' int64			long-long-int
'l' uint64			long-long-uint
'f' float			float
'd' double			double
'D' Decimal			decimal-value
's' string			short-string
'S'	[]byte			long-string
'A' []interface{} 	field-array
'T' time.Time		timestamp
'F' Table			field-table
'V' nil				no-field
*/
func readValue091(r io.Reader) (data interface{}, err error) {
	vType, err := ReadOctet(r)
	if err != nil {
		return nil, err
	}

	switch vType {
	case 't':
		rData, err := ReadOctet(r)
		if err != nil {
			return nil, err
		}
		data = rData != 0
	case 'b':
		var rData int8
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'B':
		var rData uint8
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'U':
		var rData int16
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'u':
		var rData uint16
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'I':
		var rData int32
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'i':
		var rData uint32
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'L':
		var rData int64
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'l':
		var rData uint64
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'f':
		var rData float32
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'd':
		var rData float64
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'D':
		var rData = Decimal{0, 0}

		if err := binary.Read(r, binary.BigEndian, &rData.Scale); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &rData.Value); err != nil {
			return nil, err
		}
		return rData, nil
	case 's':
		var rData string
		if rData, err = ReadShortstr(r); err == nil {
			return nil, err
		}

		return rData, nil
	case 'S':
		var rData []byte
		if rData, err = ReadLongstr(r); err == nil {
			return nil, err
		}

		return rData, nil
	case 'T':
		var rData time.Time
		if rData, err = ReadTimestamp(r); err == nil {
			return nil, err
		}

		return rData, nil
	case 'A':
		var rData []interface{}
		if rData, err = readArray(r, Proto091); err == nil {
			return nil, err
		}
		return rData, nil
	case 'F':
		var rData *Table
		if rData, err = ReadTable(r, Proto091); err == nil {
			return nil, err
		}
		return rData, nil
	case 'V':
		return nil, nil
	}

	return nil, errors.New(fmt.Sprintf("Unsupported type by %s protocol", Proto091))
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
func readValueRabbit(r io.Reader) (data interface{}, err error) {
	vType, err := ReadOctet(r)
	if err != nil {
		return nil, err
	}

	switch vType {
	case 't':
		rData, err := ReadOctet(r)
		if err != nil {
			return nil, err
		}
		return rData != 0, nil
	case 'b':
		var rData int8
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 's':
		var rData int16
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'I':
		var rData int32
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'l':
		var rData int64
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'f':
		var rData float32
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'd':
		var rData float64
		if err := binary.Read(r, binary.BigEndian, &rData); err != nil {
			return nil, err
		}
		return rData, nil
	case 'D':
		var rData = Decimal{0, 0}

		if err := binary.Read(r, binary.BigEndian, &rData.Scale); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &rData.Value); err != nil {
			return nil, err
		}
		return rData, nil
	case 'S':
		var rData []byte
		if rData, err = ReadLongstr(r); err != nil {
			return nil, err
		}

		return string(rData), nil
	case 'T':
		var rData time.Time
		if rData, err = ReadTimestamp(r); err != nil {
			return nil, err
		}

		return rData, nil
	case 'x':
		var rData []interface{}
		if rData, err = readArray(r, ProtoRabbit); err != nil {
			return nil, err
		}
		return rData, nil
	case 'F':
		var rData *Table
		if rData, err = ReadTable(r, ProtoRabbit); err != nil {
			return nil, err
		}
		return rData, nil
	case 'V':
		return nil, nil
	}

	return nil, errors.New(fmt.Sprintf("Unsupported type %c (%s) by %s protocol", vType, ProtoRabbit))
}

func WriteTable(writer io.Writer, table *Table, protoVersion string) (err error) {
	var buf = bytes.NewBuffer(make([]byte, 0))
	for key, v := range *table {
		if err := WriteShortstr(buf, key); err != nil {
			return err
		}
		if err := writeV(buf, v, protoVersion); err != nil {
			return err
		}
	}
	return WriteLongstr(writer, buf.Bytes())
}

func writeV(writer io.Writer, v interface{}, protoVersion string) (err error) {
	switch protoVersion {
	case Proto091:
		return writeValue091(writer, v)
	case ProtoRabbit:
		return writeValueRabbit(writer, v)
	}

	return errors.New(fmt.Sprintf("Unknown proto version [%s]", protoVersion))
}

/*
Standard amqp-0-9-1 table fields

't' bool			boolean
'b' int8			short-short-int
'B' uint8			short-short-uint
'U' int16			short-int
'u' uint16			short-uint
'I' int32			long-int
'i' uint32			long-uint
'L' int64			long-long-int
'l' uint64			long-long-uint
'f' float			float
'd' double			double
'D' Decimal			decimal-value
's' string			short-string
'S'	[]byte			long-string
'A' []interface{} 	field-array
'T' time.Time		timestamp
'F' Table			field-table
'V' nil				no-field
*/
func writeValue091(writer io.Writer, v interface{}) (err error) {
	switch value := v.(type) {
	case bool:
		if err = WriteOctet(writer, byte('t')); err == nil {
			if value {
				err = binary.Write(writer, binary.BigEndian, uint8(1))
			} else {
				err = binary.Write(writer, binary.BigEndian, uint8(0))
			}
		}
	case int8:
		if err = WriteOctet(writer, byte('b')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int8(value))
		}
	case uint8:
		if err = WriteOctet(writer, byte('B')); err == nil {
			err = binary.Write(writer, binary.BigEndian, uint8(value))
		}
	case int16:
		if err = WriteOctet(writer, byte('U')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int16(value))
		}
	case uint16:
		if err = binary.Write(writer, binary.BigEndian, byte('u')); err == nil {
			err = binary.Write(writer, binary.BigEndian, uint16(value))
		}
	case int32:
		if err = binary.Write(writer, binary.BigEndian, byte('I')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int32(value))
		}
	case uint32:
		if err = binary.Write(writer, binary.BigEndian, byte('i')); err == nil {
			err = binary.Write(writer, binary.BigEndian, uint32(value))
		}
	case int64:
		if err = binary.Write(writer, binary.BigEndian, byte('L')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int64(value))
		}
	case uint64:
		if err = binary.Write(writer, binary.BigEndian, byte('l')); err == nil {
			err = binary.Write(writer, binary.BigEndian, uint64(value))
		}
	case float32:
		if err = binary.Write(writer, binary.BigEndian, byte('f')); err == nil {
			err = binary.Write(writer, binary.BigEndian, float32(value))
		}
	case float64:
		if err = binary.Write(writer, binary.BigEndian, byte('d')); err == nil {
			err = binary.Write(writer, binary.BigEndian, float64(value))
		}
	case Decimal:
		if err = binary.Write(writer, binary.BigEndian, byte('D')); err == nil {
			if err = binary.Write(writer, binary.BigEndian, byte(value.Scale)); err == nil {
				err = binary.Write(writer, binary.BigEndian, uint32(value.Value))
			}
		}
	case string:
		if err = WriteOctet(writer, byte('s')); err == nil {
			err = WriteShortstr(writer, value)
		}
	case []byte:
		if err = WriteOctet(writer, byte('S')); err == nil {
			err = WriteLongstr(writer, value)
		}
	case time.Time:
		if err = WriteOctet(writer, byte('T')); err == nil {
			err = WriteTimestamp(writer, value)
		}
	case []interface{}:
		if err = WriteOctet(writer, byte('A')); err == nil {
			err = writeArray(writer, value, Proto091)
		}

	case Table:
		if err = WriteOctet(writer, byte('F')); err == nil {
			err = WriteTable(writer, &value, Proto091)
		}
	case nil:
		err = binary.Write(writer, binary.BigEndian, byte('V'))
	default:
		err = errors.New(fmt.Sprintf("Unsupported type by %s protocol", Proto091))
	}

	return
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
func writeValueRabbit(writer io.Writer, v interface{}) (err error) {
	switch value := v.(type) {
	case bool:
		if err = WriteOctet(writer, byte('t')); err == nil {
			if value {
				err = binary.Write(writer, binary.BigEndian, uint8(1))
			} else {
				err = binary.Write(writer, binary.BigEndian, uint8(0))
			}
		}
	case int8:
		if err = WriteOctet(writer, byte('b')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int8(value))
		}
	case uint8:
		if err = WriteOctet(writer, byte('b')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int8(value))
		}
	case int16:
		if err = WriteOctet(writer, byte('s')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int16(value))
		}
	case uint16:
		if err = binary.Write(writer, binary.BigEndian, byte('s')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int16(value))
		}
	case int32:
		if err = binary.Write(writer, binary.BigEndian, byte('I')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int32(value))
		}
	case uint32:
		if err = binary.Write(writer, binary.BigEndian, byte('I')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int32(value))
		}
	case int64:
		if err = binary.Write(writer, binary.BigEndian, byte('l')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int64(value))
		}
	case uint64:
		if err = binary.Write(writer, binary.BigEndian, byte('l')); err == nil {
			err = binary.Write(writer, binary.BigEndian, int64(value))
		}
	case float32:
		if err = binary.Write(writer, binary.BigEndian, byte('f')); err == nil {
			err = binary.Write(writer, binary.BigEndian, float32(value))
		}
	case float64:
		if err = binary.Write(writer, binary.BigEndian, byte('d')); err == nil {
			err = binary.Write(writer, binary.BigEndian, float64(value))
		}
	case Decimal:
		if err = binary.Write(writer, binary.BigEndian, byte('D')); err == nil {
			if err = binary.Write(writer, binary.BigEndian, byte(value.Scale)); err == nil {
				err = binary.Write(writer, binary.BigEndian, uint32(value.Value))
			}
		}
	case []byte:
		if err = WriteOctet(writer, byte('S')); err == nil {
			err = WriteLongstr(writer, value)
		}
	case string:
		if err = WriteOctet(writer, byte('S')); err == nil {
			err = WriteLongstr(writer, []byte(value))
		}
	case time.Time:
		if err = WriteOctet(writer, byte('T')); err == nil {
			err = WriteTimestamp(writer, value)
		}
	case []interface{}:
		if err = WriteOctet(writer, byte('x')); err == nil {
			err = writeArray(writer, value, ProtoRabbit)
		}
	case Table:
		if err = WriteOctet(writer, byte('F')); err == nil {
			err = WriteTable(writer, &value, ProtoRabbit)
		}
	case nil:
		err = binary.Write(writer, binary.BigEndian, byte('V'))
	default:
		err = errors.New(fmt.Sprintf("Unsupported type by %s protocol", Proto091))
	}

	return
}

func writeArray(writer io.Writer, array []interface{}, protoVersion string) error {
	var buf = bytes.NewBuffer([]byte{})
	for _, v := range array {
		if err := writeV(buf, v, protoVersion); err != nil {
			return err
		}
	}
	return WriteLongstr(writer, buf.Bytes())
}

func readArray(r io.Reader, protoVersion string) (data []interface{}, err error) {
	data = make([]interface{}, 0, 0)
	var arrayData []byte
	if arrayData, err = ReadLongstr(r); err != nil {
		return nil, err
	}

	arrayBuffer := bytes.NewBuffer(arrayData)
	for arrayBuffer.Len() > 0 {
		var itemV interface{}
		if itemV, err = readV(arrayBuffer, protoVersion); err != nil {
			return nil, err
		}

		data = append(data, itemV)
	}

	return data, nil
}

func ReadContentHeader(r io.Reader, protoVersion string) (*ContentHeader, error) {
	// 14 bytes for class-id | weight | body size | property flags
	var header = make([]byte, 14)
	if err := binary.Read(r, binary.BigEndian, header); err != nil {
		return nil, err
	}

	contentHeader := &ContentHeader{}
	headerBuf := bytes.NewBuffer(header)

	contentHeader.ClassId, _ = ReadShort(headerBuf)
	contentHeader.Weight, _ = ReadShort(headerBuf)
	contentHeader.BodySize, _ = ReadLonglong(headerBuf)
	contentHeader.PropertyFlags, _ = ReadShort(headerBuf)

	contentHeader.PropertyList = &BasicPropertyList{}
	if err := contentHeader.PropertyList.Read(r, contentHeader.PropertyFlags, protoVersion); err != nil {
		return nil, err
	}

	return contentHeader, nil
}
