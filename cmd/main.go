package main

import (
	"time"
	"bytes"
)

func main()  {
	bf := bytes.NewBuffer([]byte{})

	var v interface{}
	v = true
	switch value := v.(type) {
	case bool:
		bf.WriteByte('t')
		if value {
			bf.WriteByte(byte(1))
		} else {
			bf.WriteByte(byte(0))
		}
	}

	time.Sleep(time.Hour)
}