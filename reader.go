package tinydriver

import (
	"bufio"
	"encoding/binary"
	"log"
)

const strSeparator byte = 0

type Reader struct {
	*bufio.Reader
}

func NewReader(reader *bufio.Reader) *Reader {
	return &Reader{Reader: reader}
}

func (r Reader) ReadNumBytes(num int) ([]byte, error) {
	var buf []byte

	for i := 0; i < num; i++ {
		b, err := r.ReadByte()
		if err != nil {
			log.Print("EOF: ", i)
			return nil, err
		}

		buf = append(buf, b)
	}

	return buf, nil
}

func (r Reader) ReadMessageType() (byte, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	return b, nil
}

func (r Reader) Int32() (int32, error) {
	buf, err := r.ReadNumBytes(4)
	if err != nil {
		return 0, err
	}

	return int32(binary.BigEndian.Uint32(buf)), nil
}

func (r Reader) int16() (int16, error) {
	buf, err := r.ReadNumBytes(2)
	if err != nil {
		return 0, err
	}

	return int16(binary.BigEndian.Uint16(buf)), nil
}

func (r Reader) Discard(n int32) error {
	_, err := r.ReadNumBytes(int(n))
	if err != nil {
		return err
	}

	return nil
}

func (r Reader) MsgLen() (int32, error) {
	buf, err := r.ReadNumBytes(4)
	if err != nil {
		return 0, err
	}

	return int32(binary.BigEndian.Uint32(buf)) - 4, nil
}

func (r Reader) String() (string, error) {
	data, err := r.ReadBytes(strSeparator)
	if err != nil {
		return "", err
	}

	return string(data[:len(data)-1]), nil
}

func (r Reader) parseError() (error, error) {
	var dErr driverError

	const (
		errSeverityKey = 'S'
		errMessageKey  = 'M'
		errCodeKey     = 'C'
	)

	for {
		key, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if key == 0 {
			break
		}

		str, err := r.String()
		if err != nil {
			return nil, err
		}

		switch key {
		case errSeverityKey:
			dErr.Severity = str
		case errMessageKey:
			dErr.Message = str
		case errCodeKey:
			dErr.Code = str
		}
	}

	return dErr, nil
}
