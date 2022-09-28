package tinydriver

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type rows struct {
	cn     *Connection
	desc   []*rowDescription
	closed bool
}

type rowDescription struct {
	name string
	oid  int32
}

// PostgreSQL type ID
const (
	oidInt4 = 23
	oidText = 25
)

func newRows(cn *Connection, desc []*rowDescription) *rows {
	return &rows{
		cn:   cn,
		desc: desc,
	}
}

func (r *rows) Columns() []string {
	if r.closed || r.desc == nil {
		return nil
	}

	var names []string

	for _, item := range r.desc {
		names = append(names, item.name)
	}

	return names
}

func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	defer r.close()

	for {
		switch err := r.Next(nil); err {
		case nil, io.EOF:
			return nil
		default: // unexpected error
			_ = r.cn.Close()
			return err
		}
	}
}

func (r *rows) close() {
	r.closed = true

	if r.desc != nil {
		r.desc = nil
	}
}

func (r *rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	eof, err := r.next(dest)

	switch {
	case errors.Is(err, io.EOF):
		return io.ErrUnexpectedEOF
	case err != nil:
		return fmt.Errorf("next: %w", err)
	case eof:
		return io.EOF
	}

	return nil
}

func (r *rows) next(dest []driver.Value) (eof bool, err error) {
	for {
		msgType, err := r.cn.reader.ReadMessageType()
		if err != nil {
			return false, err
		}

		msgLen, err := r.cn.reader.MsgLen()
		if err != nil {
			return false, err
		}

		switch msgType {
		case msgDataRow:
			return false, r.readDataRow(dest)
		case msgCommandComplete:
			if err := r.cn.reader.Discard(msgLen); err != nil {
				return false, err
			}
		case msgReadyForQuery:
			r.close()

			if err := r.cn.reader.Discard(msgLen); err != nil {
				return false, err
			}

			return true, nil
		default:
			return false, fmt.Errorf("not implemented: %s", string(msgType))
		}
	}
}

func (r *rows) readDataRow(dest []driver.Value) error {
	numCol, err := r.cn.reader.int16()
	if err != nil {
		return err
	}

	if len(dest) != int(numCol) {
		return fmt.Errorf("query returned %d columns, but Scan dest has %d items",
			numCol, len(dest))
	}

	for i := 0; i < int(numCol); i++ {
		dataLen, err := r.cn.reader.Int32()
		if err != nil {
			return err
		}

		val, err := r.readColumnValue(r.desc[i].oid, int(dataLen))
		if err != nil {
			return err
		}

		if dest != nil {
			dest[i] = val
		}
	}

	return nil
}

func (r *rows) readColumnValue(dataType int32, dataLen int) (interface{}, error) {
	b, err := r.cn.reader.ReadNumBytes(dataLen)
	if err != nil {
		return nil, err
	}

	switch dataType {
	case oidInt4:
		return strconv.ParseInt(string(b), 10, 32)
	case oidText:
		return string(b), nil
	default:
		return nil, fmt.Errorf("data type not implemented: %s", dataType)
	}
}
