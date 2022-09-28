package tinydriver

import (
	"bufio"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"mellium.im/sasl"
)

// https://www.postgresql.org/docs/current/protocol-message-formats.html

// Message format
const (
	msgAuthenticationOk           = 'R'
	msgBackendKeyData             = 'K'
	msgReadyForQuery              = 'Z'
	msgParameterStatus            = 'S'
	msgSASLInitialResponse        = 'p'
	msgSASLResponse               = 'p'
	msgAuthenticationSASLContinue = 'R'
	msgAuthenticationSASLFinal    = 'R'
	msgErrorResponse              = 'E'
	msgRowDescription             = 'T'
	msgCommandComplete            = 'C'
	msgNoticeResponse             = 'N'

	msgDataRow = 'D'
	msgQuery   = 'Q'

	kindAuthenticationOk int32 = 10
)

type Connection struct {
	cfg       *Config
	conn      net.Conn
	reader    *Reader
	processID int32
	secretKey int32
}

func (c *Connection) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

const protocolVersion int32 = 196608

func NewConnection(ctx context.Context, cfg *Config) (*Connection, error) {
	netDialer := &net.Dialer{
		Timeout:   time.Second,
		KeepAlive: 5 * time.Minute,
	}

	nConn, err := netDialer.DialContext(ctx, cfg.Network, cfg.Addr())
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	reader := NewReader(bufio.NewReader(nConn))
	conn := Connection{conn: nConn, reader: reader, cfg: cfg}

	if err := conn.handshake(ctx); err != nil {
		return nil, fmt.Errorf("handshake: %w", err)
	}

	return &conn, nil
}

func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) handshake(ctx context.Context) error {
	data := c.prepareStartupMsg()

	if _, err := c.conn.Write(data); err != nil {
		return err
	}

	for {
		msgType, err := c.reader.ReadMessageType()
		if err != nil {
			return err
		}

		msgLen, err := c.reader.MsgLen()
		if err != nil {
			return err
		}

		switch msgType {
		case msgAuthenticationOk:
			// STEP 1: AUTH
			kind, err := c.reader.Int32()
			if err != nil {
				return fmt.Errorf("get auth kind: %w", err)
			}

			switch kind {
			case kindAuthenticationOk:
				if err := c.authSASL(ctx, c.reader); err != nil {
					return fmt.Errorf("auth: %w", err)
				}
			default:
				return fmt.Errorf("auth kind not implemented: %d", kind)
			}
		case msgReadyForQuery:
			return c.reader.Discard(msgLen)
		case msgBackendKeyData:
			// STEP 3: BackendKeyData
			processID, err := c.reader.Int32()
			if err != nil {
				return err
			}

			secretKey, err := c.reader.Int32()
			if err != nil {
				return err
			}

			c.processID = processID
			c.secretKey = secretKey
		case msgParameterStatus:
			// STEP 2: parameterStatus
			if err := c.reader.Discard(msgLen); err != nil {
				return err
			}

		default:
			return fmt.Errorf("not implemented: %s", string(msgType))
		}
	}
}

func (c *Connection) writeQuery(ctx context.Context, query string) error {
	var b Buffer

	b.WriteStartMsg(msgQuery)
	b.WriteString(query)
	b.CalculateSize(1)

	_, err := c.conn.Write(b.Data())

	return err
}

func (c *Connection) authSASL(ctx context.Context, reader *Reader) error {
	var saslMech sasl.Mechanism

	mech, err := reader.String()
	if err != nil {
		return err
	}

	switch mech {
	case sasl.ScramSha256.Name:
		saslMech = sasl.ScramSha256
	default:
		return fmt.Errorf("mechanism not implemented: %s", mech)
	}

	// extra zero
	_, err = reader.String()
	if err != nil {
		return err
	}

	creds := sasl.Credentials(func() (Username, Password, Identity []byte) {
		return []byte(c.cfg.Username), []byte(c.cfg.Password), nil
	})

	client := sasl.NewClient(saslMech, creds)

	_, resp, err := client.Step(nil)
	if err != nil {
		return fmt.Errorf("client.Step 1 failed: %w", err)
	}

	data := c.saslFirstMsg(saslMech, resp)

	_, err = c.conn.Write(data)
	if err != nil {
		return err
	}

	msgType, err := reader.ReadMessageType()
	if err != nil {
		return err
	}

	switch msgType {
	case msgAuthenticationSASLContinue:
		msgLen, err := reader.MsgLen()
		if err != nil {
			return err
		}

		challenge, err := reader.Int32()
		if err != nil {
			return err
		}

		if challenge != 11 {
			return errors.New("challenge failed")
		}

		const challengeLen = 4 // int32

		payload, err := reader.ReadNumBytes(int(msgLen - challengeLen))
		if err != nil {
			return err
		}

		_, resp, err = client.Step(payload)
		if err != nil {
			return fmt.Errorf("client.Step 2 failed: %w", err)
		}

		var b Buffer

		b.WriteStartMsg(msgSASLResponse)
		b.WriteBytes(resp)
		b.CalculateSize(1)

		if _, err = c.conn.Write(b.Data()); err != nil {
			return err
		}
	default:
		return fmt.Errorf("not implemented: %s", string(msgType))
	}

	msgType, err = reader.ReadMessageType()
	if err != nil {
		return err
	}

	msgLen, err := reader.MsgLen()
	if err != nil {
		return err
	}

	switch msgType {
	case msgAuthenticationSASLFinal:
		challenge, err := reader.Int32()
		if err != nil {
			return err
		}

		if challenge != 12 {
			return errors.New("challenge failed")
		}

		resp, err := reader.ReadNumBytes(int(msgLen - 4))
		if err != nil {
			return err
		}

		msgType, err := reader.ReadMessageType()
		if err != nil {
			return err
		}

		_, err = reader.MsgLen()
		if err != nil {
			return err
		}

		switch msgType {
		case msgAuthenticationOk:
			n, err := reader.Int32()
			if err != nil {
				return err
			}

			if n != 0 {
				return errors.New("invalid auth code")
			}
		default:
			return fmt.Errorf("not implemented: %s", string(msgType))
		}

		if _, _, err := client.Step(resp); err != nil {
			return fmt.Errorf("client.Step 3 failed: %w", err)
		}

		if client.State() != sasl.ValidServerResponse {
			return fmt.Errorf("got state=%q, wanted %q", client.State(), sasl.ValidServerResponse)
		}
	case msgErrorResponse:
		dErr, err := c.reader.parseError()
		if err != nil {
			return err
		}

		return dErr
	default:
		return fmt.Errorf("not implemented: %s", string(msgType))
	}

	log.Println("authentication was successful")

	return nil
}

func (c *Connection) prepareStartupMsg() []byte {
	var b Buffer
	// size of data (4 byte) without first byte (message type)
	b.WriteBytes([]byte{0, 0, 0, 0})
	b.WriteInt32(protocolVersion)
	b.WriteString("user")
	b.WriteString(c.cfg.Username)
	b.WriteString("database")
	b.WriteString(c.cfg.Database)
	b.WriteBytes([]byte{0})

	b.CalculateSize(0)

	return b.Data()
}

func (c *Connection) saslFirstMsg(mech sasl.Mechanism, resp []byte) []byte {
	var b Buffer

	b.WriteBytes([]byte{msgSASLInitialResponse})
	b.WriteBytes([]byte{0, 0, 0, 0})
	b.WriteString(mech.Name)
	b.WriteInt32(int32(len(resp)))
	b.WriteBytes(resp)

	b.CalculateSize(1)

	return b.Data()
}

func (c *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, err := formatQuery(query, args)
	if err != nil {
		return nil, fmt.Errorf("format query: %w", err)
	}

	if err := c.writeQuery(ctx, q); err != nil {
		return nil, fmt.Errorf("write query: %w", err)
	}

	rows, err := c.readQueryData(ctx)
	if err != nil {
		return nil, fmt.Errorf("read query data: %w", err)
	}

	return rows, nil
}

func (c *Connection) readQueryData(ctx context.Context) (*rows, error) {
	for {
		msgType, err := c.reader.ReadMessageType()
		if err != nil {
			return nil, fmt.Errorf("read message type: %w", err)
		}

		msgLen, err := c.reader.Int32()
		if err != nil {
			return nil, fmt.Errorf("read int32: %w", err)
		}

		switch msgType {
		case msgRowDescription:
			rowsDesc, err := c.rowDescription(ctx)
			if err != nil {
				return nil, fmt.Errorf("read row description: %w", err)
			}

			return newRows(c, rowsDesc), nil
		case msgCommandComplete, msgNoticeResponse, msgParameterStatus:
			if err := c.reader.Discard(msgLen); err != nil {
				return nil, fmt.Errorf("discard: %w", err)
			}
		case msgErrorResponse:
			dErr, err := c.reader.parseError()
			if err != nil {
				return nil, fmt.Errorf("parse error: %w", err)
			}

			return nil, dErr
		default:
			return nil, fmt.Errorf("not implemented: %s", string(msgType))
		}
	}
}

func (c *Connection) rowDescription(ctx context.Context) ([]*rowDescription, error) {
	numCol, err := c.reader.int16()
	if err != nil {
		return nil, fmt.Errorf("read int16: %w", err)
	}

	var descs []*rowDescription

	for i := 0; i < int(numCol); i++ {
		columnName, err := c.reader.String()
		if err != nil {
			return nil, err
		}

		objID, err := c.reader.Int32()
		if err != nil {
			return nil, err
		}

		attrID, err := c.reader.int16()
		if err != nil {
			return nil, err
		}

		oid, err := c.reader.Int32()
		if err != nil {
			return nil, err
		}

		typLen, err := c.reader.int16()
		if err != nil {
			return nil, err
		}

		attTypMod, err := c.reader.Int32()
		if err != nil {
			return nil, err
		}

		format, err := c.reader.int16()
		if err != nil {
			return nil, err
		}

		log.Printf(
			"column name: %s, object id: %d attr id: %d, oid: %d, type len: %d , att type mod: %d format: %d",
			columnName,
			objID,
			attrID,
			oid,
			typLen,
			attTypMod,
			format,
		)

		desc := rowDescription{
			name: columnName,
			oid:  oid,
		}

		descs = append(descs, &desc)
	}

	return descs, nil
}

func formatQuery(query string, args []driver.NamedValue) (string, error) {
	switch len(args) {
	case 0:
		return query, nil
	case 1:
		val, ok := args[0].Value.(int64)
		if !ok {
			return query, errors.New("type casting error")
		}

		id := strconv.Itoa(int(val))

		return strings.Replace(query, "$1", id, 1), nil
	default:
		return query, errors.New("not implemented: only one arg")

	}
}
