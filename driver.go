package tinydriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"
	"strconv"
)

const driverName = "tiny"

func init() {
	sql.Register(driverName, NewDriver())
}

type Driver struct {
	connector *Connector
}

func NewDriver() *Driver {
	return &Driver{}
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	return connector.Connect(context.TODO())
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	cfg, err := d.parse(name)
	if err != nil {
		return nil, err
	}

	return NewConnector(cfg), nil
}

var rx = regexp.MustCompile("postgres:\\/\\/(.*):(.*)@(.*):(\\d+)\\/(.*)\\?sslmode=disable")

func (d *Driver) parse(dsn string) (*Config, error) {
	sub := rx.FindStringSubmatch(dsn)
	if len(sub) != 6 {
		return nil, errors.New("invalid connection string")
	}

	port, err := strconv.Atoi(sub[4])
	if err != nil {
		return nil, errors.New("invalid port value")
	}

	return &Config{
		Network:  "tcp",
		Host:     sub[3],
		Port:     port,
		Username: sub[1],
		Password: sub[2],
		Database: sub[5],
	}, nil
}
