package tinydriver

import (
	"context"
	"database/sql/driver"
)

type Connector struct {
	cfg *Config
}

func NewConnector(cfg *Config) *Connector {
	return &Connector{cfg: cfg}
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return NewConnection(ctx, c.cfg)
}

func (c *Connector) Driver() driver.Driver {
	return &Driver{connector: c}
}
