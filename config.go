package tinydriver

import "fmt"

type Config struct {
	Network  string
	Host     string
	Port     int
	Username string
	Password string
	Database string
}

func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
