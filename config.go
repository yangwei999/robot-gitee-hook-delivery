package main

import (
	"errors"

	"github.com/opensourceways/community-robot-lib/mq"
)

type configuration struct {
	Config mq.MQConfig `json:"config" required:"true"`
	Topic  string      `json:"topic" required:"true"`
}

func (c *configuration) Validate() error {
	if c.Topic == "" {
		return errors.New("missing topic")
	}

	return nil
}

func (c *configuration) SetDefault() {
}
