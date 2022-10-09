package main

import (
	"github.com/opensourceways/community-robot-lib/mq"
)

type configuration struct {
	Config mq.MQConfig `json:"config" required:"true"`
	Secret string      `json:"secret" required:"true"`
}

func (c *configuration) Validate() error {
	return nil
}

func (c *configuration) SetDefault() {
}
