package main

import (
	"errors"

	"github.com/opensourceways/server-common-lib/utils"

	kafka "github.com/opensourceways/kafka-lib/agent"
)

type configuration struct {
	Kafka     kafka.Config `json:"kafka"          required:"true"`
	Topic     string       `json:"topic"          required:"true"`
	UserAgent string       `json:"user_agent"     required:"true"`
}

func (c *configuration) validate() error {
	if c.Topic == "" {
		return errors.New("missing topic")
	}

	if c.UserAgent == "" {
		return errors.New("missing user_agent")
	}

	return c.Kafka.Validate()
}

func loadConfig(path string) (cfg configuration, err error) {
	if err = utils.LoadFromYaml(path, &cfg); err == nil {
		err = cfg.validate()
	}

	return
}
