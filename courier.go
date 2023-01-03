package main

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/opensourceways/community-robot-lib/giteeclient"
	"github.com/opensourceways/community-robot-lib/kafka"
	"github.com/opensourceways/community-robot-lib/mq"
	"github.com/sirupsen/logrus"
)

type courier struct {
	wg    sync.WaitGroup
	topic string
	hmac  func() string
}

func (c *courier) wait() {
	c.wg.Wait()
}

// ServeHTTP validates an incoming webhook and puts it into the event channel.
func (c *courier) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	eventType, eventGUID, payload, _, ok := giteeclient.ValidateWebhook(w, r, c.hmac)
	if !ok {
		return
	}
	fmt.Fprint(w, "Event received. Have a nice day.")

	l := logrus.WithFields(
		logrus.Fields{
			"event-type": eventType,
			"event-id":   eventGUID,
		},
	)

	if err := c.publish(payload, r.Header, l); err != nil {
		l.WithError(err).Error()
	}
}

func (c *courier) publish(payload []byte, h http.Header, l *logrus.Entry) error {
	header := map[string]string{
		"content-type":      h.Get("content-type"),
		"X-Gitee-Event":     h.Get("X-Gitee-Event"),
		"X-Gitee-Timestamp": h.Get("X-Gitee-Timestamp"),
		"X-Gitee-Token":     h.Get("X-Gitee-Token"),
	}

	msg := mq.Message{
		Header: header,
		Body:   payload,
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		if err := kafka.Publish(c.topic, &msg); err != nil {
			l.WithError(err).Error("failed to publish msg")
		} else {
			l.Info(fmt.Sprintf("publish message to %s topic success", c.topic))
		}
	}()

	return nil
}
