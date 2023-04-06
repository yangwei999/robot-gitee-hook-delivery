package main

import (
	"fmt"
	"net/http"
	"sync"

	kafka "github.com/opensourceways/kafka-lib/agent"
	"github.com/opensourceways/robot-gitee-lib/client"
	"github.com/sirupsen/logrus"
)

type delivery struct {
	wg        sync.WaitGroup
	hmac      func() string
	topic     string
	userAgent string
}

func (d *delivery) wait() {
	d.wg.Wait()
}

func (d *delivery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	eventType, eventGUID, payload, _, ok := client.ValidateWebhook(w, r, d.hmac)
	if !ok {
		return
	}

	fmt.Fprint(w, "Event received. Have a nice day.")

	d.publish(payload, r.Header, eventType, eventGUID)
}

func (d *delivery) publish(payload []byte, h http.Header, eventType, eventGUID string) {
	header := map[string]string{
		"content-type":      h.Get("content-type"),
		"X-Gitee-Event":     h.Get("X-Gitee-Event"),
		"X-Gitee-Timestamp": h.Get("X-Gitee-Timestamp"),
		"X-Gitee-Token":     h.Get("X-Gitee-Token"),
		"User-Agent":        d.userAgent,
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		l := logrus.WithFields(
			logrus.Fields{
				"event-type": eventType,
				"event-id":   eventGUID,
			},
		)

		if err := kafka.Publish(d.topic, header, payload); err != nil {
			l.Errorf("failed to publish msg, err:%s", err.Error())
		} else {
			l.Debugf("publish message to topic(%s) successfully", d.topic)
		}
	}()
}
