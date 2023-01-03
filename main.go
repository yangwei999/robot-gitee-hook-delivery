package main

import (
	"flag"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/opensourceways/community-robot-lib/config"
	"github.com/opensourceways/community-robot-lib/interrupts"
	"github.com/opensourceways/community-robot-lib/kafka"
	"github.com/opensourceways/community-robot-lib/logrusutil"
	"github.com/opensourceways/community-robot-lib/mq"
	liboptions "github.com/opensourceways/community-robot-lib/options"
	"github.com/opensourceways/community-robot-lib/secret"
	"github.com/sirupsen/logrus"
)

const component = "robot-gitee-hook-delivery"

type options struct {
	service        liboptions.ServiceOptions
	hmacSecretFile string
}

func (o *options) Validate() error {

	return o.service.Validate()
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	o.service.AddFlags(fs)

	fs.StringVar(&o.hmacSecretFile, "hmac-secret-file", "/etc/webhook/hmac", "Path to the file containing the HMAC secret.")

	_ = fs.Parse(args)
	return o
}

func main() {
	logrusutil.ComponentInit(component)

	o := gatherOptions(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:]...)
	if err := o.Validate(); err != nil {
		logrus.WithError(err).Fatal("Invalid options")
	}

	configAgent := config.NewConfigAgent(func() config.Config {
		return new(configuration)
	})
	if err := configAgent.Start(o.service.ConfigFile); err != nil {
		logrus.WithError(err).Fatal("Error starting config agent.")
	}

	getConfiguration := func() *configuration {
		cfgn := new(configuration)
		_, cfg := configAgent.GetConfig()

		if v, ok := cfg.(*configuration); ok {
			cfgn = v
		}

		return cfgn
	}

	secretAgent := new(secret.Agent)
	if err := secretAgent.Start([]string{o.hmacSecretFile}); err != nil {
		logrus.WithError(err).Fatal("Error starting secret agent.")
	}

	gethmac := secretAgent.GetTokenGenerator(o.hmacSecretFile)

	cfg := getConfiguration()
	c := courier{topic: cfg.Topic, hmac: func() string {
		return string(gethmac())
	}}

	if err := initBroker(cfg); err != nil {
		logrus.WithError(err).Fatal("Error init broker.")
	}

	defer interrupts.WaitForGracefulShutdown()
	interrupts.OnInterrupt(func() {
		configAgent.Stop()

		_ = kafka.Disconnect()

		c.wait()
	})

	run(&c, o.service.Port, o.service.GracePeriod)
}

func run(c *courier, port int, gracePeriod time.Duration) {

	// Return 200 on / for health checks.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})

	// For /hook, handle a webhook normally.
	http.Handle("/gitee-hook", c)

	httpServer := &http.Server{Addr: ":" + strconv.Itoa(port)}

	interrupts.ListenAndServe(httpServer, gracePeriod)
}

func initBroker(cfg *configuration) error {
	tlsConfig, err := cfg.Config.TLSConfig.TLSConfig()
	if err != nil {
		return err
	}

	err = kafka.Init(
		mq.Addresses(cfg.Config.Addresses...),
		mq.SetTLSConfig(tlsConfig),
		mq.Log(logrus.WithField("module", "broker")),
	)

	if err != nil {
		return err
	}

	return kafka.Connect()
}
