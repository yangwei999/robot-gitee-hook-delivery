package main

import (
	"flag"
	"net/http"
	"os"
	"strconv"
	"time"

	kafka "github.com/opensourceways/kafka-lib/agent"
	"github.com/opensourceways/server-common-lib/interrupts"
	"github.com/opensourceways/server-common-lib/logrusutil"
	liboptions "github.com/opensourceways/server-common-lib/options"
	"github.com/opensourceways/server-common-lib/secret"
	"github.com/sirupsen/logrus"
)

const component = "robot-gitee-hook-delivery"

type options struct {
	service        liboptions.ServiceOptions
	enableDebug    bool
	hmacSecretFile string
}

func (o *options) Validate() error {

	return o.service.Validate()
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	o.service.AddFlags(fs)

	fs.BoolVar(
		&o.enableDebug, "enable_debug", false,
		"whether to enable debug model.",
	)

	fs.StringVar(
		&o.hmacSecretFile, "hmac-secret-file", "/etc/webhook/hmac",
		"Path to the file containing the HMAC secret.",
	)

	_ = fs.Parse(args)
	return o
}

func main() {
	logrusutil.ComponentInit(component)
	log := logrus.NewEntry(logrus.StandardLogger())

	o := gatherOptions(
		flag.NewFlagSet(os.Args[0], flag.ExitOnError),
		os.Args[1:]...,
	)
	if err := o.Validate(); err != nil {
		logrus.Errorf("invalid options, err:%s", err.Error())

		return
	}

	if o.enableDebug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("debug enabled.")
	}

	// cfg
	cfg, err := loadConfig(o.service.ConfigFile)
	if err != nil {
		logrus.Errorf("load config, err:%s", err.Error())

		return
	}

	// init kafka
	if err := kafka.Init(&cfg.Kafka, log); err != nil {
		logrus.Errorf("init kafka, err:%s", err.Error())

		return
	}

	defer kafka.Exit()

	// hmac
	secretAgent := new(secret.Agent)
	if err := secretAgent.Start([]string{o.hmacSecretFile}); err != nil {
		logrus.Errorf("start secret agent, err:%s", err.Error())

		return
	}

	defer secretAgent.Stop()

	hmac := secretAgent.GetTokenGenerator(o.hmacSecretFile)

	// server
	d := delivery{
		topic:     cfg.Topic,
		userAgent: cfg.UserAgent,
		hmac: func() string {
			return string(hmac())
		},
	}

	defer d.wait()

	run(&d, o.service.Port, o.service.GracePeriod)
}

func run(d *delivery, port int, gracePeriod time.Duration) {
	defer interrupts.WaitForGracefulShutdown()

	// Return 200 on / for health checks.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})

	// For /gitee-hook, handle a webhook normally.
	http.Handle("/gitee-hook", d)

	httpServer := &http.Server{Addr: ":" + strconv.Itoa(port)}

	interrupts.ListenAndServe(httpServer, gracePeriod)
}
