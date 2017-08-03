package main

import (
	"flag"
	"fmt"
	"github.com/Mansfield6/tikv-proxy-demo/proxy/handler"
	"github.com/Mansfield6/tikv-proxy-demo/proxy/redis"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/store/tikv"
	"strings"
)

var (
	serverPort = flag.Int("port", 6379, "listen port,default: 6379")
	pdAddr     = flag.String("pd", "localhost:2379", "pd address,default:localhost:2379")
	logPath    = flag.String("lp", "", "log file path, if empty, default:stdout")
	logLevel   = flag.String("ll", "info", "log level:INFO|WARN|ERROR default:INFO")
)

func main() {
	flag.Parse()

	defer func() {
		if msg := recover(); msg != nil {
			log.Infof("Panic: %v\n", msg)
		}
	}()

	initlog()

	log.Info("serverPort:", *serverPort)
	log.Info("pdAddr:", *pdAddr)
	log.Info("logpath:", *logPath)
	log.Info("logpaht:", *logLevel)

	driver := tikv.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s?cluster=1", *pdAddr))
	if err != nil {
		log.Fatal(err)
	}

	myhandler := &handler.TxTikvHandler{store}

	srv, err := redis.NewServer(redis.DefaultConfig().Port(*serverPort).Handler(myhandler))
	if err != nil {
		panic(err)
	}

	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

func initlog() {
	if len(*logPath) > 0 {
		log.SetHighlighting(false)
		err := log.SetOutputByName(*logPath)
		if err != nil {
			log.Fatalf("set log name failed - %s", err)
		}
	}

	switch strings.ToUpper(*logLevel) {
	case "INFO":
		log.SetLevel(log.LOG_LEVEL_INFO)
	case "ERROR":
		log.SetLevel(log.LOG_LEVEL_ERROR)
	case "WARN":
		log.SetLevel(log.LOG_LEVEL_WARN)
	default:
		log.SetLevel(log.LOG_LEVEL_INFO)
	}

	log.SetRotateByDay()
}
