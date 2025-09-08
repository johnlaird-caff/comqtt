// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 wind
// SPDX-FileContributor: wind (573966@qq.com)

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johnlaird-caff/comqtt/cluster/log"
	"github.com/johnlaird-caff/comqtt/config"
	"github.com/johnlaird-caff/comqtt/mqtt"
	"github.com/johnlaird-caff/comqtt/mqtt/hooks/auth"
	"github.com/johnlaird-caff/comqtt/mqtt/hooks/storage/badger"
	"github.com/johnlaird-caff/comqtt/mqtt/hooks/storage/bolt"
	"github.com/johnlaird-caff/comqtt/mqtt/hooks/storage/redis"
	"github.com/johnlaird-caff/comqtt/mqtt/listeners"
	"github.com/johnlaird-caff/comqtt/mqtt/rest"
	"github.com/johnlaird-caff/comqtt/plugin"
	hauth "github.com/johnlaird-caff/comqtt/plugin/auth/http"
	mauth "github.com/johnlaird-caff/comqtt/plugin/auth/mysql"
	pauth "github.com/johnlaird-caff/comqtt/plugin/auth/postgresql"
	rauth "github.com/johnlaird-caff/comqtt/plugin/auth/redis"
	cokafka "github.com/johnlaird-caff/comqtt/plugin/bridge/kafka"
	rv8 "github.com/redis/go-redis/v9"
	"go.etcd.io/bbolt"
)

func pprof() {
	go func() {
		log.Info("listen pprof", "error", http.ListenAndServe(":6060", nil))
	}()
}

func main() {
	sigCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	err := realMain(sigCtx)
	onError(err, "")
}

func realMain(ctx context.Context) error {
	var err error
	var confFile string
	cfg := config.New()

	flag.StringVar(&confFile, "conf", "", "read the program parameters from the config file")
	flag.UintVar(&cfg.StorageWay, "storage-way", 1, "storage way optional items:0 memory, 1 bolt, 2 badger, 3 redis")
	flag.UintVar(&cfg.Auth.Way, "auth-way", 0, "authentication way optional items:0 anonymous, 1 username and password, 2 clientid")
	flag.UintVar(&cfg.Auth.Datasource, "auth-ds", 0, "authentication datasource optional items:0 free, 1 redis, 2 mysql, 3 postgresql, 4 http")
	flag.StringVar(&cfg.Auth.ConfPath, "auth-path", "", "config file path should correspond to the auth-datasource")
	flag.StringVar(&cfg.Mqtt.TCP, "tcp", ":1883", "network address for Mqtt TCP listener")
	flag.StringVar(&cfg.Mqtt.WS, "ws", ":1882", "network address for Mqtt Websocket listener")
	flag.StringVar(&cfg.Mqtt.HTTP, "http", ":8080", "network address for web info dashboard listener")
	flag.BoolVar(&cfg.Log.Enable, "log-enable", true, "log enabled or not")
	flag.StringVar(&cfg.Log.Filename, "log-file", "./logs/comqtt.log", "log filename")
	//parse arguments
	flag.Parse()
	//load config file
	if len(confFile) > 0 {
		if cfg, err = config.Load(confFile); err != nil {
			onError(err, "")
		}
	}

	//enable pprof
	if cfg.PprofEnable {
		pprof()
	}

	//init log
	log.Init(&cfg.Log)
	if cfg.Log.Enable && cfg.Log.Output == log.OutputFile {
		fmt.Println("log output to the files, please check")
	}

	// create server instance and init hooks
	cfg.Mqtt.Options.Logger = log.Default()
	server := mqtt.New(&cfg.Mqtt.Options)
	log.Info("comqtt server initializing...")
	initStorage(server, cfg)
	initAuth(server, cfg)
	initBridge(server, cfg)

	// gen tls config
	var listenerConfig *listeners.Config
	if tlsConfig, err := config.GenTlsConfig(cfg); err != nil {
		onError(err, "")
	} else {
		if tlsConfig != nil {
			listenerConfig = &listeners.Config{TLSConfig: tlsConfig}
		}
	}

	// add tcp listener
	tcp := listeners.NewTCP("tcp", cfg.Mqtt.TCP, listenerConfig)
	onError(server.AddListener(tcp), "add tcp listener")

	// add websocket listener
	ws := listeners.NewWebsocket("ws", cfg.Mqtt.WS, listenerConfig)
	onError(server.AddListener(ws), "add websocket listener")

	// add http listener
	http := listeners.NewHTTP("stats", cfg.Mqtt.HTTP, nil, rest.New(server).GenHandlers())
	onError(server.AddListener(http), "add http listener")

	errCh := make(chan error, 1)
	// start server
	go func() {
		err := server.Serve()
		if err != nil {
			errCh <- err
		}
	}()

	//log.Info("comqtt server started")

	select {
	case err := <-errCh:
		onError(err, "server error")
	case <-ctx.Done():
		log.Warn("caught signal, stopping...")
	}
	server.Close()
	log.Info("main.go finished")
	return nil
}

func initAuth(server *mqtt.Server, conf *config.Config) {
	logMsg := "init auth"
	if conf.Auth.Way == config.AuthModeAnonymous {
		server.AddHook(new(auth.AllowHook), nil)
	} else if conf.Auth.Way == config.AuthModeUsername || conf.Auth.Way == config.AuthModeClientid {
		switch conf.Auth.Datasource {
		case config.AuthDSRedis:
			opts := rauth.Options{}
			onError(plugin.LoadYaml(conf.Auth.ConfPath, &opts), logMsg)
			onError(server.AddHook(new(rauth.Auth), &opts), logMsg)
		case config.AuthDSMysql:
			opts := mauth.Options{}
			onError(plugin.LoadYaml(conf.Auth.ConfPath, &opts), logMsg)
			onError(server.AddHook(new(mauth.Auth), &opts), logMsg)
		case config.AuthDSPostgresql:
			opts := pauth.Options{}
			onError(plugin.LoadYaml(conf.Auth.ConfPath, &opts), logMsg)
			onError(server.AddHook(new(pauth.Auth), &opts), logMsg)
		case config.AuthDSHttp:
			opts := hauth.Options{}
			onError(plugin.LoadYaml(conf.Auth.ConfPath, &opts), logMsg)
			onError(server.AddHook(new(hauth.Auth), &opts), logMsg)
		}
	} else {
		onError(config.ErrAuthWay, logMsg)
	}
}

func initStorage(server *mqtt.Server, conf *config.Config) {
	logMsg := "init storage"
	switch conf.StorageWay {
	case config.StorageWayBolt:
		onError(server.AddHook(new(bolt.Hook), &bolt.Options{
			Path: conf.StoragePath,
			Options: &bbolt.Options{
				Timeout: 500 * time.Millisecond,
			},
		}), logMsg)
	case config.StorageWayBadger:
		onError(server.AddHook(new(badger.Hook), &badger.Options{
			Path: conf.StoragePath,
		}), logMsg)
	case config.StorageWayRedis:
		onError(server.AddHook(new(redis.Hook), &redis.Options{
			HPrefix: conf.Redis.HPrefix,
			Options: &rv8.Options{
				Addr:     conf.Redis.Options.Addr,
				DB:       conf.Redis.Options.DB,
				Password: conf.Redis.Options.Password,
			},
		}), logMsg)
	}
}

func initBridge(server *mqtt.Server, conf *config.Config) {
	logMsg := "init bridge"
	if conf.BridgeWay == config.BridgeWayNone {
		return
	} else if conf.BridgeWay == config.BridgeWayKafka {
		opts := cokafka.Options{}
		onError(plugin.LoadYaml(conf.BridgePath, &opts), logMsg)
		onError(server.AddHook(new(cokafka.Bridge), &opts), logMsg)
	}
}

// onError handle errors and simplify code
func onError(err error, msg string) {
	if err != nil {
		log.Error(msg, "error", err)
		os.Exit(1)
	}
}
