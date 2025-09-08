// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 wind
// SPDX-FileContributor: wind (573966@qq.com)

package main

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	csRt "github.com/johnlaird-caff/comqtt/cluster/rest"

	cs "github.com/johnlaird-caff/comqtt/cluster"
	"github.com/johnlaird-caff/comqtt/cluster/log"
	coredis "github.com/johnlaird-caff/comqtt/cluster/storage/redis"
	"github.com/johnlaird-caff/comqtt/config"
	mqtt "github.com/johnlaird-caff/comqtt/mqtt"
	"github.com/johnlaird-caff/comqtt/mqtt/hooks/auth"
	"github.com/johnlaird-caff/comqtt/mqtt/listeners"
	mqttRt "github.com/johnlaird-caff/comqtt/mqtt/rest"
	"github.com/johnlaird-caff/comqtt/plugin"
	hauth "github.com/johnlaird-caff/comqtt/plugin/auth/http"
	mauth "github.com/johnlaird-caff/comqtt/plugin/auth/mysql"
	pauth "github.com/johnlaird-caff/comqtt/plugin/auth/postgresql"
	rauth "github.com/johnlaird-caff/comqtt/plugin/auth/redis"
	cokafka "github.com/johnlaird-caff/comqtt/plugin/bridge/kafka"
	"github.com/redis/go-redis/v9"
)

var agent *cs.Agent

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
	var members string
	cfg := config.New()

	flag.StringVar(&confFile, "conf", "", "read the program parameters from the config file")
	flag.UintVar(&cfg.StorageWay, "storage-way", 3, "storage way options:0 memory, 1 bolt, 2 badger, 3 redis")
	flag.UintVar(&cfg.Auth.Way, "auth-way", 0, "authentication way options:0 anonymous, 1 username and password, 2 clientid")
	flag.UintVar(&cfg.Auth.Datasource, "auth-ds", 0, "authentication datasource options:0 free, 1 redis, 2 mysql, 3 postgresql, 4 http")
	flag.StringVar(&cfg.Auth.ConfPath, "auth-path", "", "config file path should correspond to the auth-datasource")
	flag.StringVar(&cfg.Mqtt.TCP, "tcp", ":1883", "network address for mqtt tcp listener")
	flag.StringVar(&cfg.Mqtt.WS, "ws", ":1882", "network address for mqtt websocket listener")
	flag.StringVar(&cfg.Mqtt.HTTP, "http", ":8080", "network address for web info dashboard listener")
	flag.StringVar(&cfg.Cluster.NodeName, "node-name", "", "node name must be unique in the cluster")
	flag.StringVar(&cfg.Cluster.BindAddr, "bind-ip", "127.0.0.1", "the ip used for discovery and communication between nodes. It is usually set to the intranet ip addr.")
	flag.IntVar(&cfg.Cluster.BindPort, "gossip-port", 7946, "this port is used to discover nodes in a cluster")
	flag.IntVar(&cfg.Cluster.RaftPort, "raft-port", 8946, "this port is used for raft peer communication")
	flag.BoolVar(&cfg.Cluster.RaftBootstrap, "raft-bootstrap", false, "should be `true` for the first node of the cluster. It can elect a leader without any other nodes being present.")
	flag.StringVar(&cfg.Cluster.RaftLogLevel, "raft-log-level", "error", "Raft log level, with supported values debug, info, warn, error.")
	flag.StringVar(&members, "members", "", "seeds member list of cluster,such as 192.168.0.103:7946,192.168.0.104:7946")
	flag.BoolVar(&cfg.Cluster.GrpcEnable, "grpc-enable", false, "grpc is used for raft transport and reliable communication between nodes")
	flag.IntVar(&cfg.Cluster.GrpcPort, "grpc-port", 17946, "grpc communication port between nodes")
	flag.StringVar(&cfg.Redis.Options.Addr, "redis", "127.0.0.1:6379", "redis address for cluster mode")
	flag.StringVar(&cfg.Redis.Options.Password, "redis-pass", "", "redis password for cluster mode")
	flag.IntVar(&cfg.Redis.Options.DB, "redis-db", 0, "redis db for cluster mode")
	flag.BoolVar(&cfg.Log.Enable, "log-enable", true, "log enabled or not")
	flag.StringVar(&cfg.Log.Filename, "log-file", "./logs/comqtt.log", "log filename")
	flag.StringVar(&cfg.Cluster.NodesFileDir, "nodes-file-dir", "", "directory holds nodes.json assisting node discovery for cluster")
	//parse arguments
	flag.Parse()
	//load config file
	if len(confFile) > 0 {
		if cfg, err = config.Load(confFile); err != nil {
			return fmt.Errorf("load config file error: %w", err)
		}
	} else {
		if members != "" {
			cfg.Cluster.Members = strings.Split(members, ",")
		} else {
			cfg.Cluster.Members = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.Cluster.BindPort))}
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

	// init node and bind mqtt server
	if cfg.Cluster.Members == nil {
		onError(config.ErrClusterOpts, "members parameter etc")
	} else {
		initClusterNode(server, cfg)
	}

	// gen tls config
	var listenerConfig *listeners.Config
	if tlsConfig, err := config.GenTlsConfig(cfg); err != nil {
		onError(err, "gen tls config")
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
	csHls := csRt.New(agent).GenHandlers()
	mqHls := mqttRt.New(server).GenHandlers()
	maps.Copy(csHls, mqHls)
	http := listeners.NewHTTP("stats", cfg.Mqtt.HTTP, nil, csHls)
	onError(server.AddListener(http), "add http listener")

	errCh := make(chan error, 1)
	// start server
	go func() {
		err := server.Serve()
		if err != nil {
			errCh <- err
		}
	}()
	log.Info("cluster node started")

	// exit
	select {
	case err := <-errCh:
		onError(err, "server error")

	case <-ctx.Done():
		server.Log.Warn("caught signal, stopping...")
	}
	agent.Stop()
	server.Close()
	return nil
}

func initAuth(server *mqtt.Server, conf *config.Config) {
	logMsg := "init auth"
	if conf.Auth.Way == config.AuthModeAnonymous {
		server.AddHook(new(auth.AllowHook), nil)
	} else if conf.Auth.Way == config.AuthModeUsername || conf.Auth.Way == config.AuthModeClientid {
		ledger := auth.Ledger{}
		if conf.Auth.BlacklistPath != "" {
			onError(plugin.LoadYaml(conf.Auth.BlacklistPath, &ledger), logMsg)
		}
		switch conf.Auth.Datasource {
		case config.AuthDSRedis:
			opts := rauth.Options{}
			onError(plugin.LoadYaml(conf.Auth.ConfPath, &opts), logMsg)
			onError(server.AddHook(new(rauth.Auth), &opts), logMsg)
			opts.SetBlacklist(&ledger)
		case config.AuthDSMysql:
			opts := mauth.Options{}
			onError(plugin.LoadYaml(conf.Auth.ConfPath, &opts), logMsg)
			onError(server.AddHook(new(mauth.Auth), &opts), logMsg)
			opts.SetBlacklist(&ledger)
		case config.AuthDSPostgresql:
			opts := pauth.Options{}
			onError(plugin.LoadYaml(conf.Auth.ConfPath, &opts), logMsg)
			onError(server.AddHook(new(pauth.Auth), &opts), logMsg)
			opts.SetBlacklist(&ledger)
		case config.AuthDSHttp:
			opts := hauth.Options{}
			onError(plugin.LoadYaml(conf.Auth.ConfPath, &opts), logMsg)
			onError(server.AddHook(new(hauth.Auth), &opts), logMsg)
			opts.SetBlacklist(&ledger)
		}
	} else {
		onError(config.ErrAuthWay, logMsg)
	}
}

func initStorage(server *mqtt.Server, conf *config.Config) {
	logMsg := "init storage"
	if conf.StorageWay != config.StorageWayRedis {
		onError(config.ErrStorageWay, logMsg)
	}
	err := server.AddHook(new(coredis.Storage), &coredis.Options{
		HPrefix: conf.Redis.HPrefix,
		Options: &redis.Options{
			Addr:     conf.Redis.Options.Addr,
			DB:       conf.Redis.Options.DB,
			Username: conf.Redis.Options.Username,
			Password: conf.Redis.Options.Password,
		},
	})
	onError(err, logMsg)
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

func initClusterNode(server *mqtt.Server, conf *config.Config) {
	//setup member node
	agent = cs.NewAgent(&conf.Cluster)
	agent.BindMqttServer(server)
	onError(agent.Start(), "create node and join cluster")
	log.Info("cluster node created")
}

// onError handle errors and simplify code
func onError(err error, msg string) {
	if err != nil {
		log.Error(msg, "error", err)
		os.Exit(1)
	}
}
