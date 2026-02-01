package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
)

// Command line parameters
var (
	nodeID   string
	lClient string
	// aClient string
	lPeer string
	// aPeer string
	cluster string
)

func init() {
	flag.StringVar(&nodeID, "id", "default", "Node ID")
	flag.StringVar(&lClient, "lclient", "http://127.0.0.1:2379", "")
	// flag.StringVar(&aClient, "aclient", "", "")
	flag.StringVar(&lPeer, "lpeer", "http://127.0.0.1:2380", "")
	// flag.StringVar(&aPeer, "apeer", "", "")
	flag.StringVar(&cluster, "cluster", "default=http://127.0.0.1:2380", "")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	cfg := embed.NewConfig()
	cfg.Name = nodeID
	cfg.Dir = nodeID + ".etcd"
	cfg.WalDir = cfg.Dir + "/wal"
	cfg.EnablePprof = true
	cfg.LogOutputs = []string{"stderr"}

	u, err := url.Parse(lClient)
	if err != nil {
		log.Fatalln(err)
	}
	cfg.ListenClientUrls = []url.URL{*u}
	cfg.AdvertiseClientUrls = []url.URL{*u}

	u, err = url.Parse(lClient)
	if err != nil {
		log.Fatalln(err)
	}

	u, err = url.Parse(lPeer)
	if err != nil {
		log.Fatalln(err)
	}
	cfg.ListenPeerUrls = []url.URL{*u}
	cfg.AdvertisePeerUrls = []url.URL{*u}

	cfg.InitialCluster = cluster

	// log.Printf("cfg: %+v", cfg)

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	select {
	case <-e.Server.ReadyNotify():
		log.Printf("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		log.Printf("Server took too long to start!")
	}

	log.Println("Put key")
	e.Server.KV().Put([]byte("key"), []byte("value"), lease.LeaseID(0))

	result, err := e.Server.KV().Range(context.TODO(), []byte("key"), nil, mvcc.RangeOptions{Limit: 1})
	if err != nil {
		log.Println("Failed to read key:", err)
	} else {
		log.Println("Read key:", string(result.KVs[0].Value))
	}

	// Block the main goroutine until a signal is received on the 'signals' channel, or the server fails
	select {
	case sig := <-signals:
		log.Printf("\nReceived signal: %s. Exiting gracefully.\n", sig)
	case err := <-e.Err():
		log.Printf("\nServer failed: %s\n", err)
	}

	e.Server.Stop()
}
