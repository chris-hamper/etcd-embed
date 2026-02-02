package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
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
		log.Fatalln("Failed to start etcd:", err)
	}
	defer e.Close()

	// zap := e.GetLogger()

	select {
	case <-e.Server.ReadyNotify():
		log.Printf("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		log.Printf("Server took too long to start!")
	}

	ctx := context.Background()
	c := v3client.New(e.Server)

	time.Sleep(3*time.Second)
	log.Println("Leader:", e.Server.Leader().String())

	if nodeID == "node0" {
		log.Println("Putting keys...")

		wg := &sync.WaitGroup{}
		mut := &sync.Mutex{}
		var elapsed time.Duration

		// Spawn 100 workers that each Put 1000 random keys
		start := time.Now()
		for range 100 {
			wg.Go(func() {
				for range 1000 {
					key := strconv.FormatInt(int64(rand.IntN(1000)), 10)
					value := strconv.FormatInt(int64(rand.IntN(1000000)), 10)

					start := time.Now()
					c.KV.Put(ctx, key, value)
					end := time.Now()

					mut.Lock()
					elapsed += end.Sub(start)
					mut.Unlock()
				}
			})
		}

		wg.Wait()
		log.Println("Done putting keys. Wall clock elapsed:", time.Since(start), "\nAvg Put:", elapsed / 100000)
	}

	tick := time.NewTicker(time.Second)

	// Block the main goroutine until a signal is received on the 'signals' channel, or the server fails
	var done bool
	for !done {
		select {
		case sig := <-signals:
			log.Printf("\nReceived signal: %s. Exiting gracefully.\n", sig)
			done = true

		case err := <-e.Err():
			log.Printf("\nServer failed: %s\n", err)
			done = true
		// case <-tick.C:
		// 	resp, err := c.KV.Get(ctx, "0")
		// 	if err != nil {
		// 		log.Println("Failed to read key:", err)
		// 	} else if resp.Count > 0 {
		// 		log.Printf("[0] = %s\n    Response: %+v", string(resp.Kvs[0].Value), resp)
		// 	} else {
		// 		log.Println("No value for [0]")
		// 	}
		}
	}
	tick.Stop()

	e.Server.Stop()
}
