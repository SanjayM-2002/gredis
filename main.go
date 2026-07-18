package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/SanjayM-2002/gredis/config"
	"github.com/SanjayM-2002/gredis/server"
)

func setupFlags() {
	flag.StringVar(&config.Host, "host", "0.0.0.0", "host for the gredis server")
	flag.IntVar(&config.Port, "port", 6363, "port for the gredis server")
	flag.Parse()
}

func main() {
	setupFlags()
	log.Println("starting gredis")

	var sigs chan os.Signal = make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		if err := server.RunAsyncTCPServer(&wg); err != nil {
			log.Fatal(err)
		}
	}()
	go server.WaitForSignal(&wg, sigs)

	wg.Wait()
}
