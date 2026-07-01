package main

import (
	"flag"
	"log"

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
	if err := server.RunAsyncTCPServer(); err != nil {
		log.Fatal(err)
	}
}
