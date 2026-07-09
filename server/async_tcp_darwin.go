package server

import (
	"log"
	"net"
	"syscall"
	"time"

	"github.com/SanjayM-2002/gredis/config"
	"github.com/SanjayM-2002/gredis/core"
)

var con_clients int = 0
var cronFrequency time.Duration = 1 * time.Second
var lastCronExecTime time.Time = time.Now()

func RunAsyncTCPServer() error {
	log.Println("starting an asynchronous TCP server (kqueue) on", config.Host, config.Port)

	max_clients := 20000

	// Create KQUEUE Event Objects to hold events
	var events []syscall.Kevent_t = make([]syscall.Kevent_t, max_clients)

	// Create a socket
	// (macOS has no SOCK_NONBLOCK flag, so non-blocking is set separately below)
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(serverFD)

	// Allow restarting the server immediately on the same port
	if err = syscall.SetsockoptInt(serverFD, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return err
	}

	// Set the Socket operate in a non-blocking mode
	if err = syscall.SetNonblock(serverFD, true); err != nil {
		return err
	}

	// Bind the IP and the port
	ip4 := net.ParseIP(config.Host).To4()
	if err = syscall.Bind(serverFD, &syscall.SockaddrInet4{
		Port: config.Port,
		Addr: [4]byte{ip4[0], ip4[1], ip4[2], ip4[3]},
	}); err != nil {
		return err
	}

	// Start listening
	if err = syscall.Listen(serverFD, max_clients); err != nil {
		return err
	}

	// AsyncIO starts here!!

	// creating KQUEUE instance
	kqFD, err := syscall.Kqueue()
	if err != nil {
		log.Fatal(err)
	}
	defer syscall.Close(kqFD)

	// Specify the events we want to get hints about
	// and set the socket on which
	var socketServerEvent syscall.Kevent_t
	syscall.SetKevent(&socketServerEvent, serverFD, syscall.EVFILT_READ, syscall.EV_ADD)

	// Listen to read events on the Server itself
	// (Kevent with a change list and no event list only registers, it does not wait)
	if _, err = syscall.Kevent(kqFD, []syscall.Kevent_t{socketServerEvent}, nil, nil); err != nil {
		return err
	}

	for {
		if time.Now().After(lastCronExecTime.Add(cronFrequency)) {
			core.DeleteExpiredKeys()
			lastCronExecTime = time.Now()
		}

		// see if any FD is ready for an IO
		// (Kevent with no change list and an event list waits; nil timeout = wait forever)
		nevents, e := syscall.Kevent(kqFD, nil, events[:], nil)
		if e != nil {
			continue
		}

		for i := 0; i < nevents; i++ {
			// if the socket server itself is ready for an IO
			if int(events[i].Ident) == serverFD {
				// accept the incoming connection from a client
				fd, _, err := syscall.Accept(serverFD)
				if err != nil {
					log.Println("err", err)
					continue
				}

				// increase the number of concurrent clients count
				con_clients++
				syscall.SetNonblock(fd, true)
				log.Println("client connected on fd", fd, "concurrent clients", con_clients)

				// add this new TCP connection to be monitored
				var socketClientEvent syscall.Kevent_t
				syscall.SetKevent(&socketClientEvent, fd, syscall.EVFILT_READ, syscall.EV_ADD)
				if _, err := syscall.Kevent(kqFD, []syscall.Kevent_t{socketClientEvent}, nil, nil); err != nil {
					log.Fatal(err)
				}
			} else {
				comm := core.FDComm{Fd: int(events[i].Ident)}
				cmds, err := readCommands(comm)
				if err != nil {
					syscall.Close(int(events[i].Ident))
					con_clients -= 1
					log.Println("client disconnected on fd", events[i].Ident, "concurrent clients", con_clients)
					continue
				}
				respond(cmds, comm)
			}
		}
	}
}
