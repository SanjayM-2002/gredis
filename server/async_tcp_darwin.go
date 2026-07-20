package server

import (
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SanjayM-2002/gredis/config"
	"github.com/SanjayM-2002/gredis/core"
)

var cronFrequency time.Duration = 1 * time.Second
var lastCronExecTime time.Time = time.Now()

const EngineStatus_WAITING int32 = 1 << 1
const EngineStatus_BUSY int32 = 1 << 2
const EngineStatus_SHUTTING_DOWN int32 = 1 << 3
const EngineStatus_TRANSACTION int32 = 1 << 4

var eStatus int32 = EngineStatus_WAITING

var connectedClients map[int]*core.Client

func init() {
	connectedClients = make(map[int]*core.Client)
}

func WaitForSignal(wg *sync.WaitGroup, sigs chan os.Signal) {
	defer wg.Done()
	<-sigs

	// if server is busy continue to wait
	for atomic.LoadInt32(&eStatus) == EngineStatus_BUSY {
	}

	// CRITICAL TO HANDLE
	// We do not want server to ever go back to BUSY state
	// when control flow is here ->

	// immediately set the status to be SHUTTING DOWN
	// the only place where we can set the status to be SHUTTING DOWN
	atomic.StoreInt32(&eStatus, EngineStatus_SHUTTING_DOWN)

	// if server is in any other state, initiate a shutdown
	core.Shutdown()
	os.Exit(0)
}

func RunAsyncTCPServer(wg *sync.WaitGroup) error {
	defer wg.Done()
	defer func() {
		atomic.StoreInt32(&eStatus, EngineStatus_SHUTTING_DOWN)
	}()

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

	// loop until the server is not shutting down
	for atomic.LoadInt32(&eStatus) != EngineStatus_SHUTTING_DOWN {
		if time.Now().After(lastCronExecTime.Add(cronFrequency)) {
			core.DeleteExpiredKeys()
			lastCronExecTime = time.Now()
		}

		// Say, the Engine triggered SHUTTING down when the control flow is here ->
		// Current: Engine status == WAITING
		// Update: Engine status = SHUTTING_DOWN
		// Then we have to exit (handled in Signal Handler)

		// see if any FD is ready for an IO
		// (Kevent with no change list and an event list waits; nil timeout = wait forever)
		nevents, e := syscall.Kevent(kqFD, nil, events[:], nil)
		if e != nil {
			continue
		}

		// Here, we do not want server to go back from SHUTTING DOWN
		// to BUSY
		// If the engine status == SHUTTING_DOWN over here ->
		// We have to exit
		// hence the only legal transitiion is from WAITING to BUSY
		// if that does not happen then we can exit.

		// mark engine as BUSY only when it is in the waiting state
		if !atomic.CompareAndSwapInt32(&eStatus, EngineStatus_WAITING, EngineStatus_BUSY) {
			// if swap unsuccessful then the existing status is not WAITING, but something else
			switch eStatus {
			case EngineStatus_SHUTTING_DOWN:
				return nil
			}
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

				connectedClients[fd] = core.NewClient(fd)
				syscall.SetNonblock(fd, true)
				log.Println("client connected on fd", fd, "concurrent clients", len(connectedClients))

				// add this new TCP connection to be monitored
				var socketClientEvent syscall.Kevent_t
				syscall.SetKevent(&socketClientEvent, fd, syscall.EVFILT_READ, syscall.EV_ADD)
				if _, err := syscall.Kevent(kqFD, []syscall.Kevent_t{socketClientEvent}, nil, nil); err != nil {
					log.Fatal(err)
				}
			} else {
				comm := connectedClients[int(events[i].Ident)]
				if comm == nil {
					continue
				}
				cmds, err := readCommands(comm)
				if err != nil {
					syscall.Close(int(events[i].Ident))
					delete(connectedClients, int(events[i].Ident))
					log.Println("client disconnected on fd", events[i].Ident, "concurrent clients", len(connectedClients))
					continue
				}
				respond(cmds, comm)
			}
		}

		// mark engine as WAITING
		// no contention as the signal handler is blocked until
		// the engine is BUSY
		atomic.StoreInt32(&eStatus, EngineStatus_WAITING)
	}

	return nil
}
