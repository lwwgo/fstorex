// fstorex-dn is the data node binary for the distributed file system.
//
// Usage:
//
//	./fstorex-dn -addr=:9101 -mds=localhost:9001 -datadir=/tmp/dn1
//
// Responsibility: stores actual file content and registers itself with MDS on startup.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"

	"github.com/lwwgo/fstorex/internal/datanode"
)

func main() {
	addr := flag.String("addr", ":9101", "RPC listen address")
	mdsAddr := flag.String("mds", "localhost:9001", "metadata server address")
	dataDir := flag.String("datadir", "/tmp/dn1", "local data directory")
	flag.Parse()

	// Structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Create the data node
	dn, err := datanode.NewDataNode(*dataDir, *mdsAddr, *addr, logger)
	if err != nil {
		logger.Error("failed to create data node", "error", err)
		os.Exit(1)
	}

	// Register RPC service
	if err := dn.RegisterRPC(); err != nil {
		logger.Error("failed to register RPC", "error", err)
		os.Exit(1)
	}

	// Start TCP listener
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("failed to listen", "addr", *addr, "error", err)
		os.Exit(1)
	}
	logger.Info("data node starting", "addr", *addr, "mds", *mdsAddr, "data_dir", *dataDir)
	fmt.Printf("Data Node listening on %s\n", *addr)

	// Start heartbeat goroutine: first heartbeat registers, periodic heartbeats keep alive.
	// If MDS is not ready yet, the first heartbeat fails silently and retries on next tick.
	dn.StartHeartbeat()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-sigCh:
					return
				default:
					continue
				}
			}
			go rpc.ServeConn(conn)
		}
	}()

	<-sigCh
	logger.Info("data node shutting down")
	listener.Close()
}
