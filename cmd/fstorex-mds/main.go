// fstorex-mds is the metadata server binary for the distributed file system (Raft cluster edition).
//
// Each fstorex-mds node serves two roles simultaneously:
//  1. Raft cluster member: participates in elections and log replication to guarantee metadata consistency
//  2. Business RPC service: provides metadata operation interfaces to clients/data nodes
//
// Usage (3-node cluster):
//
//	# Node 1
//	./fstorex-mds -id=localhost:9001 -peers=localhost:9002,localhost:9003 -waldir=/tmp/mds1/wal -snapdir=/tmp/mds1/snap
//	# Node 2
//	./fstorex-mds -id=localhost:9002 -peers=localhost:9001,localhost:9003 -waldir=/tmp/mds2/wal -snapdir=/tmp/mds2/snap
//	# Node 3
//	./fstorex-mds -id=localhost:9003 -peers=localhost:9001,localhost:9002 -waldir=/tmp/mds3/wal -snapdir=/tmp/mds3/snap
//
// Clients may connect to any node: write operations are automatically forwarded to the leader, while reads can be served by any node.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lwwgo/fstorex/internal/metadata"
	rafttypes "github.com/lwwgo/goraft/types"
)

func main() {
	id := flag.String("id", "localhost:9001", "this node's Raft ID, also the business RPC listen address (e.g. localhost:9001)")
	peersStr := flag.String("peers", "", "other node addresses, comma-separated (e.g. localhost:9002,localhost:9003)")
	walDir := flag.String("waldir", "/tmp/mds/wal", "Raft WAL log directory")
	snapDir := flag.String("snapdir", "/tmp/mds/snap", "Raft snapshot directory")
	maxIndexSpan := flag.Uint64("maxindexspan", 1000, "snapshot trigger threshold (take snapshot when log count exceeds this value)")
	flag.Parse()

	// Structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Parse peers
	var peers []string
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			p = strings.TrimSpace(p)
			if p != "" && p != *id {
				peers = append(peers, p)
			}
		}
	}

	// Ensure directories exist
	os.MkdirAll(*walDir, 0755)
	os.MkdirAll(*snapDir, 0755)

	// Build Raft configuration
	raftConfig := rafttypes.Config{
		LocalID:         *id,
		Peers:           peers,
		WalDir:          *walDir,
		SnapDir:         *snapDir,
		MaxIndexSpan:    *maxIndexSpan,
		ElectionTimeout: 3 * time.Second,
	}

	// Create the MDS integrated with Raft
	mds, err := metadata.NewMetadataServer(raftConfig, logger)
	if err != nil {
		logger.Error("failed to create metadata server", "error", err)
		os.Exit(1)
	}

	// Create a unified rpc.Server that registers both Raft internal and business services
	rpcServer := rpc.NewServer()

	// Register Raft internal service (used for election and log replication; service name must be "Server")
	if err := rpcServer.RegisterName("Server", mds.GetRaftNode()); err != nil {
		logger.Error("failed to register raft rpc service", "error", err)
		os.Exit(1)
	}

	// Register business service (accessed by clients/data nodes)
	if err := rpcServer.RegisterName("MetadataService", mds); err != nil {
		logger.Error("failed to register metadata rpc service", "error", err)
		os.Exit(1)
	}

	// Start TCP listener
	listener, err := net.Listen("tcp", *id)
	if err != nil {
		logger.Error("failed to listen", "addr", *id, "error", err)
		os.Exit(1)
	}
	logger.Info("metadata server (raft node) starting",
		"id", *id,
		"peers", peers,
		"wal_dir", *walDir,
		"snap_dir", *snapDir)
	fmt.Printf("Metadata Server (Raft) listening on %s\n", *id)

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
			go rpcServer.ServeConn(conn)
		}
	}()

	<-sigCh
	logger.Info("metadata server shutting down")
	listener.Close()
}
