// fstorex is the unified client CLI for FStoreX distributed file system.
//
// Usage:
//
//	./fstorex -mds=localhost:9001 <command> [args]
//
// Commands:
//
//	mkdir <path>                    Create directory
//	put [-overwrite] <local> <remote> Upload file
//	get <remote> <local>            Download file
//	ls [path]                       List directory (default: /)
//	stat <path>                     Show file/directory info
//	rm <path>                       Delete file/directory
//	replicas <path>                 Show file's replica locations
//	nodes                           List registered data nodes
//	gc                              Trigger orphan data garbage collection
//	mount <mountpoint>              Mount FStoreX as local filesystem (FUSE)
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/lwwgo/fstorex/internal/client"
	"github.com/lwwgo/fstorex/internal/fuse"
	"github.com/lwwgo/fstorex/internal/types"
)

func main() {
	mdsAddr := flag.String("mds", "localhost:9001", "metadata server address")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	// Structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	c := client.NewClient(*mdsAddr, logger)

	cmd := args[0]

	switch cmd {
	case "mkdir":
		if len(args) < 2 {
			fmt.Println("Usage: client mkdir <path>")
			os.Exit(1)
		}
		if err := c.Mkdir(args[1]); err != nil {
			logger.Error("mkdir failed", "error", err)
			os.Exit(1)
		}
		fmt.Printf("✓ Directory created: %s\n", args[1])

	case "put":
		fs := flag.NewFlagSet("put", flag.ExitOnError)
		overwrite := fs.Bool("overwrite", false, "overwrite if file already exists")
		fs.Parse(args[1:])
		putArgs := fs.Args()
		if len(putArgs) < 2 {
			fmt.Println("Usage: client put [-overwrite] <local> <remote>")
			os.Exit(1)
		}
		var putOpts []client.PutOption
		if *overwrite {
			putOpts = append(putOpts, client.WithCreateMode(types.OverwriteIfExists))
		}
		if err := c.PutFile(putArgs[0], putArgs[1], putOpts...); err != nil {
			logger.Error("put failed", "error", err)
			os.Exit(1)
		}
		fmt.Printf("✓ File uploaded: %s → %s\n", putArgs[0], putArgs[1])

	case "get":
		if len(args) < 3 {
			fmt.Println("Usage: client get <remote> <local>")
			os.Exit(1)
		}
		if err := c.GetFile(args[1], args[2]); err != nil {
			logger.Error("get failed", "error", err)
			os.Exit(1)
		}
		fmt.Printf("✓ File downloaded: %s → %s\n", args[1], args[2])

	case "ls":
		path := "/"
		if len(args) >= 2 {
			path = args[1]
		}
		infos, err := c.ListDir(path)
		if err != nil {
			logger.Error("ls failed", "error", err)
			os.Exit(1)
		}
		if len(infos) == 0 {
			fmt.Println("(empty)")
			return
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tSIZE\tMODIFIED\tTYPE")
		for _, info := range infos {
			typ := "FILE"
			if info.IsDir {
				typ = "DIR"
			}
			fmt.Fprintf(w, "%s\t%d\t%s\t%s\n", info.Name, info.Size, info.ModTime.Format(time.RFC3339), typ)
		}
		w.Flush()

	case "stat":
		if len(args) < 2 {
			fmt.Println("Usage: client stat <path>")
			os.Exit(1)
		}
		info, err := c.Stat(args[1])
		if err != nil {
			logger.Error("stat failed", "error", err)
			os.Exit(1)
		}
		fmt.Printf("Name:      %s\n", info.Name)
		fmt.Printf("Path:      %s\n", info.Path)
		fmt.Printf("Type:      %s\n", map[bool]string{true: "Directory", false: "File"}[info.IsDir])
		fmt.Printf("Size:      %d bytes\n", info.Size)
		fmt.Printf("Created:   %s\n", info.CreatedAt.Format(time.RFC3339))
		fmt.Printf("Modified:  %s\n", info.ModTime.Format(time.RFC3339))

	case "rm":
		if len(args) < 2 {
			fmt.Println("Usage: client rm <path>")
			os.Exit(1)
		}
		if err := c.Delete(args[1]); err != nil {
			logger.Error("rm failed", "error", err)
			os.Exit(1)
		}
		fmt.Printf("✓ Path deleted: %s\n", args[1])

	case "replicas":
		if len(args) < 2 {
			fmt.Println("Usage: client replicas <path>")
			os.Exit(1)
		}
		replicas, err := c.GetReplicas(args[1])
		if err != nil {
			logger.Error("replicas failed", "error", err)
			os.Exit(1)
		}
		fmt.Printf("Replicas for %s (%d total):\n", args[1], len(replicas))
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(w, "#\tDATA NODE\tREMOTE PATH")
		for i, r := range replicas {
			fmt.Fprintf(w, "%d\t%s\t%s\n", i+1, r.Addr, r.RemotePath)
		}
		w.Flush()

	case "nodes":
		nodes, err := c.ListDataNodes()
		if err != nil {
			logger.Error("list nodes failed", "error", err)
			os.Exit(1)
		}
		fmt.Printf("Data nodes (%d):\n", len(nodes))
		for _, n := range nodes {
			fmt.Printf("  - %s\n", n)
		}

	case "gc":
		if err := c.TriggerGC(); err != nil {
			logger.Error("trigger GC failed", "error", err)
			os.Exit(1)
		}
		fmt.Println("✓ GC triggered in background (check MDS logs for results)")

	case "mount":
		if len(args) < 2 {
			fmt.Println("Usage: fstorex -mds=<addr> mount <mountpoint>")
			os.Exit(1)
		}
		mountPoint := args[1]
		// 检查挂载点存在
		if info, err := os.Stat(mountPoint); err != nil || !info.IsDir() {
			fmt.Fprintf(os.Stderr, "Error: mount point %s does not exist or is not a directory\n", mountPoint)
			os.Exit(1)
		}
		// 挂载（阻塞直到收到信号）
		unmount, err := fuse.Mount(*mdsAddr, mountPoint, logger)
		if err != nil {
			logger.Error("mount failed", "error", err)
			os.Exit(1)
		}
		logger.Info("FStoreX mounted", "mount_point", mountPoint, "mds", *mdsAddr)
		logger.Info("Press Ctrl+C to unmount")
		// 等待信号优雅卸载
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("received shutdown signal, unmounting...")
		if err := unmount(); err != nil {
			logger.Error("unmount failed", "error", err)
			os.Exit(1)
		}
		logger.Info("unmounted successfully")
		return

	default:
		fmt.Printf("Unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("FStoreX Client")
	fmt.Println()
	fmt.Println("Usage: fstorex -mds=<addr> <command> [args]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  mkdir <path>                    Create directory")
	fmt.Println("  put [-overwrite] <local> <remote> Upload file (-overwrite to overwrite existing)")
	fmt.Println("  get <remote> <local>            Download file")
	fmt.Println("  ls [path]                       List directory (default: /)")
	fmt.Println("  stat <path>                     Show file/directory info")
	fmt.Println("  rm <path>                       Delete file/directory")
	fmt.Println("  replicas <path>                 Show file's replica locations")
	fmt.Println("  nodes                           List registered data nodes")
	fmt.Println("  gc                              Trigger orphan data garbage collection")
	fmt.Println("  mount <mountpoint>              Mount FStoreX as local filesystem (FUSE)")
}
