// Package client_test 提供端到端集成测试，验证 FileHandle 全套能力。
// 测试方式：in-process 启动单节点 MDS（Raft）+ 单 DataNode，用 Client 连接。
package client_test

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lwwgo/fstorex/internal/client"
	"github.com/lwwgo/fstorex/internal/datanode"
	"github.com/lwwgo/fstorex/internal/metadata"
	rafttypes "github.com/lwwgo/goraft/types"
)

// testCluster 封装一个 in-process 启动的单节点集群。
type testCluster struct {
	mdsAddr string
	dnAddr  string
	cleanup func()
}

// startCluster 启动单节点 MDS + 单 DataNode，返回测试集群。
func startCluster(t *testing.T) *testCluster {
	t.Helper()

	// 拿随机端口
	mdsLn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen mds: %v", err)
	}
	mdsAddr := mdsLn.Addr().String()

	dnLn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen dn: %v", err)
	}
	dnAddr := dnLn.Addr().String()

	// 目录
	baseDir := t.TempDir()
	walDir := filepath.Join(baseDir, "mds-wal")
	snapDir := filepath.Join(baseDir, "mds-snap")
	dnDir := filepath.Join(baseDir, "dn-data")
	os.MkdirAll(walDir, 0755)
	os.MkdirAll(snapDir, 0755)
	os.MkdirAll(dnDir, 0755)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// 启动 MDS（单节点 Raft）
	raftConfig := rafttypes.Config{
		LocalID:         mdsAddr,
		Peers:           []string{mdsAddr}, // 单节点：需包含自己才能投票选举
		WalDir:          walDir,
		SnapDir:         snapDir,
		MaxIndexSpan:    1000,
		ElectionTimeout: 500 * time.Millisecond,
	}
	mds, err := metadata.NewMetadataServer(raftConfig, logger)
	if err != nil {
		t.Fatalf("create mds: %v", err)
	}

	mdsRPC := rpc.NewServer()
	mdsRPC.RegisterName("Server", mds.GetRaftNode())
	mdsRPC.RegisterName("MetadataService", mds)

	go func() {
		for {
			conn, err := mdsLn.Accept()
			if err != nil {
				return
			}
			go mdsRPC.ServeConn(conn)
		}
	}()

	// 先等 MDS 选出 leader（轮询直到能成功写）
	c := client.NewClient(mdsAddr, logger)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.Mkdir("/probe"); err == nil {
			c.Delete("/probe")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// leader 就绪后再启动 DataNode，确保第一次心跳就能成功注册
	dn, err := datanode.NewDataNode(dnDir, mdsAddr, dnAddr, logger)
	if err != nil {
		t.Fatalf("create dn: %v", err)
	}
	dnRPC := rpc.NewServer()
	if err := dnRPC.RegisterName("DataService", dn); err != nil {
		t.Fatalf("register dn rpc: %v", err)
	}

	go func() {
		for {
			conn, err := dnLn.Accept()
			if err != nil {
				return
			}
			go dnRPC.ServeConn(conn)
		}
	}()

	dn.StartHeartbeat()

	// 轮询等待 DataNode 注册成功
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		nodes, err := c.ListDataNodes()
		if err == nil && len(nodes) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return &testCluster{
		mdsAddr: mdsAddr,
		dnAddr:  dnAddr,
		cleanup: func() {
			mdsLn.Close()
			dnLn.Close()
		},
	}
}

func (tc *testCluster) newClient(t *testing.T) *client.Client {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return client.NewClient(tc.mdsAddr, logger)
}

// ===== 测试场景 =====

// 场景 1：空文件读（O_CREAT 后未写）返回 0 + EOF
func TestEmptyFileRead(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	fh, err := c.Open("/empty.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer fh.Close()

	buf := make([]byte, 100)
	n, err := fh.ReadAt(buf, 0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got err=%v n=%d", err, n)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// 场景 2：写后读一致
func TestWriteReadConsistency(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	// 写
	fh, err := c.Open("/hello.txt", client.O_CREAT|client.O_WRONLY)
	if err != nil {
		t.Fatalf("open for write: %v", err)
	}
	data := []byte("hello fstorex")
	n, err := fh.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("writeat: %v", err)
	}
	if n != len(data) {
		t.Fatalf("writeat: expected %d, got %d", len(data), n)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	// 读
	fh2, err := c.Open("/hello.txt", client.O_RDONLY)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer fh2.Close()

	buf := make([]byte, 100)
	n, err = fh2.ReadAt(buf, 0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got err=%v", err)
	}
	if string(buf[:n]) != string(data) {
		t.Fatalf("data mismatch: expected %q, got %q", data, buf[:n])
	}
}

// 场景 3：sparse write
func TestSparseWrite(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	fh, err := c.Open("/sparse.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer fh.Close()

	// 写 offset 100 的 "x"
	_, err = fh.WriteAt([]byte("x"), 100)
	if err != nil {
		t.Fatalf("writeat: %v", err)
	}

	// 读 offset 0 的 101 字节（刚好读完整个文件，EOF 可有可无）
	buf := make([]byte, 101)
	n, _ := fh.ReadAt(buf, 0)
	if n != 101 {
		t.Fatalf("expected 101 bytes, got %d", n)
	}
	// 前 100 字节应为 0
	for i := 0; i < 100; i++ {
		if buf[i] != 0 {
			t.Fatalf("byte %d should be 0, got %d", i, buf[i])
		}
	}
	if buf[100] != 'x' {
		t.Fatalf("byte 100 should be 'x', got %q", buf[100])
	}
}

// 场景 4：O_TRUNC 后 FileID 不变
func TestTruncateKeepsFileID(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	// 第一次写
	fh, err := c.Open("/trunc.txt", client.O_CREAT|client.O_WRONLY)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	fh.WriteAt([]byte("first content"), 0)
	fh.Close()

	info1, err := c.Stat("/trunc.txt")
	if err != nil {
		t.Fatalf("stat 1: %v", err)
	}
	fileID1 := info1.FileID
	if fileID1 == "" {
		t.Fatal("FileID should not be empty")
	}

	// O_TRUNC 重写
	fh2, err := c.Open("/trunc.txt", client.O_TRUNC|client.O_WRONLY)
	if err != nil {
		t.Fatalf("open trunc: %v", err)
	}
	fh2.WriteAt([]byte("second"), 0)
	fh2.Close()

	info2, err := c.Stat("/trunc.txt")
	if err != nil {
		t.Fatalf("stat 2: %v", err)
	}
	if info2.FileID != fileID1 {
		t.Fatalf("FileID changed after O_TRUNC: %s -> %s", fileID1, info2.FileID)
	}
	if info2.Size != 6 {
		t.Fatalf("expected size 6, got %d", info2.Size)
	}
}

// 场景 5：Truncate 缩小后读返回截断内容
func TestTruncateShrink(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	fh, err := c.Open("/shrink.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer fh.Close()

	// 写 100 字节
	data := make([]byte, 100)
	for i := range data {
		data[i] = 'A'
	}
	fh.WriteAt(data, 0)

	// 截断到 50
	if err := fh.Truncate(50); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// 读应只能读 50 字节
	buf := make([]byte, 100)
	n, err := fh.ReadAt(buf, 0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got err=%v", err)
	}
	if n != 50 {
		t.Fatalf("expected 50 bytes after truncate, got %d", n)
	}
}

// 场景 6：Rename 后旧 fd 仍可读
func TestRenameOldFDValid(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	// 打开并写
	fh, err := c.Open("/old.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	data := []byte("persist after rename")
	fh.WriteAt(data, 0)

	// rename
	if err := c.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}

	// 旧 fd 仍可读
	buf := make([]byte, 100)
	n, err := fh.ReadAt(buf, 0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got err=%v", err)
	}
	if string(buf[:n]) != string(data) {
		t.Fatalf("old fd read mismatch: expected %q, got %q", data, buf[:n])
	}

	// 新路径也能读
	fh2, err := c.Open("/new.txt", client.O_RDONLY)
	if err != nil {
		t.Fatalf("open new path: %v", err)
	}
	defer fh2.Close()
	n2, _ := fh2.ReadAt(buf, 0)
	if string(buf[:n2]) != string(data) {
		t.Fatalf("new path read mismatch: expected %q, got %q", data, buf[:n2])
	}

	fh.Close()
}

// 场景 7：并发随机读写一致性
// 多个 goroutine 对同一文件做分区不重叠的随机写 + 全局随机读，
// 实时维护本地 reference buffer，最后全量读回与 reference 逐字节比对。
// 注意：写操作分区不重叠（避免 last-write-wins 导致的 reference 乱序），
// 读操作可读任意区域。
func TestConcurrentRandomReadWrite(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	const fileSize = 1 << 20 // 1MB
	const numGoroutines = 20
	const opsPerGoroutine = 50
	const maxBlockSize = 4096

	// 预填充 reference buffer
	reference := make([]byte, fileSize)
	for i := range reference {
		reference[i] = byte(i % 256)
	}

	// 打开文件写入初始数据
	fh, err := c.Open("/concurrent.bin", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := fh.WriteAt(reference, 0); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	// 每个 goroutine 负责一个不重叠的写区域
	regionSize := int64(fileSize / numGoroutines)
	var refMu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)*1000 + time.Now().UnixNano()))
			buf := make([]byte, maxBlockSize)
			regionStart := int64(id) * regionSize
			regionEnd := regionStart + regionSize
			if id == numGoroutines-1 {
				regionEnd = fileSize // 最后一个 goroutine 负责余数
			}

			for op := 0; op < opsPerGoroutine; op++ {
				if rng.Intn(2) == 0 {
					// Write：只写自己的区域，不重叠
					offset := regionStart + rng.Int63n(regionEnd-regionStart)
					maxWrite := int(regionEnd - offset)
					size := rng.Intn(maxBlockSize) + 1
					if size > maxWrite {
						size = maxWrite
					}
					data := make([]byte, size)
					for i := range data {
						data[i] = byte(rng.Intn(256))
					}
					n, err := fh.WriteAt(data, offset)
					if err != nil {
						errCh <- fmt.Errorf("goroutine %d write at offset %d: %w", id, offset, err)
						return
					}
					if n != len(data) {
						errCh <- fmt.Errorf("goroutine %d short write: %d != %d", id, n, len(data))
						return
					}
					refMu.Lock()
					copy(reference[offset:offset+int64(size)], data)
					refMu.Unlock()
				} else {
					// Read：读任意区域，验证不报错（不做实时比对，
					// 因为读和并发写之间没有 happens-before 关系）
					offset := rng.Int63n(fileSize)
					size := rng.Intn(maxBlockSize) + 1
					if offset+int64(size) > fileSize {
						size = int(fileSize - offset)
					}
					if size == 0 {
						continue
					}
					n, err := fh.ReadAt(buf[:size], offset)
					if err != nil && err != io.EOF {
						errCh <- fmt.Errorf("goroutine %d read at offset %d: %w", id, offset, err)
						return
					}
					if n == 0 && err != io.EOF {
						errCh <- fmt.Errorf("goroutine %d read 0 bytes without EOF at offset %d", id, offset)
						return
					}
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	if err := fh.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 全量读回校验
	fh2, err := c.Open("/concurrent.bin", client.O_RDONLY)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer fh2.Close()

	finalBuf := make([]byte, fileSize)
	n, err := fh2.ReadAt(finalBuf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("final read: %v", err)
	}
	if n != fileSize {
		t.Fatalf("final read short: %d != %d", n, fileSize)
	}

	refMu.Lock()
	match := bytes.Equal(finalBuf, reference)
	refMu.Unlock()
	if !match {
		for i := 0; i < fileSize; i++ {
			if finalBuf[i] != reference[i] {
				t.Fatalf("final data mismatch at byte %d: expected %d, got %d", i, reference[i], finalBuf[i])
			}
		}
	}
}

// 场景 8：并发 sparse write 不破坏零填充区域
// 多个 goroutine 同时向带 gap 的 offset 写数据，验证 sparse 区域保持为 0。
func TestConcurrentSparseWrite(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	const totalSize = 4 << 20 // 4MB
	const numGoroutines = 20
	const blockSize = 64 * 1024 // 每个 goroutine 写 64KB
	const gapSize = 64 * 1024   // 每个 block 之间留 64KB sparse gap

	fh, err := c.Open("/sparse_concurrent.bin", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// 先扩展到 totalSize，确保尾部未写区域存在（为 0）
	if err := fh.Truncate(totalSize); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// 每个 goroutine 写一段带 gap 的不重叠区域
	// block i 写在 offset = i * (blockSize + gapSize)
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			offset := int64(id) * (blockSize + gapSize)
			data := make([]byte, blockSize)
			// 用 goroutine ID 填充，便于验证
			for i := range data {
				data[i] = byte(id)
			}
			n, err := fh.WriteAt(data, offset)
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d write at %d: %w", id, offset, err)
				return
			}
			if n != len(data) {
				errCh <- fmt.Errorf("goroutine %d short write: %d != %d", id, n, len(data))
				return
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	if err := fh.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 全量读回校验
	fh2, err := c.Open("/sparse_concurrent.bin", client.O_RDONLY)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer fh2.Close()

	finalBuf := make([]byte, totalSize)
	n, err := fh2.ReadAt(finalBuf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	if n != totalSize {
		t.Fatalf("read short: %d != %d", n, totalSize)
	}

	// 校验每个 block 的内容，以及 block 之间的 gap 区域为 0
	for g := 0; g < numGoroutines; g++ {
		blockOffset := int64(g) * (blockSize + gapSize)
		// 校验 block 内容
		for i := 0; i < blockSize; i++ {
			if finalBuf[blockOffset+int64(i)] != byte(g) {
				t.Fatalf("block %d byte %d mismatch: expected %d, got %d", g, i, g, finalBuf[blockOffset+int64(i)])
			}
		}
		// 校验 gap 区域为 0（最后一个 block 后面的 gap 也要校验）
		gapStart := blockOffset + blockSize
		gapEnd := gapStart + gapSize
		if gapEnd > totalSize {
			gapEnd = totalSize
		}
		for i := gapStart; i < gapEnd; i++ {
			if finalBuf[i] != 0 {
				t.Fatalf("gap byte %d should be 0, got %d", i, finalBuf[i])
			}
		}
	}

	// 校验尾部未写区域为 0
	lastBlockEnd := int64(numGoroutines-1)*(blockSize+gapSize) + blockSize
	for i := lastBlockEnd; i < totalSize; i++ {
		if finalBuf[i] != 0 {
			t.Fatalf("tail byte %d should be 0, got %d", i, finalBuf[i])
		}
	}
}
