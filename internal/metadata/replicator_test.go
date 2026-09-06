package metadata

import (
	"bytes"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lwwgo/fstorex/internal/client"
	"github.com/lwwgo/fstorex/internal/datanode"
	rafttypes "github.com/lwwgo/goraft/types"
)

// clusterDN 表示一个测试 DataNode 实例。
type clusterDN struct {
	dn       *datanode.DataNode
	listener net.Listener
	addr     string
}

// replicatorTestCluster 封装 MDS + 多个 DataNode 的测试集群。
// 白盒测试（metadata 包内），可直接访问 mds 实例触发补副本。
type replicatorTestCluster struct {
	mds     *MetadataServer
	mdsLn   net.Listener
	mdsAddr string
	dns     []*clusterDN
	logger  *slog.Logger
}

// startReplicatorCluster 启动 MDS + n 个 DataNode。
// heartbeatTimeout 设置为 100ms 以加速故障检测；
// 补副本通过手动调用 mds.runReplication() 触发，不依赖定时器。
func startReplicatorCluster(t *testing.T, n int) *replicatorTestCluster {
	t.Helper()
	if n < 1 {
		t.Fatalf("need at least 1 data node")
	}

	baseDir := t.TempDir()
	walDir := filepath.Join(baseDir, "mds-wal")
	snapDir := filepath.Join(baseDir, "mds-snap")
	os.MkdirAll(walDir, 0755)
	os.MkdirAll(snapDir, 0755)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// MDS listener
	mdsLn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen mds: %v", err)
	}
	mdsAddr := mdsLn.Addr().String()

	// 启动 MDS（单节点 Raft）
	raftConfig := rafttypes.Config{
		LocalID:         mdsAddr,
		Peers:           []string{mdsAddr},
		WalDir:          walDir,
		SnapDir:         snapDir,
		MaxIndexSpan:    1000,
		ElectionTimeout: 300 * time.Millisecond,
	}
	mds, err := NewMetadataServer(raftConfig, logger)
	if err != nil {
		t.Fatalf("create mds: %v", err)
	}
	// 测试加速：缩短心跳超时
	mds.heartbeatTimeout = 100 * time.Millisecond
	// replicateInterval 设大，避免后台 goroutine 疯狂提交 Raft op；
	// 测试中通过手动调用 runReplication() 触发补副本。
	mds.replicateInterval = 30 * time.Second

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

	// 等 MDS 选出 leader
	c := client.NewClient(mdsAddr, logger)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.Mkdir("/probe"); err == nil {
			c.Delete("/probe")
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 启动 n 个 DataNode
	dns := make([]*clusterDN, 0, n)
	for i := 0; i < n; i++ {
		dnLn, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("listen dn%d: %v", i, err)
		}
		dnAddr := dnLn.Addr().String()
		dnDir := filepath.Join(baseDir, "dn-data", dnAddr)
		os.MkdirAll(dnDir, 0755)

		dn, err := datanode.NewDataNode(dnDir, mdsAddr, dnAddr, logger)
		if err != nil {
			t.Fatalf("create dn%d: %v", i, err)
		}
		// 测试加速：缩短 DN 心跳间隔，让存活 DN 持续刷新心跳
		dn.HeartbeatInterval = 50 * time.Millisecond
		dnRPC := rpc.NewServer()
		if err := dnRPC.RegisterName("DataService", dn); err != nil {
			t.Fatalf("register dn%d rpc: %v", i, err)
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

		dns = append(dns, &clusterDN{dn: dn, listener: dnLn, addr: dnAddr})
	}

	// 轮询等待所有 DataNode 注册成功
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		nodes, err := c.ListDataNodes()
		if err == nil && len(nodes) >= n {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return &replicatorTestCluster{
		mds:     mds,
		mdsLn:   mdsLn,
		mdsAddr: mdsAddr,
		dns:     dns,
		logger:  logger,
	}
}

func (tc *replicatorTestCluster) cleanup() {
	for _, dn := range tc.dns {
		dn.dn.Stop()
		dn.listener.Close()
	}
	tc.mdsLn.Close()
}

func (tc *replicatorTestCluster) newClient() *client.Client {
	return client.NewClient(tc.mdsAddr, tc.logger)
}

// killDN 停止指定 DataNode：停止心跳 + 关闭 RPC listener，模拟节点故障。
func (tc *replicatorTestCluster) killDN(index int) {
	dn := tc.dns[index]
	dn.dn.Stop()
	dn.listener.Close()
}

// TestAutoReplication_SingleNodeFailure 验证单个 DataNode 故障后自动补副本。
// 流程：3 DN + RF=3 → 创建文件写数据 → 杀 DN2 → 手动触发补副本 → 验证 RF 恢复且数据一致。
func TestAutoReplication_SingleNodeFailure(t *testing.T) {
	tc := startReplicatorCluster(t, 4) // 4 DN，杀 1 个后仍有 3 个存活，可恢复 RF=3
	defer tc.cleanup()

	tc.mds.replicaCount = 3
	c := tc.newClient()

	// 1. 创建文件并写入数据
	content := bytes.Repeat([]byte("FSTOREX-REPLICA-TEST-"), 100) // ~2MB
	fh, err := c.Open("/replicate-test.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := fh.WriteAt(content, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 2. 验证文件有 3 个副本
	tc.mds.mu.RLock()
	e := tc.mds.lookup("/replicate-test.txt")
	tc.mds.mu.RUnlock()
	if e == nil {
		t.Fatal("file not found in metadata")
	}
	if len(e.replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(e.replicas))
	}
	originalReplicas := make([]string, len(e.replicas))
	for i, r := range e.replicas {
		originalReplicas[i] = r.Addr
	}
	t.Logf("initial replicas: %v", originalReplicas)

	// 3. 杀掉持有该文件副本的一个 DataNode（确保 deadDNAddr 在副本列表中）
	deadDNAddr := originalReplicas[0]
	killIndex := -1
	for i, dn := range tc.dns {
		if dn.addr == deadDNAddr {
			killIndex = i
			break
		}
	}
	if killIndex < 0 {
		t.Fatalf("cannot find DN %s in cluster", deadDNAddr)
	}
	tc.killDN(killIndex)
	t.Logf("killed DN at %s (index %d)", deadDNAddr, killIndex)

	// 4. 手动触发健康检查，让 MDS 发现 DN 死亡并移除
	time.Sleep(150 * time.Millisecond) // 等待心跳超时
	tc.mds.checkDataNodeHealth()

	// 验证 dead DN 已从集群移除
	tc.mds.mu.RLock()
	nodeCount := len(tc.mds.dataNodes)
	tc.mds.mu.RUnlock()
	if nodeCount != 3 {
		t.Fatalf("expected 3 alive nodes after kill, got %d", nodeCount)
	}

	// 5. 触发补副本
	tc.mds.runReplication()

	// 6. 等待补副本完成（副本数恢复到 3，状态 complete）
	deadline := time.Now().Add(10 * time.Second)
	var repaired bool
	for time.Now().Before(deadline) {
		tc.mds.mu.RLock()
		e = tc.mds.lookup("/replicate-test.txt")
		tc.mds.mu.RUnlock()
		if e != nil && len(e.replicas) == 3 && e.status == StatusComplete {
			// 确保新副本地址不同于 dead DN
			hasDead := false
			for _, r := range e.replicas {
				if r.Addr == deadDNAddr {
					hasDead = true
					break
				}
			}
			if !hasDead {
				repaired = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !repaired {
		tc.mds.mu.RLock()
		e = tc.mds.lookup("/replicate-test.txt")
		tc.mds.mu.RUnlock()
		addrs := make([]string, len(e.replicas))
		for i, r := range e.replicas {
			addrs[i] = r.Addr
		}
		t.Fatalf("replication did not complete: status=%s replicas=%v", e.status, addrs)
	}

	t.Logf("repaired replicas: %d", len(e.replicas))

	// 7. 验证数据一致性：读回文件内容
	fh, err = c.Open("/replicate-test.txt", client.O_RDONLY)
	if err != nil {
		t.Fatalf("open after repair: %v", err)
	}
	defer fh.Close()
	buf := make([]byte, len(content))
	n, err := fh.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("read after repair: %v", err)
	}
	if n != len(content) {
		t.Fatalf("expected %d bytes, got %d", len(content), n)
	}
	if !bytes.Equal(buf, content) {
		t.Fatal("data mismatch after replication")
	}
	t.Log("data verification passed after replication")
}

// TestAutoReplication_WriteBlockedDuringRecovery 验证补副本期间写被拒绝。
func TestAutoReplication_WriteBlockedDuringRecovery(t *testing.T) {
	tc := startReplicatorCluster(t, 4)
	defer tc.cleanup()

	tc.mds.replicaCount = 3
	c := tc.newClient()

	// 创建文件
	content := []byte("hello world recovery test")
	fh, err := c.Open("/recovery-write-test.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	fh.WriteAt(content, 0)
	fh.Close()

	// 杀掉一个 DN 并触发健康检查
	tc.killDN(1)
	time.Sleep(150 * time.Millisecond)
	tc.mds.checkDataNodeHealth()

	// 手动将文件标记为 recovering（模拟补副本进行中）
	tc.mds.submitCommand(&commandPayload{
		Op:        OpUpdateStatus,
		Path:      "/recovery-write-test.txt",
		NewStatus: StatusRecovering,
	})

	// 验证写被拒绝：O_RDWR 打开应该被拒绝
	_, err = c.Open("/recovery-write-test.txt", client.O_RDWR)
	if err == nil {
		t.Fatal("expected open with O_RDWR to be blocked during recovery, but it succeeded")
	}
	t.Logf("write open correctly blocked during recovery: %v", err)

	// 验证读正常：O_RDONLY 打开应该成功
	fh, err = c.Open("/recovery-write-test.txt", client.O_RDONLY)
	if err != nil {
		t.Fatalf("open for read should succeed during recovery: %v", err)
	}
	defer fh.Close()
	buf := make([]byte, len(content))
	n, err := fh.ReadAt(buf, 0)
	if err != nil || n != len(content) || !bytes.Equal(buf, content) {
		t.Fatalf("read failed during recovery: n=%d err=%v", n, err)
	}
	t.Log("read correctly allowed during recovery")

	// 恢复状态
	tc.mds.submitCommand(&commandPayload{
		Op:        OpUpdateStatus,
		Path:      "/recovery-write-test.txt",
		NewStatus: StatusComplete,
	})
}

// TestAutoReplication_MultipleFiles 验证多个 under-replicated 文件能串行修复。
func TestAutoReplication_MultipleFiles(t *testing.T) {
	tc := startReplicatorCluster(t, 5) // 5 DN，杀 2 个后仍有 3 个存活，可恢复 RF=3
	defer tc.cleanup()

	tc.mds.replicaCount = 3
	c := tc.newClient()

	// 创建 5 个文件
	contents := make(map[string][]byte)
	for i := 0; i < 5; i++ {
		path := "/multi-file-" + string(rune('A'+i)) + ".txt"
		data := bytes.Repeat([]byte("DATA-"+string(rune('A'+i))+"-"), 50)
		contents[path] = data
		fh, err := c.Open(path, client.O_CREAT|client.O_RDWR)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		fh.WriteAt(data, 0)
		fh.Close()
	}

	// 杀掉 2 个 DN
	tc.killDN(1)
	tc.killDN(2)
	time.Sleep(150 * time.Millisecond)
	tc.mds.checkDataNodeHealth()

	// 触发补副本（会串行修复所有 under-replicated 文件）
	tc.mds.runReplication()

	// 等待所有文件修复完成
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		allRepaired := true
		for path := range contents {
			tc.mds.mu.RLock()
			e := tc.mds.lookup(path)
			tc.mds.mu.RUnlock()
			if e == nil || len(e.replicas) < 3 || e.status != StatusComplete {
				allRepaired = false
				break
			}
		}
		if allRepaired {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 验证所有文件数据一致
	for path, expected := range contents {
		fh, err := c.Open(path, client.O_RDONLY)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		buf := make([]byte, len(expected))
		n, err := fh.ReadAt(buf, 0)
		fh.Close()
		if err != nil || n != len(expected) || !bytes.Equal(buf, expected) {
			t.Fatalf("data mismatch for %s: n=%d err=%v", path, n, err)
		}
	}
	t.Log("all 5 files repaired and verified")
}
