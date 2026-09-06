// Package metadata 实现分布式文件系统的元数据服务器（MDS）。
//
// MDS 职责：
//  1. 管理全局目录树（文件名、目录结构、权限、时间戳）
//  2. 管理文件→数据节点的映射（文件内容存在哪个 DataNode 上）
//  3. 管理 DataNode 生命周期（首次心跳即注册，超时即摘除）
//
// 高可用：多个 MDS 节点组成 Raft 集群，通过 goraft 保证元数据一致性。
//   - 写操作（Mkdir/CreateFile/Delete/Heartbeat/RemoveDataNode）：仅 leader 可处理，走 Raft 共识复制到多数派后应用。
//   - 读操作（GetFileLocation/ListDir/Stat/ListDataNodes）：所有节点都可处理，直接读本地状态机副本。
//   - follower 收到写请求时返回 leader 地址，客户端自动重定向。
package metadata

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lwwgo/fstorex/internal/types"
	"github.com/lwwgo/goraft/raft"
	rafttypes "github.com/lwwgo/goraft/types"
)

// 写操作类型常量
const (
	OpHeartbeat      = "heartbeat"
	OpRemoveDataNode = "remove_dn"
	OpMkdir          = "mkdir"
	OpCreateFile     = "create_file"
	OpCompleteFile   = "complete_file"
	OpDelete         = "delete"
	OpRename         = "rename"
	OpUpdateSize     = "update_size"
	OpUpdateReplicas = "update_replicas" // 补副本完成后原子替换副本列表
	OpUpdateStatus   = "update_status"   // 补副本期间切换文件状态（complete ↔ recovering）
)

// 文件状态（仅文件节点有效）
const (
	StatusPending    = "pending"    // 元数据已创建，数据尚未完全上传
	StatusComplete   = "complete"   // 数据已就绪，可读可写
	StatusRecovering = "recovering" // 正在补副本，可读但禁止写
)

// commandPayload is the business command payload carried in a Raft log entry.
// All fields are JSON-serialized into CommandEntry.Data, ensuring all replicas
// apply the same commands in the same order for state machine consistency.
type commandPayload struct {
	Op          string           `json:"op"`
	Path        string           `json:"path,omitempty"`
	Addr        string           `json:"addr,omitempty"`         // heartbeat/remove_dn: data node address
	Size        int64            `json:"size,omitempty"`         // create_file: file size
	Replicas    []types.Replica  `json:"replicas,omitempty"`     // create_file: initial replicas
	IsDir       bool             `json:"is_dir,omitempty"`       // delete: whether the target is a directory
	CreateMode  types.CreateMode `json:"create_mode,omitempty"`  // create_file: behavior when file exists
	FileID      string           `json:"file_id,omitempty"`      // create_file: stable object identity (inode equivalent)
	DstPath     string           `json:"dst_path,omitempty"`     // rename: destination path
	NewSize     int64            `json:"new_size,omitempty"`     // update_size: new file size
	NewReplicas []types.Replica  `json:"new_replicas,omitempty"` // update_replicas: full new replica list (replace, not append)
	NewStatus   string           `json:"new_status,omitempty"`   // update_status: target status (complete/recovering)
}

// entry is a node in the directory tree (file or directory).
type entry struct {
	name      string
	isDir     bool
	size      int64
	mode      uint32
	createdAt time.Time
	modTime   time.Time
	status    string // only for file nodes: "pending" or "complete"
	fileID    string // only for file nodes: stable object identity (inode equivalent)

	// Only for file nodes: replica locations on data nodes
	replicas []types.Replica

	// Only for directory nodes: child nodes
	children map[string]*entry
}

// MetadataServer is the core implementation of the metadata server.
// It also implements goraft's StateMachine interface: Raft handles log replication,
// while MetadataServer applies logs to the directory tree state machine.
type MetadataServer struct {
	mu            sync.RWMutex
	root          *entry               // root directory
	dataNodes     []string             // registered data node addresses
	lastHeartbeat map[string]time.Time // data node address → last heartbeat time
	raftServer    *raft.Server
	logger        *slog.Logger
	replicaCount  int // desired number of replicas per file (default 3)

	// 补副本 single-flight：防止同一文件被并发修复
	repairingMu sync.Mutex
	repairing   map[string]bool // fileID → true

	// 可配置的时间参数（默认值见 NewMetadataServer，测试时可覆盖）
	heartbeatTimeout  time.Duration // DN 心跳超时判定
	replicateInterval time.Duration // 补副本扫描间隔
}

// NewMetadataServer 创建并启动集成了 Raft 的元数据服务器。
// raftConfig 是 goraft 的配置（必须包含 LocalID、Peers、WalDir、SnapDir）。
// 注意：本方法不启动 RPC listener，由 main.go 统一管理 rpc.Server，
// 将 Raft 内部服务（"Server"）和业务服务（"MetadataService"）注册到同一端口，
// 这样 GetLeader 返回的地址客户端可直接用于业务 RPC。
func NewMetadataServer(raftConfig rafttypes.Config, logger *slog.Logger) (*MetadataServer, error) {
	mds := &MetadataServer{
		root: &entry{
			name:      "/",
			isDir:     true,
			mode:      0755,
			createdAt: time.Now(),
			modTime:   time.Now(),
			children:  make(map[string]*entry),
		},
		dataNodes:         make([]string, 0),
		lastHeartbeat:     make(map[string]time.Time),
		logger:            logger,
		replicaCount:      3, // default: 3 replicas per file
		repairing:         make(map[string]bool),
		heartbeatTimeout:  10 * time.Minute, // default DN heartbeat timeout
		replicateInterval: 5 * time.Minute,  // default replication scan interval
	}

	// 把 mds 作为 StateMachine 注入 Raft
	raftConfig.StateMachine = mds
	raftConfig.Logger = logger

	// 初始化 Raft 节点（不自动启动 RPC，由 main.go 统一管理）
	raftNode, err := raft.InitServer(raftConfig)
	if err != nil {
		return nil, fmt.Errorf("init raft server failed: %w", err)
	}
	mds.raftServer = raftNode

	// 启动 Raft 选举和心跳定时器（非阻塞）
	raftNode.Start()

	// 启动后台孤儿数据 GC（仅 leader 实际执行）
	mds.startGC()

	// 启动后台自动补副本（仅 leader 实际执行，周期性扫描）
	mds.startReplicator()

	logger.Info("metadata server with raft initialized",
		"local_id", raftConfig.LocalID,
		"peers", raftConfig.Peers)

	return mds, nil
}

// GetRaftNode 返回底层 Raft 节点，供 main.go 注册 Raft RPC 服务到共享 rpc.Server。
func (mds *MetadataServer) GetRaftNode() *raft.Server {
	return mds.raftServer
}

// IsLeader 返回当前 MDS 节点是否为 Raft leader。
func (mds *MetadataServer) IsLeader() bool {
	return mds.raftServer.IsLeader()
}

// GetLeader 返回当前 leader 的业务 RPC 地址（用于客户端重定向）。
// 注意：Raft 的 LocalID 存的是 raft RPC 地址，这里需要映射到业务地址。
func (mds *MetadataServer) GetLeader() string {
	return mds.raftServer.GetLeader()
}

// ===== 写操作（仅 leader 可处理，走 Raft 共识）=====

// checkLeader 检查当前节点是否为 leader，不是则返回错误并附带 leader 地址。
func (mds *MetadataServer) checkLeader() error {
	if !mds.raftServer.IsLeader() {
		leader := mds.raftServer.GetLeader()
		return &ErrNotLeader{Leader: leader}
	}
	return nil
}

// ErrNotLeader 表示当前节点不是 leader，客户端应重定向到 Leader。
type ErrNotLeader struct {
	Leader string
}

func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("not leader, redirect to %s", e.Leader)
}

// submitCommand 把命令封装成 Raft 日志并提交。
// 仅 leader 可调用。返回时命令已复制到多数派并应用到状态机。
func (mds *MetadataServer) submitCommand(payload *commandPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal command payload failed: %w", err)
	}
	cmd := rafttypes.CommandEntry{
		Op:   payload.Op,
		Data: data,
	}
	if err := mds.raftServer.Do(cmd); err != nil {
		return fmt.Errorf("raft do failed: %w", err)
	}
	return nil
}

// Heartbeat 数据节点定期发送心跳，首次心跳即注册，超时则被移除。
func (mds *MetadataServer) Heartbeat(addr string, reply *types.HeartbeatReply) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}
	payload := &commandPayload{Op: OpHeartbeat, Addr: addr}
	if err := mds.submitCommand(payload); err != nil {
		return err
	}
	reply.IsLeader = true
	mds.logger.Debug("data node heartbeat received", "addr", addr)
	return nil
}

// Mkdir 创建目录。目录也分配稳定 FileID（inode 等价物），
// 确保 rename 后 inode 号不变（POSIX 语义）。
func (mds *MetadataServer) Mkdir(path string, reply *bool) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}
	fileID, err := generateFileID()
	if err != nil {
		return fmt.Errorf("generate file id failed: %w", err)
	}
	payload := &commandPayload{Op: OpMkdir, Path: path, FileID: fileID}
	if err := mds.submitCommand(payload); err != nil {
		return err
	}
	*reply = true
	mds.logger.Info("directory created via raft", "path", path)
	return nil
}

// CreateFile creates file metadata. MDS selects multiple data nodes for replication.
func (mds *MetadataServer) CreateFile(args *types.CreateFileArgs, reply *types.CreateFileReply) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}

	// Pre-validate: data nodes available + parent dir exists + select replica nodes
	fileID, err := generateFileID()
	if err != nil {
		return fmt.Errorf("generate file id failed: %w", err)
	}
	mds.mu.RLock()
	if len(mds.dataNodes) == 0 {
		mds.mu.RUnlock()
		return fmt.Errorf("no data nodes registered")
	}
	parts := splitPath(args.Path)
	if len(parts) == 0 {
		mds.mu.RUnlock()
		return fmt.Errorf("invalid path: %s", args.Path)
	}
	dirParts := parts[:len(parts)-1]
	current := mds.root
	for _, part := range dirParts {
		child, exists := current.children[part]
		if !exists || !child.isDir {
			mds.mu.RUnlock()
			return fmt.Errorf("parent directory not found: %s", args.Path)
		}
		current = child
	}
	replicas := mds.selectDataNodes(args.Path, fileID, mds.replicaCount)
	mds.mu.RUnlock()

	payload := &commandPayload{
		Op:         OpCreateFile,
		Path:       args.Path,
		Size:       args.Size,
		Replicas:   replicas,
		CreateMode: args.CreateMode,
		FileID:     fileID,
	}
	if err := mds.submitCommand(payload); err != nil {
		return err
	}

	reply.Replicas = replicas
	mds.logger.Info("file metadata created via raft", "path", args.Path, "replicas", len(replicas))
	return nil
}

// CompleteFile marks a file as complete after data upload finishes.
func (mds *MetadataServer) CompleteFile(path string, reply *bool) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}
	payload := &commandPayload{Op: OpCompleteFile, Path: path}
	if err := mds.submitCommand(payload); err != nil {
		return err
	}
	*reply = true
	mds.logger.Info("file marked complete via raft", "path", path)
	return nil
}

// Delete removes a file or directory.
func (mds *MetadataServer) Delete(path string, reply *types.DeleteReply) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}

	// Pre-lookup the target to get replica cleanup info for the client
	mds.mu.RLock()
	target := mds.lookup(path)
	if target == nil {
		mds.mu.RUnlock()
		return fmt.Errorf("path not found: %s", path)
	}
	payload := &commandPayload{
		Op:       OpDelete,
		Path:     path,
		IsDir:    target.isDir,
		Replicas: target.replicas,
	}
	mds.mu.RUnlock()

	if err := mds.submitCommand(payload); err != nil {
		return err
	}

	reply.IsDir = payload.IsDir
	reply.Replicas = payload.Replicas
	mds.logger.Info("path deleted via raft", "path", path, "is_dir", payload.IsDir)
	return nil
}

// Rename renames/moves a file or directory. FileID stays unchanged (inode identity preserved).
// Atomic: the whole operation is a single Raft log entry.
func (mds *MetadataServer) Rename(args *types.RenameArgs, reply *bool) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}
	if args.SrcPath == "" || args.DstPath == "" {
		return fmt.Errorf("src_path and dst_path must not be empty")
	}
	if args.SrcPath == "/" || args.DstPath == "/" {
		return fmt.Errorf("cannot rename root")
	}
	if args.SrcPath == args.DstPath {
		*reply = true
		return nil
	}

	payload := &commandPayload{
		Op:      OpRename,
		Path:    args.SrcPath,
		DstPath: args.DstPath,
	}
	if err := mds.submitCommand(payload); err != nil {
		return err
	}
	*reply = true
	mds.logger.Info("path renamed via raft", "src", args.SrcPath, "dst", args.DstPath)
	return nil
}

// UpdateSize updates a file's size metadata after a successful write.
// Only changes size + modTime; does not touch replicas or FileID.
func (mds *MetadataServer) UpdateSize(args *types.UpdateSizeArgs, reply *bool) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}
	if args.Path == "" {
		return fmt.Errorf("path must not be empty")
	}
	if args.Size < 0 {
		return fmt.Errorf("size must not be negative")
	}

	payload := &commandPayload{
		Op:      OpUpdateSize,
		Path:    args.Path,
		NewSize: args.Size,
	}
	if err := mds.submitCommand(payload); err != nil {
		return err
	}
	*reply = true
	mds.logger.Debug("file size updated via raft", "path", args.Path, "size", args.Size)
	return nil
}

// ===== Read operations (any node can serve; reads local state machine) =====

// GetFileLocation queries where all replicas of a file are stored.
func (mds *MetadataServer) GetFileLocation(path string, reply *types.FileLocation) error {
	mds.mu.RLock()
	defer mds.mu.RUnlock()

	e := mds.lookup(path)
	if e == nil {
		return fmt.Errorf("file not found: %s", path)
	}
	if e.isDir {
		return fmt.Errorf("%s is a directory", path)
	}
	reply.FileID = e.fileID
	reply.Replicas = e.replicas
	reply.Status = e.status
	return nil
}

// ListDir 列出目录内容。
func (mds *MetadataServer) ListDir(path string, reply *[]types.FileInfo) error {
	mds.mu.RLock()
	defer mds.mu.RUnlock()

	e := mds.lookup(path)
	if e == nil {
		return fmt.Errorf("directory not found: %s", path)
	}
	if !e.isDir {
		return fmt.Errorf("%s is not a directory", path)
	}

	var infos []types.FileInfo
	for name, child := range e.children {
		infos = append(infos, types.FileInfo{
			Name:      name,
			Path:      filepath.Join(path, name),
			IsDir:     child.isDir,
			Size:      child.size,
			Mode:      child.mode,
			CreatedAt: child.createdAt,
			ModTime:   child.modTime,
			FileID:    child.fileID,
		})
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].IsDir != infos[j].IsDir {
			return infos[i].IsDir
		}
		return infos[i].Name < infos[j].Name
	})
	*reply = infos
	return nil
}

// Stat 获取文件/目录元数据。
func (mds *MetadataServer) Stat(path string, reply *types.FileInfo) error {
	mds.mu.RLock()
	defer mds.mu.RUnlock()

	e := mds.lookup(path)
	if e == nil {
		return fmt.Errorf("path not found: %s", path)
	}
	*reply = types.FileInfo{
		Name:      e.name,
		Path:      path,
		IsDir:     e.isDir,
		Size:      e.size,
		Mode:      e.mode,
		CreatedAt: e.createdAt,
		ModTime:   e.modTime,
		FileID:    e.fileID,
	}
	return nil
}

// ListDataNodes 列出所有已注册的数据节点。
func (mds *MetadataServer) ListDataNodes(_ struct{}, reply *[]string) error {
	mds.mu.RLock()
	defer mds.mu.RUnlock()
	*reply = mds.dataNodes
	return nil
}

// ===== 内部辅助方法 =====

func splitPath(path string) []string {
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")
	if path == "" {
		return nil
	}
	var parts []string
	for _, p := range strings.Split(path, "/") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

// lookup 在目录树中查找路径，找不到返回 nil。调用方需持读锁。
func (mds *MetadataServer) lookup(path string) *entry {
	parts := splitPath(path)
	current := mds.root
	for _, part := range parts {
		child, exists := current.children[part]
		if !exists {
			return nil
		}
		current = child
	}
	return current
}

// generateFileID generates a stable object identity (inode equivalent) using
// 16 random bytes encoded as hex. Stays zero external dependency by using crypto/rand.
func generateFileID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate file id failed: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// selectDataNodes selects N distinct data nodes for a file's replicas.
// Uses hash-based assignment to spread files deterministically across nodes.
// fileID determines the physical path on each DataNode: /data/{fileID}.
func (mds *MetadataServer) selectDataNodes(path string, fileID string, replicaCount int) []types.Replica {
	n := replicaCount
	if n > len(mds.dataNodes) {
		n = len(mds.dataNodes)
	}
	if n < 1 {
		n = 1
	}

	// Deterministic hash to pick the first node, then rotate for diversity
	hash := 0
	for _, c := range path {
		hash = hash*31 + int(c)
	}
	startIdx := hash % len(mds.dataNodes)
	if startIdx < 0 {
		startIdx = -startIdx
	}

	remotePath := "/data/" + fileID
	replicas := make([]types.Replica, 0, n)
	seen := make(map[string]bool)
	for i := 0; i < n; i++ {
		idx := (startIdx + i) % len(mds.dataNodes)
		addr := mds.dataNodes[idx]
		if seen[addr] {
			continue
		}
		seen[addr] = true
		replicas = append(replicas, types.Replica{
			Addr:       addr,
			RemotePath: remotePath,
		})
	}
	return replicas
}

// ===== JSON 序列化辅助结构（Snapshot/Restore 用）=====

type metaSnapshot struct {
	Root          *entrySnapshot       `json:"root"`
	DataNodes     []string             `json:"data_nodes"`
	LastHeartbeat map[string]time.Time `json:"last_heartbeat"`
}

func (s *metaSnapshot) toEntry() *entry {
	if s.Root == nil {
		return &entry{
			name:     "/",
			isDir:    true,
			mode:     0755,
			children: make(map[string]*entry),
		}
	}
	return s.Root.toEntry()
}

type entrySnapshot struct {
	Name      string                    `json:"name"`
	IsDir     bool                      `json:"is_dir"`
	Size      int64                     `json:"size"`
	Mode      uint32                    `json:"mode"`
	CreatedAt time.Time                 `json:"created_at"`
	ModTime   time.Time                 `json:"mod_time"`
	Status    string                    `json:"status,omitempty"`
	FileID    string                    `json:"file_id,omitempty"`
	Replicas  []types.Replica           `json:"replicas,omitempty"`
	Children  map[string]*entrySnapshot `json:"children,omitempty"`
}

func (e *entry) toSnapshot() *entrySnapshot {
	s := &entrySnapshot{
		Name:      e.name,
		IsDir:     e.isDir,
		Size:      e.size,
		Mode:      e.mode,
		CreatedAt: e.createdAt,
		ModTime:   e.modTime,
		Status:    e.status,
		FileID:    e.fileID,
		Replicas:  e.replicas,
	}
	if e.isDir {
		s.Children = make(map[string]*entrySnapshot)
		for name, child := range e.children {
			s.Children[name] = child.toSnapshot()
		}
	}
	return s
}

func (s *entrySnapshot) toEntry() *entry {
	e := &entry{
		name:      s.Name,
		isDir:     s.IsDir,
		size:      s.Size,
		mode:      s.Mode,
		createdAt: s.CreatedAt,
		modTime:   s.ModTime,
		status:    s.Status,
		fileID:    s.FileID,
		replicas:  s.Replicas,
	}
	if s.IsDir {
		e.children = make(map[string]*entry)
		for name, childSnap := range s.Children {
			e.children[name] = childSnap.toEntry()
		}
	}
	return e
}

// Compile-time assertion: ensure MetadataServer fully implements MetadataService.
var _ types.MetadataService = (*MetadataServer)(nil)
