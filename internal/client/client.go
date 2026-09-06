// Package client 实现分布式文件系统的客户端。
//
// 客户端职责：
//  1. 对用户暴露简单的 CLI 命令（put/get/ls/mkdir/rm/stat）
//  2. 元数据操作（mkdir/ls/stat/rm）→ 直接 RPC 到 MDS
//  3. 数据操作（put/get）→ 先 RPC 到 MDS 拿位置，再 RPC 到 DataNode 传数据
package client

import (
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"strings"
	"sync"

	"github.com/lwwgo/fstorex/internal/types"
)

// Client 分布式文件系统客户端。
type Client struct {
	mdsAddr string
	logger  *slog.Logger
}

// NewClient 创建客户端。
func NewClient(mdsAddr string, logger *slog.Logger) *Client {
	return &Client{mdsAddr: mdsAddr, logger: logger}
}

// dialMDS 连接到 MDS。
func (c *Client) dialMDS() (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", c.mdsAddr)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to metadata server at %s: %w", c.mdsAddr, err)
	}
	return client, nil
}

// callMDSWithRedirect 调用 MDS RPC，自动处理 follower→leader 重定向。
// 写操作（Mkdir/CreateFile/Delete/Heartbeat/RemoveDataNode）仅 leader 可处理，
// follower 会返回 "not leader, redirect to <addr>" 错误，此方法自动提取地址并重试。
func (c *Client) callMDSWithRedirect(method string, args, reply any) error {
	addr := c.mdsAddr
	for retries := 0; retries < 3; retries++ {
		rpcClient, err := rpc.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("cannot connect to metadata server at %s: %w", addr, err)
		}
		err = rpcClient.Call(method, args, reply)
		rpcClient.Close()
		if err == nil {
			return nil
		}
		// 检测 not leader 错误并重定向
		if leader := extractLeaderAddr(err.Error()); leader != "" && leader != addr {
			c.logger.Info("not leader, redirecting", "from", addr, "to", leader)
			addr = leader
			continue
		}
		return err
	}
	return fmt.Errorf("redirect retries exhausted for method %s", method)
}

// extractLeaderAddr 从 "not leader, redirect to localhost:9002" 格式的错误中提取 leader 地址。
func extractLeaderAddr(errMsg string) string {
	const prefix = "not leader, redirect to "
	if idx := strings.Index(errMsg, prefix); idx >= 0 {
		return strings.TrimSpace(errMsg[idx+len(prefix):])
	}
	return ""
}

// dialDataNode 连接到指定的数据节点。
func (c *Client) dialDataNode(addr string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to data node at %s: %w", addr, err)
	}
	return client, nil
}

// Mkdir 创建目录。
func (c *Client) Mkdir(path string) error {
	var reply bool
	if err := c.callMDSWithRedirect("MetadataService.Mkdir", path, &reply); err != nil {
		return fmt.Errorf("mkdir failed: %w", err)
	}
	c.logger.Info("directory created", "path", path)
	return nil
}

// PutOption configures optional behavior for PutFile.
type PutOption func(*putConfig)

type putConfig struct {
	createMode types.CreateMode
}

// WithCreateMode sets the CreateMode for PutFile (default: CreateIfNotExist).
func WithCreateMode(mode types.CreateMode) PutOption {
	return func(c *putConfig) {
		c.createMode = mode
	}
}

// PutFile uploads a local file to the distributed file system.
// Flow: read local file → RPC MDS create metadata (get all replica locations) →
//
//	parallel RPC to all DataNodes to store content.
//
// Options:
//
//	WithCreateMode(types.OverwriteIfExists) — overwrite existing file instead of erroring.
func (c *Client) PutFile(localPath, remotePath string, opts ...PutOption) error {
	cfg := &putConfig{createMode: types.CreateIfNotExist}
	for _, opt := range opts {
		opt(cfg)
	}

	// 1. Read local file
	content, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("read local file failed: %w", err)
	}
	c.logger.Info("local file read", "local", localPath, "size", len(content))

	// 2. RPC MDS create file metadata, get all replica locations (auto-redirect to leader)
	createArgs := &types.CreateFileArgs{
		Path:       remotePath,
		Size:       int64(len(content)),
		CreateMode: cfg.createMode,
	}
	var createReply types.CreateFileReply
	if err := c.callMDSWithRedirect("MetadataService.CreateFile", createArgs, &createReply); err != nil {
		return fmt.Errorf("create metadata failed: %w", err)
	}
	c.logger.Info("metadata created, replicas assigned", "path", remotePath, "replicas", len(createReply.Replicas))

	// 3. Store content to all replica DataNodes in parallel
	type storeResult struct {
		addr string
		err  error
	}
	resultCh := make(chan storeResult, len(createReply.Replicas))
	for _, replica := range createReply.Replicas {
		go func(r types.Replica) {
			dn, err := c.dialDataNode(r.Addr)
			if err != nil {
				resultCh <- storeResult{addr: r.Addr, err: err}
				return
			}
			defer dn.Close()

			storeArgs := &types.StoreArgs{
				Path:    r.RemotePath,
				Content: content,
			}
			var storeReply bool
			if err := dn.Call("DataService.StoreData", storeArgs, &storeReply); err != nil {
				resultCh <- storeResult{addr: r.Addr, err: fmt.Errorf("store data failed: %w", err)}
				return
			}
			resultCh <- storeResult{addr: r.Addr, err: nil}
		}(replica)
	}

	// Wait for all stores, count successes
	successCount := 0
	var firstErr error
	for i := 0; i < len(createReply.Replicas); i++ {
		result := <-resultCh
		if result.err == nil {
			successCount++
		} else if firstErr == nil {
			firstErr = result.err
		}
	}

	// Need at least one successful replica
	if successCount == 0 {
		// Best-effort cleanup: remove the metadata we just created to avoid
		// leaving a ghost file (metadata exists but no data on any replica).
		// The delete itself may fail (e.g. client crash before this point),
		// so we log a warning but still return the original upload error.
		if delErr := c.Delete(remotePath); delErr != nil {
			c.logger.Warn("best-effort cleanup of ghost metadata failed", "path", remotePath, "error", delErr)
		} else {
			c.logger.Info("ghost metadata cleaned up", "path", remotePath)
		}
		return fmt.Errorf("all %d replica stores failed: %w", len(createReply.Replicas), firstErr)
	}

	// Mark file as complete so reads can succeed.
	var completeReply bool
	if err := c.callMDSWithRedirect("MetadataService.CompleteFile", remotePath, &completeReply); err != nil {
		// Data was uploaded successfully; completion failure is non-fatal
		// (file stays pending, GC will eventually clean it if never completed).
		c.logger.Warn("failed to mark file complete", "path", remotePath, "error", err)
	}

	c.logger.Info("file uploaded", "remote", remotePath, "size", len(content), "replicas_ok", successCount, "replicas_total", len(createReply.Replicas))
	return nil
}

// CompleteFile marks a file as complete after upload.
func (c *Client) CompleteFile(path string) error {
	var reply bool
	if err := c.callMDSWithRedirect("MetadataService.CompleteFile", path, &reply); err != nil {
		return fmt.Errorf("complete file failed: %w", err)
	}
	return nil
}

// GetFile downloads a file from the distributed file system to local.
// Flow: RPC MDS get all replica locations → try each DataNode in order until one succeeds → write local file.
func (c *Client) GetFile(remotePath, localPath string) error {
	// 1. RPC MDS query file locations
	mds, err := c.dialMDS()
	if err != nil {
		return err
	}
	defer mds.Close()

	var loc types.FileLocation
	if err := mds.Call("MetadataService.GetFileLocation", remotePath, &loc); err != nil {
		return fmt.Errorf("get file location failed: %w", err)
	}
	c.logger.Info("file locations resolved", "remote", remotePath, "replicas", len(loc.Replicas), "status", loc.Status)

	// Pending files are treated as empty (like POSIX creat() before write):
	// write 0-byte content directly without contacting DataNodes.
	if loc.Status == "pending" {
		if err := os.WriteFile(localPath, []byte{}, 0644); err != nil {
			return fmt.Errorf("write local file failed: %w", err)
		}
		c.logger.Info("pending file downloaded as empty", "remote", remotePath, "local", localPath)
		return nil
	}

	// 2. Try each replica in order until one succeeds
	var content []byte
	var lastErr error
	for _, replica := range loc.Replicas {
		dn, err := c.dialDataNode(replica.Addr)
		if err != nil {
			lastErr = err
			c.logger.Warn("failed to connect to replica, trying next", "data_node", replica.Addr, "error", err)
			continue
		}

		var replicaContent []byte
		if err := dn.Call("DataService.GetData", replica.RemotePath, &replicaContent); err != nil {
			lastErr = err
			c.logger.Warn("failed to read from replica, trying next", "data_node", replica.Addr, "error", err)
			dn.Close()
			continue
		}
		dn.Close()
		content = replicaContent
		break
	}
	if content == nil {
		return fmt.Errorf("all %d replicas failed, last error: %w", len(loc.Replicas), lastErr)
	}

	// 3. Write local file
	if err := os.WriteFile(localPath, content, 0644); err != nil {
		return fmt.Errorf("write local file failed: %w", err)
	}

	c.logger.Info("file downloaded", "remote", remotePath, "local", localPath, "size", len(content))
	return nil
}

// ListDir 列出目录内容。
func (c *Client) ListDir(path string) ([]types.FileInfo, error) {
	mds, err := c.dialMDS()
	if err != nil {
		return nil, err
	}
	defer mds.Close()

	var infos []types.FileInfo
	if err := mds.Call("MetadataService.ListDir", path, &infos); err != nil {
		return nil, fmt.Errorf("list dir failed: %w", err)
	}
	return infos, nil
}

// Stat 获取文件/目录元数据。
func (c *Client) Stat(path string) (*types.FileInfo, error) {
	mds, err := c.dialMDS()
	if err != nil {
		return nil, err
	}
	defer mds.Close()

	var info types.FileInfo
	if err := mds.Call("MetadataService.Stat", path, &info); err != nil {
		return nil, fmt.Errorf("stat failed: %w", err)
	}
	return &info, nil
}

// Delete removes a file or directory.
// If it's a file, also notifies all replica DataNodes to clean up content.
func (c *Client) Delete(path string) error {
	var reply types.DeleteReply
	if err := c.callMDSWithRedirect("MetadataService.Delete", path, &reply); err != nil {
		return fmt.Errorf("delete metadata failed: %w", err)
	}

	// If it's a file, notify all replica DataNodes to clean up content
	if !reply.IsDir && len(reply.Replicas) > 0 {
		for _, replica := range reply.Replicas {
			dn, err := c.dialDataNode(replica.Addr)
			if err != nil {
				c.logger.Warn("failed to connect to replica for cleanup", "data_node", replica.Addr, "error", err)
				continue // metadata already deleted, cleanup failure is non-blocking
			}

			var dnReply bool
			if err := dn.Call("DataService.DeleteData", replica.RemotePath, &dnReply); err != nil {
				c.logger.Warn("failed to delete data from replica", "data_node", replica.Addr, "error", err)
			} else {
				c.logger.Info("data cleaned on replica", "data_node", replica.Addr)
			}
			dn.Close()
		}
	}

	c.logger.Info("path deleted", "path", path)
	return nil
}

// GetReplicas queries and returns all replica locations for a file.
func (c *Client) GetReplicas(path string) ([]types.Replica, error) {
	mds, err := c.dialMDS()
	if err != nil {
		return nil, err
	}
	defer mds.Close()

	var loc types.FileLocation
	if err := mds.Call("MetadataService.GetFileLocation", path, &loc); err != nil {
		return nil, fmt.Errorf("get file location failed: %w", err)
	}
	return loc.Replicas, nil
}

// ListDataNodes 列出所有已注册的数据节点。
func (c *Client) ListDataNodes() ([]string, error) {
	mds, err := c.dialMDS()
	if err != nil {
		return nil, err
	}
	defer mds.Close()

	var nodes []string
	if err := mds.Call("MetadataService.ListDataNodes", struct{}{}, &nodes); err != nil {
		return nil, fmt.Errorf("list data nodes failed: %w", err)
	}
	return nodes, nil
}

// TriggerGC 手动触发一次孤儿数据垃圾回收。
func (c *Client) TriggerGC() error {
	var reply bool
	if err := c.callMDSWithRedirect("MetadataService.TriggerGC", struct{}{}, &reply); err != nil {
		return fmt.Errorf("trigger GC failed: %w", err)
	}
	return nil
}

// ===== FileHandle 相关 Client 方法 =====

// Open 打开或创建文件，返回 FileHandle。
//
// 语义（对齐 POSIX）：
//   - 不存在 + O_CREAT → 创建新文件（新 FileID，status=pending）
//   - 不存在 + 无 O_CREAT → 返回 ENOENT
//   - 已存在 + O_EXCL → 返回错误（排他创建）
//   - 已存在 + O_TRUNC → 打开原文件并截断为 0（FileID 不变，先数据后元数据）
//   - 已存在 + 无 O_TRUNC → 正常打开
func (c *Client) Open(path string, flags int) (*FileHandle, error) {
	// 1. 检查文件是否存在
	info, err := c.Stat(path)
	exists := err == nil
	if err != nil && !strings.Contains(err.Error(), "not found") {
		return nil, fmt.Errorf("open: stat failed: %w", err)
	}

	var fileID string
	var size int64
	var status string
	var replicas []types.Replica

	if !exists {
		// 文件不存在
		if flags&O_CREAT == 0 {
			return nil, fmt.Errorf("open: no such file or directory: %s", path)
		}
		// O_CREAT: 创建新文件
		createArgs := &types.CreateFileArgs{
			Path:       path,
			Size:       0,
			CreateMode: types.CreateIfNotExist, // O_CREAT 不覆盖，O_TRUNC 单独处理
		}
		var createReply types.CreateFileReply
		if err := c.callMDSWithRedirect("MetadataService.CreateFile", createArgs, &createReply); err != nil {
			return nil, fmt.Errorf("open: create failed: %w", err)
		}
		replicas = createReply.Replicas
		status = "pending"
		size = 0
		// fileID 将从 GetFileLocation 拿到
	} else {
		// 文件已存在
		if flags&O_EXCL != 0 {
			return nil, fmt.Errorf("open: file already exists (O_EXCL): %s", path)
		}
		if info.IsDir {
			return nil, fmt.Errorf("open: is a directory: %s", path)
		}
		// O_TRUNC: 打开原文件并截断为 0（FileID 不变）
		if flags&O_TRUNC != 0 {
			if err := c.truncateAllReplicas(path, 0); err != nil {
				return nil, fmt.Errorf("open: truncate replicas failed: %w", err)
			}
			// 先数据后元数据
			if err := c.updateSize(path, 0); err != nil {
				return nil, fmt.Errorf("open: update size failed: %w", err)
			}
			size = 0
		} else {
			size = info.Size
		}
	}

	// 2. 获取副本位置（同时拿到 FileID）
	loc, err := c.getFileLocation(path)
	if err != nil {
		return nil, fmt.Errorf("open: get location failed: %w", err)
	}
	fileID = loc.FileID
	if replicas == nil {
		replicas = loc.Replicas
	}
	if status == "" {
		status = loc.Status
	}

	// Recovering 状态下禁止写（补副本期间保证新副本数据一致）
	if status == "recovering" && flags&(O_WRONLY|O_RDWR) != 0 {
		return nil, fmt.Errorf("open: file is recovering, write not allowed: %s", path)
	}

	c.logger.Info("file opened", "path", path, "file_id", fileID, "flags", flags, "size", size, "status", status)
	return &FileHandle{
		client:   c,
		path:     path,
		fileID:   fileID,
		flags:    flags,
		replicas: replicas,
		size:     size,
		status:   status,
	}, nil
}

// Rename 重命名/移动文件或目录。FileID 不变，已打开的 FileHandle 仍指向原对象。
func (c *Client) Rename(src, dst string) error {
	args := &types.RenameArgs{SrcPath: src, DstPath: dst}
	var reply bool
	if err := c.callMDSWithRedirect("MetadataService.Rename", args, &reply); err != nil {
		return fmt.Errorf("rename failed: %w", err)
	}
	c.logger.Info("path renamed", "src", src, "dst", dst)
	return nil
}

// ===== FileHandle 内部辅助方法 =====

// truncateAllReplicas 并行截断所有副本到 size，全部成功才返回。
func (c *Client) truncateAllReplicas(path string, size int64) error {
	loc, err := c.getFileLocation(path)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errs := make([]error, len(loc.Replicas))
	for i, replica := range loc.Replicas {
		wg.Add(1)
		go func(idx int, r types.Replica) {
			defer wg.Done()
			errs[idx] = c.truncateReplica(r, size)
		}(i, replica)
	}
	wg.Wait()

	var firstErr error
	for _, err := range errs {
		if err != nil {
			firstErr = err
			break
		}
	}
	return firstErr
}

// updateSize 调用 MDS 更新文件大小。
func (c *Client) updateSize(path string, size int64) error {
	args := &types.UpdateSizeArgs{Path: path, Size: size}
	var reply bool
	return c.callMDSWithRedirect("MetadataService.UpdateSize", args, &reply)
}

// getFileLocation 查询文件位置并返回 FileID。
func (c *Client) getFileLocation(path string) (*types.FileLocation, error) {
	mds, err := c.dialMDS()
	if err != nil {
		return nil, err
	}
	defer mds.Close()

	var loc types.FileLocation
	if err := mds.Call("MetadataService.GetFileLocation", path, &loc); err != nil {
		return nil, err
	}
	return &loc, nil
}

// rangeRead 从指定副本随机读取，返回实际读到字节数、是否 EOF、错误。
func (c *Client) rangeRead(replica types.Replica, offset int64, buf []byte) (int, bool, error) {
	dn, err := c.dialDataNode(replica.Addr)
	if err != nil {
		return 0, false, err
	}
	defer dn.Close()

	args := &types.RangeReadArgs{
		Path:   replica.RemotePath,
		Offset: offset,
		Length: int64(len(buf)),
	}
	var reply types.RangeReadReply
	if err := dn.Call("DataService.RangeRead", args, &reply); err != nil {
		return 0, false, fmt.Errorf("range read failed: %w", err)
	}
	n := copy(buf, reply.Data)
	return n, reply.EOF, nil
}

// partialWrite 向指定副本随机写入。
func (c *Client) partialWrite(replica types.Replica, offset int64, data []byte) error {
	dn, err := c.dialDataNode(replica.Addr)
	if err != nil {
		return err
	}
	defer dn.Close()

	args := &types.PartialWriteArgs{
		Path:   replica.RemotePath,
		Offset: offset,
		Data:   data,
	}
	var reply bool
	if err := dn.Call("DataService.PartialWrite", args, &reply); err != nil {
		return fmt.Errorf("partial write failed: %w", err)
	}
	return nil
}

// truncateReplica 截断指定副本。
func (c *Client) truncateReplica(replica types.Replica, size int64) error {
	dn, err := c.dialDataNode(replica.Addr)
	if err != nil {
		return err
	}
	defer dn.Close()

	args := &types.TruncateArgs{
		Path: replica.RemotePath,
		Size: size,
	}
	var reply bool
	if err := dn.Call("DataService.Truncate", args, &reply); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}
	return nil
}

// syncReplica fsync 指定副本。
func (c *Client) syncReplica(replica types.Replica) error {
	dn, err := c.dialDataNode(replica.Addr)
	if err != nil {
		return err
	}
	defer dn.Close()

	args := &types.SyncArgs{Path: replica.RemotePath}
	var reply bool
	if err := dn.Call("DataService.Sync", args, &reply); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
	return nil
}
