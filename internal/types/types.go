// Package types 定义三个分布式组件共享的数据结构和 RPC 接口。
//
// 架构总览：
//
//	Client ──RPC──▶ MetadataServer（管目录树 + 文件→数据节点映射）
//	Client ──RPC──▶ DataNode（存实际文件内容）
//	DataNode ──RPC──▶ MetadataServer（启动时注册自己）
package types

import "time"

// FileInfo 描述一个文件或目录的元数据（与本地版本保持一致）。
type FileInfo struct {
	Name      string    `json:"name"`
	Path      string    `json:"path"`
	IsDir     bool      `json:"is_dir"`
	Size      int64     `json:"size"`
	Mode      uint32    `json:"mode"`
	CreatedAt time.Time `json:"created_at"`
	ModTime   time.Time `json:"mod_time"`
	// FileID 是文件对象的稳定身份（inode 等价物）。
	// 创建时生成，rename/unlink 后保持不变，DataNode 物理路径为 /data/{FileID}。
	// 目录的 FileID 为空字符串。
	FileID string `json:"file_id,omitempty"`
}

// Replica describes one copy of a file on a specific data node.
type Replica struct {
	Addr       string `json:"addr"`        // data node RPC address
	RemotePath string `json:"remote_path"` // storage path on that data node
}

// FileLocation describes where all replicas of a file are stored.
type FileLocation struct {
	FileID   string    `json:"file_id"`  // stable object identity (inode equivalent)
	Replicas []Replica `json:"replicas"` // all replica locations
	Status   string    `json:"status"`   // file status: "pending" or "complete"
}

// NodeFile 是 DataNode 本地磁盘上的文件信息，用于 GC 扫描时汇报给 MDS。
// 注意：这是物理层视角的文件，与 FileInfo（MDS 逻辑目录树视角）不同。
type NodeFile struct {
	Path    string    `json:"path"`     // 虚拟路径（/开头）
	ModTime time.Time `json:"mod_time"` // 节点本地最后修改时间
}

// ===== MetadataServer RPC 接口定义 =====

// MetadataService 元数据服务，管理目录树和文件→数据节点的映射。
type MetadataService interface {
	// Heartbeat 数据节点定期发送心跳，首次心跳即注册，超时则被移除。
	Heartbeat(addr string, reply *HeartbeatReply) error

	// Mkdir 创建目录。
	Mkdir(path string, reply *bool) error

	// CreateFile 创建文件元数据，MDS 会选择多个数据节点分配多副本，返回副本位置。
	CreateFile(args *CreateFileArgs, reply *CreateFileReply) error

	// CompleteFile 标记文件数据已上传完成，状态从 pending 变为 complete。
	CompleteFile(path string, reply *bool) error

	// GetFileLocation 查询文件内容存储在哪些数据节点。
	GetFileLocation(path string, reply *FileLocation) error

	// ListDir 列出目录下的文件和子目录。
	ListDir(path string, reply *[]FileInfo) error

	// Stat 获取文件/目录元数据。
	Stat(path string, reply *FileInfo) error

	// Delete 删除文件或目录，返回需要清理的数据节点列表。
	Delete(path string, reply *DeleteReply) error

	// Rename 重命名/移动文件或目录，FileID 不变（走 Raft 原子操作）。
	Rename(args *RenameArgs, reply *bool) error

	// UpdateSize 更新文件大小（写成功后由 client 调用），只改元数据不碰数据。
	UpdateSize(args *UpdateSizeArgs, reply *bool) error

	// ListDataNodes 列出所有已注册的数据节点。
	ListDataNodes(_ struct{}, reply *[]string) error

	// TriggerGC 手动触发一次孤儿数据垃圾回收（仅 leader 可调用）。
	TriggerGC(_ struct{}, reply *bool) error
}

// CreateMode 控制文件已存在时的行为（互斥二选一）。
type CreateMode uint32

const (
	// CreateIfNotExist 默认：不存在才创建，已存在则报错（类似 POSIX O_CREAT|O_EXCL）。
	CreateIfNotExist CreateMode = iota
	// OverwriteIfExists 不存在则创建，已存在则覆盖（类似 POSIX O_CREAT|O_TRUNC）。
	OverwriteIfExists
)

// CreateFileArgs 创建文件的请求参数。
type CreateFileArgs struct {
	Path       string     `json:"path"`
	Size       int64      `json:"size"`
	CreateMode CreateMode `json:"create_mode"` // 文件已存在时的行为，默认 CreateIfNotExist
}

// HeartbeatReply 是心跳响应，告知 DataNode 当前 MDS 角色及 leader 地址。
type HeartbeatReply struct {
	IsLeader bool   `json:"is_leader"` // 当前节点是否 leader
	Leader   string `json:"leader"`    // 如果不是 leader，告知 leader 地址
}

// CreateFileReply is the response for creating a file, containing all assigned replica locations.
type CreateFileReply struct {
	Replicas []Replica `json:"replicas"`
}

// DeleteReply is the response for a delete operation, containing all replica locations to clean up.
type DeleteReply struct {
	Replicas []Replica `json:"replicas"`
	IsDir    bool      `json:"is_dir"`
}

// RenameArgs 重命名/移动的请求参数。
type RenameArgs struct {
	SrcPath string `json:"src_path"`
	DstPath string `json:"dst_path"`
}

// UpdateSizeArgs 更新文件大小的请求参数。
type UpdateSizeArgs struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// ===== DataNode RPC 接口定义 =====

// DataService 数据节点服务，存储实际的文件内容。
type DataService interface {
	// StoreData 存储文件内容（整文件覆盖，兼容旧 CLI）。
	StoreData(args *StoreArgs, reply *bool) error

	// GetData 读取文件内容（整文件读，兼容旧 CLI）。
	GetData(path string, reply *[]byte) error

	// DeleteData 删除文件内容。
	DeleteData(path string, reply *bool) error

	// HealthCheck 健康检查。
	HealthCheck(_ struct{}, reply *bool) error

	// ListAllPaths 返回该数据节点持有的所有文件信息（供 MDS GC 用）。
	ListAllPaths(_ struct{}, reply *[]NodeFile) error

	// RangeRead 随机读取：从 offset 读最多 Length 字节。
	// 到达文件末尾时返回 EOF=true（data 可能为空），不返回错误。
	RangeRead(args *RangeReadArgs, reply *RangeReadReply) error

	// PartialWrite 随机写入：向 offset 写入 Data，支持 sparse file。
	PartialWrite(args *PartialWriteArgs, reply *bool) error

	// Truncate 调整文件大小（缩小丢弃尾部，扩大补 0）。
	Truncate(args *TruncateArgs, reply *bool) error

	// Sync 强制 fsync 确保持久化。
	Sync(args *SyncArgs, reply *bool) error

	// ReplicateFile 从源 DataNode 拉取文件副本到本地。
	// 由 MDS 编排，目标 DN 主动 dial 源 DN，分块 RangeRead + PartialWrite。
	// 数据搬运不经过 MDS，避免成为带宽瓶颈。
	ReplicateFile(args *ReplicateFileArgs, reply *bool) error
}

// StoreArgs 存储数据的请求参数。
type StoreArgs struct {
	Path    string `json:"path"`
	Content []byte `json:"content"`
}

// RangeReadArgs 随机读取的请求参数。
type RangeReadArgs struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

// RangeReadReply 随机读取的响应：Data 为实际读到的字节，EOF 表示是否到达文件末尾。
type RangeReadReply struct {
	Data []byte `json:"data"`
	EOF  bool   `json:"eof"`
}

// PartialWriteArgs 随机写入的请求参数。
type PartialWriteArgs struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

// TruncateArgs 截断/扩展的请求参数。
type TruncateArgs struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// SyncArgs fsync 的请求参数。
type SyncArgs struct {
	Path string `json:"path"`
}

// ReplicateFileArgs 副本复制请求参数。
// 目标 DataNode 收到后主动从 SourceAddr 分块拉取数据写入本地 TargetRemotePath。
type ReplicateFileArgs struct {
	SourceAddr       string `json:"source_addr"`        // 源 DataNode RPC 地址
	SourceRemotePath string `json:"source_remote_path"` // 源 DN 上的物理路径 /data/{fileID}
	TargetRemotePath string `json:"target_remote_path"` // 目标 DN 上的物理路径 /data/{fileID}
	FileSize         int64  `json:"file_size"`          // 文件总大小，用于分块拉取
}
