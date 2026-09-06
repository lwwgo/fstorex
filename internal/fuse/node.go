// Package fuse 实现 FStoreX 的 FUSE 挂载层，将 Linux VFS 操作翻译成
// FStoreX Client 调用：
//
//	VFS open/read/write/mkdir/rename/unlink
//	  → FUSE 回调
//	  → client.Open/ReadAt/WriteAt/Mkdir/Rename/Delete
//	  → MDS + DataNode RPC
//
// 架构分层（POSIX inode 层 vs fd 层，go-fuse node-based API）：
//
//	fuseNode (inode 层，持久)
//	  ├─ 元数据:    Getattr
//	  ├─ 目录操作:  Lookup / Readdir / Mkdir
//	  ├─ 命名操作:  Create / Unlink / Rename
//	  └─ 打开入口:  Open() ← 唯一能创建 fd 的
//
//	             │
//	             │ Open() 返回
//	             ▼
//
//	fuseFileHandle (fd 层，临时)
//	  ├─ 数据操作:  Read / Write
//	  ├─ 同步操作:  Fsync
//	  └─ 销毁:      Release
//
// 为什么要分两层？因为 POSIX 文件系统严格区分：
//   - inode：文件本身，持久存在（只要不删除就一直有），对应 fuseNode
//   - fd：一次 Open 的临时会话，open 创建、close 销毁，对应 fuseFileHandle
//
// 分开才能支持"同一文件被打开多次、各自独立读写位置、独立关闭"。
//
// 设计要点（Phase 1.5 最小实现）：
//   - 使用 hanwen/go-fuse/v2 的 node-based API，自动处理 inode 树和路径解析
//   - path-based lookup（最小实现），后续可优化为 FileID-based
//   - AttrTimeout/EntryTimeout 1s 减少 Getattr/Lookup 对 MDS 的压力
//   - Release（不是 Flush）映射到 FileHandle.Close，确保只关一次
package fuse

import (
	"context"
	"log/slog"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/lwwgo/fstorex/internal/client"
	"github.com/lwwgo/fstorex/internal/types"
)

// ===== FUSE inode 节点 =====

// fuseNode 是 inode 层的实现，对应 POSIX 中的 struct inode，代表"文件本身"，
// 持久存在（只要不删除就一直有）。同一个结构体复用于文件和目录，通过
// client.Stat 结果（IsDir）区分类型。
//
// 与 fuseFileHandle 的本质区别：
//   - fuseNode 是 inode 层，管"文件的存在/命名/属性"，不持有读写会话
//   - fuseFileHandle 是 fd 层，管"某次打开的数据读写"，由 fuseNode.Open() 创建
//
// 职责（inode 层回调）：
//   - 元数据：Getattr（查 mode/size/mtime）
//   - 目录：Lookup（查子节点）、Readdir（列目录）
//   - 命名：Create（创建文件）、Mkdir（创建目录）、Unlink（删除）、Rename（重命名）
//   - 打开入口：Open() —— 这是唯一能创建 fd 层的回调
//
// 生命周期：fuseNode 在首次 Lookup 时创建，内核缓存 + go-fuse inode 树管理，
// Unlink 时通过 RmChild 从内存树移除。path 是 FStoreX 逻辑路径（root 为 "/"），
// 永远用 "/" 分隔，与 OS 无关。
type fuseNode struct {
	fs.Inode
	client *client.Client
	path   string // 对应的 FStoreX 逻辑路径，root 为 "/"，用 "/" 分隔
	logger *slog.Logger
}

// ===== 元数据回调 =====

// Getattr 返回节点属性。设 AttrTimeout 减少内核重复查询。
func (n *fuseNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	info, err := n.client.Stat(n.path)
	if err != nil {
		return errToErrno(err)
	}
	out.Attr = infoToAttr(info)
	out.SetTimeout(defaultTimeout)
	return 0
}

// Lookup 在目录节点下查找子节点。
func (n *fuseNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := joinPath(n.path, name)
	info, err := n.client.Stat(childPath)
	if err != nil {
		return nil, errToErrno(err)
	}
	child := &fuseNode{
		client: n.client,
		path:   childPath,
		logger: n.logger,
	}
	inode := n.NewInode(ctx, child, fs.StableAttr{
		Mode: modeFromInfo(*info),
		Ino:  inodeFromPath(childPath),
	})
	out.Attr = infoToAttr(info)
	out.SetEntryTimeout(defaultTimeout)
	out.SetAttrTimeout(defaultTimeout)
	return inode, 0
}

// Readdir 列出目录内容。
func (n *fuseNode) Readdir(_ context.Context) (fs.DirStream, syscall.Errno) {
	infos, err := n.client.ListDir(n.path)
	if err != nil {
		return nil, errToErrno(err)
	}
	entries := make([]fuse.DirEntry, 0, len(infos))
	for _, info := range infos {
		entries = append(entries, fuse.DirEntry{
			Name: info.Name,
			Mode: modeFromInfo(info),
			Ino:  inodeFromPath(joinPath(n.path, info.Name)),
		})
	}
	return fs.NewListDirStream(entries), 0
}

// ===== 文件操作回调 =====

// Open 打开文件，返回 file handle。
func (n *fuseNode) Open(_ context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	fh, err := n.client.Open(n.path, int(flags))
	if err != nil {
		return nil, 0, errToErrno(err)
	}
	// 去掉底层不支持的 flag（FUSE 内核可能传 O_APPEND 等）
	openFlags := flags &^ syscall.O_APPEND
	return &fuseFileHandle{fh: fh, path: n.path, logger: n.logger}, openFlags, 0
}

// Create 创建并打开文件。
func (n *fuseNode) Create(ctx context.Context, name string, _ uint32, flags uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	childPath := joinPath(n.path, name)
	// POSIX Create 语义：O_CREAT | O_TRUNC
	createFlags := int(flags) | client.O_CREAT | client.O_TRUNC
	if createFlags&syscall.O_ACCMODE == 0 {
		createFlags |= syscall.O_RDWR
	}

	fh, err := n.client.Open(childPath, createFlags)
	if err != nil {
		return nil, nil, 0, errToErrno(err)
	}

	child := &fuseNode{
		client: n.client,
		path:   childPath,
		logger: n.logger,
	}
	inode := n.NewInode(ctx, child, fs.StableAttr{
		Mode: syscall.S_IFREG | 0644,
		Ino:  inodeFromPath(childPath),
	})
	// Create 后文件 size=0，设属性
	out.Attr = fuse.Attr{
		Mode:  syscall.S_IFREG | 0644,
		Nlink: 1,
	}
	out.SetEntryTimeout(defaultTimeout)
	out.SetAttrTimeout(defaultTimeout)

	openFlags := flags &^ syscall.O_APPEND
	return inode, &fuseFileHandle{fh: fh, path: childPath, logger: n.logger}, openFlags, 0
}

// Mkdir 创建目录。
func (n *fuseNode) Mkdir(ctx context.Context, name string, _ uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := joinPath(n.path, name)
	if err := n.client.Mkdir(childPath); err != nil {
		return nil, errToErrno(err)
	}
	child := &fuseNode{
		client: n.client,
		path:   childPath,
		logger: n.logger,
	}
	inode := n.NewInode(ctx, child, fs.StableAttr{
		Mode: syscall.S_IFDIR | 0755,
		Ino:  inodeFromPath(childPath),
	})
	out.Attr = fuse.Attr{
		Mode:  syscall.S_IFDIR | 0755,
		Nlink: 2,
	}
	out.SetEntryTimeout(defaultTimeout)
	out.SetAttrTimeout(defaultTimeout)
	return inode, 0
}

// Unlink 删除文件或空目录。
func (n *fuseNode) Unlink(_ context.Context, name string) syscall.Errno {
	childPath := joinPath(n.path, name)
	if err := n.client.Delete(childPath); err != nil {
		return errToErrno(err)
	}
	n.RmChild(name)
	return 0
}

// Rename 重命名/移动。
func (n *fuseNode) Rename(_ context.Context, name string, newParent fs.InodeEmbedder, newName string, _ uint32) syscall.Errno {
	srcPath := joinPath(n.path, name)
	dstNode, ok := newParent.(*fuseNode)
	if !ok {
		return syscall.EIO
	}
	dstPath := joinPath(dstNode.path, newName)

	if err := n.client.Rename(srcPath, dstPath); err != nil {
		return errToErrno(err)
	}
	n.RmChild(name)
	return 0
}

// ===== 辅助函数 =====

func infoToAttr(info *types.FileInfo) fuse.Attr {
	mode := modeFromInfo(*info)
	attr := fuse.Attr{
		Mode:  mode,
		Size:  uint64(info.Size),
		Mtime: uint64(info.ModTime.Unix()),
	}
	if info.IsDir {
		attr.Nlink = 2
	} else {
		attr.Nlink = 1
	}
	return attr
}

func modeFromInfo(info types.FileInfo) uint32 {
	if info.IsDir {
		return syscall.S_IFDIR | 0755
	}
	return syscall.S_IFREG | 0644
}

func joinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

// inodeFromPath 用路径生成一个稳定的 inode 号。
// FUSE 要求 inode 号在文件系统内唯一且稳定，用路径的简单哈希即可。
func inodeFromPath(path string) uint64 {
	var h uint64 = 1469598103934665603 // FNV offset basis
	for _, c := range path {
		h ^= uint64(c)
		h *= 1099511628211 // FNV prime
	}
	if h == 0 {
		return 1 // root
	}
	return h
}

// errToErrno 将 client 错误映射到 FUSE errno。
func errToErrno(err error) syscall.Errno {
	msg := err.Error()
	switch {
	case contains(msg, "not found"), contains(msg, "no such file"):
		return syscall.ENOENT
	case contains(msg, "already exists"):
		return syscall.EEXIST
	case contains(msg, "is a directory"), contains(msg, "not a directory"):
		return syscall.ENOTDIR
	case contains(msg, "permission"):
		return syscall.EACCES
	default:
		return syscall.EIO
	}
}

func contains(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// 编译期接口断言：确保 fuseNode 实现了 go-fuse 的 inode 层接口
var _ fs.NodeGetattrer = (*fuseNode)(nil)
var _ fs.NodeLookuper = (*fuseNode)(nil)
var _ fs.NodeReaddirer = (*fuseNode)(nil)
var _ fs.NodeOpener = (*fuseNode)(nil)
var _ fs.NodeCreater = (*fuseNode)(nil)
var _ fs.NodeMkdirer = (*fuseNode)(nil)
var _ fs.NodeUnlinker = (*fuseNode)(nil)
var _ fs.NodeRenamer = (*fuseNode)(nil)
