package fuse

import (
	"context"
	"io"
	"log/slog"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/lwwgo/fstorex/internal/client"
)

// fuseFileHandle 是 fd 层（file descriptor）的实现，对应 POSIX 中的
// struct file，是一次 Open 操作创建的临时会话，用完即销毁。
//
// 与 fuseNode 的本质区别：
//   - fuseNode 是 inode 层，代表"文件本身"，持久存在（只要不删除就一直有）
//   - fuseFileHandle 是 fd 层，代表"某次打开会话"，open 创建、close 销毁
//
// 生命周期：
//
//	fuseNode.Open() → 返回 fuseFileHandle → 用户 Read/Write/Fsync → Release() 销毁
//
// 关键特性：
//   - 同一个 fuseNode 可以被 Open 多次，产生多个独立的 fuseFileHandle
//   - 每个 fd 持有自己的 *client.FileHandle，独立读写位置、独立关闭
//   - fd 关闭不影响 inode 本身，也不影响其他已打开的 fd
//
// 为什么不放在 fuseNode 上？因为 inode 层和 fd 层是 POSIX 文件系统的核心
// 分层：inode 层管"文件的存在/命名/属性"，fd 层管"这次打开的数据读写"。
// 分开才能支持"同一文件被打开多次、各自独立 offset"。
type fuseFileHandle struct {
	fh     *client.FileHandle // 底层 FStoreX 文件句柄，Open 时创建、Close 时关闭
	path   string             // 仅用于日志
	logger *slog.Logger
}

// Read 从指定 offset 读取数据，是 fd 层读操作的入口。
func (h *fuseFileHandle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := h.fh.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		h.logger.Warn("fuse read failed", "path", h.path, "offset", off, "error", err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

// Write 向指定 offset 写入数据，是 fd 层写操作的入口。
func (h *fuseFileHandle) Write(_ context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := h.fh.WriteAt(data, off)
	if err != nil {
		h.logger.Warn("fuse write failed", "path", h.path, "offset", off, "error", err)
		return 0, syscall.EIO
	}
	return uint32(n), 0
}

// Fsync 强制刷盘，对应 fsync(2)。
func (h *fuseFileHandle) Fsync(_ context.Context, _ uint32) syscall.Errno {
	if err := h.fh.Sync(); err != nil {
		h.logger.Warn("fuse fsync failed", "path", h.path, "error", err)
		return syscall.EIO
	}
	return 0
}

// Release 关闭文件句柄，是 fd 生命周期的终点。
//
// 注意：FUSE 有 Flush 和 Release 两个回调，Flush 可能被调用多次，
// Release 保证只调一次，所以 Close 必须映射到 Release 而不是 Flush。
func (h *fuseFileHandle) Release(_ context.Context) syscall.Errno {
	if err := h.fh.Close(); err != nil {
		h.logger.Warn("fuse release failed", "path", h.path, "error", err)
		return syscall.EIO
	}
	return 0
}

// 编译期接口断言：确保 fuseFileHandle 实现了 go-fuse 的 fd 层接口
var _ fs.FileReader = (*fuseFileHandle)(nil)
var _ fs.FileWriter = (*fuseFileHandle)(nil)
var _ fs.FileFsyncer = (*fuseFileHandle)(nil)
var _ fs.FileReleaser = (*fuseFileHandle)(nil)
