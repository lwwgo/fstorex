package fuse

import (
	"log/slog"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/lwwgo/fstorex/internal/client"
)

// Mount 挂载 FStoreX 到本地目录。
//
//	mdsAddr   — MDS 地址（任意节点，自动重定向到 leader）
//	mountPoint — 本地挂载点（必须已存在）
//	logger     — 日志器
//
// 返回一个 unmount 函数和错误。调用方应该在收到信号时调 unmount。
func Mount(mdsAddr, mountPoint string, logger *slog.Logger) (func() error, error) {
	c := client.NewClient(mdsAddr, logger)

	// root node：路径 "/"
	root := &fuseNode{
		client: c,
		path:   "/",
		logger: logger,
	}

	// 挂载选项
	options := &fs.Options{
		AttrTimeout:  &defaultTimeout,
		EntryTimeout: &defaultTimeout,
	}
	options.Name = "fstorex"
	options.Options = []string{"default_permissions"}

	server, err := fs.Mount(mountPoint, root, options)
	if err != nil {
		return nil, err
	}

	logger.Info("fuse mounted", "mount_point", mountPoint, "mds", mdsAddr)

	unmount := func() error {
		logger.Info("unmounting", "mount_point", mountPoint)
		return server.Unmount()
	}

	return unmount, nil
}

// defaultTimeout 是告诉内核 VFS 缓存文件属性和目录项的时长。
//
// 设此值后，内核在有效期内（AttrTimeout 缓存 Getattr 结果，
// EntryTimeout 缓存 Lookup 结果）不会重复向 FUSE 进程发请求，
// 直接用自己的缓存应答，从而大幅减少 MDS RPC 调用。
//
// 取值权衡：
//   - 0：不缓存，每次 stat/ls 都打 MDS，性能差
//   - 1s（当前）：平衡性能与一致性，MDS 压力小 10 倍以上
//   - 过大：内核缓存过久，其他客户端的变更本地不可见
var defaultTimeout = 1 * time.Second
