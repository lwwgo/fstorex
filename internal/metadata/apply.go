package metadata

import (
	"fmt"
	"time"

	"github.com/lwwgo/fstorex/internal/types"
)

// applyHeartbeat 处理心跳：首次心跳自动注册，后续更新最后心跳时间。
func (mds *MetadataServer) applyHeartbeat(p *commandPayload) error {
	// 不存在则自动加入（即注册）
	found := false
	for _, a := range mds.dataNodes {
		if a == p.Addr {
			found = true
			break
		}
	}
	if !found {
		mds.dataNodes = append(mds.dataNodes, p.Addr)
	}
	// 更新心跳时间
	mds.lastHeartbeat[p.Addr] = time.Now()
	return nil
}

// applyRemoveDataNode 移除超时未心跳的数据节点。
func (mds *MetadataServer) applyRemoveDataNode(p *commandPayload) error {
	for i, a := range mds.dataNodes {
		if a == p.Addr {
			mds.dataNodes = append(mds.dataNodes[:i], mds.dataNodes[i+1:]...)
			break
		}
	}
	delete(mds.lastHeartbeat, p.Addr)
	return nil
}

func (mds *MetadataServer) applyMkdir(p *commandPayload) error {
	parts := splitPath(p.Path)
	if len(parts) == 0 {
		return fmt.Errorf("invalid path: %s", p.Path)
	}
	current := mds.root
	now := time.Now()

	// 遍历中间目录，必须已存在（POSIX mkdir 不隐式创建父目录）
	for i := 0; i < len(parts)-1; i++ {
		child, exists := current.children[parts[i]]
		if !exists || !child.isDir {
			return fmt.Errorf("parent directory not found: %s (at level %d: %s)", p.Path, i, parts[i])
		}
		current = child
	}

	// 最后一级：创建或校验
	lastPart := parts[len(parts)-1]
	if child, exists := current.children[lastPart]; exists {
		if !child.isDir {
			return fmt.Errorf("%s is not a directory: %s", p.Path, p.Path)
		}
		// 目录已存在，保留原 FileID 不变（幂等）
		return nil
	}

	current.children[lastPart] = &entry{
		name:      lastPart,
		isDir:     true,
		mode:      0755,
		createdAt: now,
		modTime:   now,
		fileID:    p.FileID, // 目录也分配稳定 FileID（inode 等价物）
		children:  make(map[string]*entry),
	}
	current.modTime = now
	return nil
}

func (mds *MetadataServer) applyCreateFile(p *commandPayload) error {
	parts := splitPath(p.Path)
	if len(parts) == 0 {
		return fmt.Errorf("invalid path: %s", p.Path)
	}
	dirParts := parts[:len(parts)-1]
	fileName := parts[len(parts)-1]

	current := mds.root
	for _, part := range dirParts {
		child, exists := current.children[part]
		if !exists || !child.isDir {
			return fmt.Errorf("parent directory not found: %s", p.Path)
		}
		current = child
	}

	// Existence check in Raft Apply (authoritative, serial) to avoid time of check to time of use race.
	existing, exists := current.children[fileName]
	if exists {
		if existing.isDir {
			return fmt.Errorf("%s is a directory, cannot create file with same name", p.Path)
		}
		// Default mode (CreateIfNotExist): reject if file already exists.
		if p.CreateMode != types.OverwriteIfExists {
			return fmt.Errorf("file already exists: %s", p.Path)
		}
		// Overwrite mode: replace metadata. NOTE: old replicas' data on DataNodes
		// becomes orphaned and requires separate garbage collection (known limitation).
	}

	now := time.Now()
	createdAt := now
	fileID := p.FileID // 新文件使用 payload 中的 FileID
	if exists {
		// POSIX O_TRUNC preserves creation time; only modTime updates.
		createdAt = existing.createdAt
		// FileID 不变：覆盖时保留旧对象身份，绝不重新生成（FileID 是 inode 等价物）
		fileID = existing.fileID
	}
	current.children[fileName] = &entry{
		name:      fileName,
		isDir:     false,
		size:      p.Size,
		mode:      0644,
		createdAt: createdAt,
		modTime:   now,
		status:    StatusPending,
		fileID:    fileID,
		replicas:  p.Replicas,
	}
	current.modTime = now
	return nil
}

func (mds *MetadataServer) applyCompleteFile(p *commandPayload) error {
	e := mds.lookup(p.Path)
	if e == nil {
		return fmt.Errorf("path not found: %s", p.Path)
	}
	if e.isDir {
		return fmt.Errorf("%s is a directory, cannot complete", p.Path)
	}
	e.status = StatusComplete
	e.modTime = time.Now()
	return nil
}

func (mds *MetadataServer) applyDelete(p *commandPayload) error {
	parts := splitPath(p.Path)
	if len(parts) == 0 {
		return fmt.Errorf("cannot delete root")
	}
	dirParts := parts[:len(parts)-1]
	fileName := parts[len(parts)-1]

	current := mds.root
	for _, part := range dirParts {
		child, exists := current.children[part]
		if !exists || !child.isDir {
			return fmt.Errorf("path not found: %s", p.Path)
		}
		current = child
	}

	if _, exists := current.children[fileName]; !exists {
		return fmt.Errorf("path not found: %s", p.Path)
	}
	delete(current.children, fileName)
	current.modTime = time.Now()
	return nil
}

// applyRename renames/moves a file or directory.
// FileID stays unchanged (inode identity preserved); DataNode physical paths are never touched.
// If dst exists as a file, it's overwritten (old entry's replicas become GC orphans).
// If dst exists as a directory, the operation is rejected.
func (mds *MetadataServer) applyRename(p *commandPayload) error {
	srcParts := splitPath(p.Path)
	dstParts := splitPath(p.DstPath)
	if len(srcParts) == 0 || len(dstParts) == 0 {
		return fmt.Errorf("invalid path: src=%s dst=%s", p.Path, p.DstPath)
	}

	srcFileName := srcParts[len(srcParts)-1]
	dstFileName := dstParts[len(dstParts)-1]
	srcDirParts := srcParts[:len(srcParts)-1]
	dstDirParts := dstParts[:len(dstParts)-1]

	// Find src parent
	srcParent := mds.root
	for _, part := range srcDirParts {
		child, exists := srcParent.children[part]
		if !exists || !child.isDir {
			return fmt.Errorf("source path not found: %s", p.Path)
		}
		srcParent = child
	}
	srcEntry, exists := srcParent.children[srcFileName]
	if !exists {
		return fmt.Errorf("source path not found: %s", p.Path)
	}

	// Prevent directory cycles: moving a directory into its own subtree would
	// create a cycle that overflows recursive traversal (GC, ListDir, etc.).
	if srcEntry.isDir {
		// Walk dst parent chain; if we encounter srcEntry, reject.
		ancestor := mds.root
		ancestorFound := false
		for _, part := range dstDirParts {
			if ancestor == srcEntry {
				ancestorFound = true
				break
			}
			child, exists := ancestor.children[part]
			if !exists || !child.isDir {
				break // dst parent doesn't exist yet, no cycle possible
			}
			ancestor = child
		}
		if ancestorFound || ancestor == srcEntry {
			return fmt.Errorf("cannot move directory into its own subtree: %s -> %s", p.Path, p.DstPath)
		}
	}

	// Find dst parent
	dstParent := mds.root
	for _, part := range dstDirParts {
		child, exists := dstParent.children[part]
		if !exists || !child.isDir {
			return fmt.Errorf("destination parent directory not found: %s", p.DstPath)
		}
		dstParent = child
	}

	// Check dst conflict
	if dstEntry, exists := dstParent.children[dstFileName]; exists {
		if dstEntry.isDir {
			return fmt.Errorf("destination is an existing directory: %s", p.DstPath)
		}
		// dst is an existing file: overwrite it. Old entry's replicas become
		// orphans and will be cleaned by GC based on FileID matching.
		delete(dstParent.children, dstFileName)
	}

	// Detach from src parent, attach to dst parent with new name.
	// FileID, replicas, createdAt, status all stay unchanged.
	delete(srcParent.children, srcFileName)
	srcEntry.name = dstFileName
	dstParent.children[dstFileName] = srcEntry

	now := time.Now()
	srcParent.modTime = now
	dstParent.modTime = now
	srcEntry.modTime = now
	return nil
}

// applyUpdateSize updates only the file's size and modTime.
// Does not touch replicas or FileID.
func (mds *MetadataServer) applyUpdateSize(p *commandPayload) error {
	e := mds.lookup(p.Path)
	if e == nil {
		return fmt.Errorf("path not found: %s", p.Path)
	}
	if e.isDir {
		return fmt.Errorf("%s is a directory, cannot update size", p.Path)
	}
	// Recovering 状态拒绝写（补副本期间禁止写，保证新副本数据一致）
	if e.status == StatusRecovering {
		return fmt.Errorf("file is recovering, write not allowed: %s", p.Path)
	}
	e.size = p.NewSize
	e.modTime = time.Now()
	return nil
}

// applyUpdateReplicas 原子替换文件的完整副本列表（补副本完成后调用）。
// 直接替换而非增删，避免多次 Raft op 的竞态。
func (mds *MetadataServer) applyUpdateReplicas(p *commandPayload) error {
	e := mds.lookup(p.Path)
	if e == nil {
		return fmt.Errorf("path not found: %s", p.Path)
	}
	if e.isDir {
		return fmt.Errorf("%s is a directory, cannot update replicas", p.Path)
	}
	if len(p.NewReplicas) == 0 {
		return fmt.Errorf("new replicas must not be empty: %s", p.Path)
	}
	e.replicas = p.NewReplicas
	e.modTime = time.Now()
	return nil
}

// applyUpdateStatus 切换文件状态（complete ↔ recovering）。
// 用于补副本状态机：complete → recovering（开始补）→ complete（补完）。
func (mds *MetadataServer) applyUpdateStatus(p *commandPayload) error {
	e := mds.lookup(p.Path)
	if e == nil {
		return fmt.Errorf("path not found: %s", p.Path)
	}
	if e.isDir {
		return fmt.Errorf("%s is a directory, cannot update status", p.Path)
	}
	if p.NewStatus != StatusComplete && p.NewStatus != StatusRecovering && p.NewStatus != StatusPending {
		return fmt.Errorf("invalid target status: %s", p.NewStatus)
	}
	e.status = p.NewStatus
	e.modTime = time.Now()
	return nil
}
