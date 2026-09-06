package metadata

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/lwwgo/fstorex/internal/types"
)

const (
	// replicateScanTimeout 是单次扫描+修复的整体超时，防止长时间阻塞。
	replicateScanTimeout = 5 * time.Minute

	// replicateBatchSize 是单次扫描最多收集的待修复任务数。
	// 批量收集避免每修一个文件就重新遍历整棵元数据树。
	replicateBatchSize = 16
)

// startReplicator 启动后台周期性补副本 goroutine。
// 仅 leader 实际执行扫描和修复；follower 空闲等待。
// 采用纯周期性扫描（不做事件触发），路径简单清晰，符合系统设计目标。
func (mds *MetadataServer) startReplicator() {
	go func() {
		ticker := time.NewTicker(mds.replicateInterval)
		defer ticker.Stop()
		for range ticker.C {
			if !mds.IsLeader() {
				continue
			}
			mds.runReplication()
		}
	}()
	mds.logger.Info("background re-replication reconciler started", "interval", mds.replicateInterval)
}

// runReplication 执行一轮补副本扫描+修复。
// 批量收集 under-replicated 文件（一次 BFS 最多收集 K 个），然后串行修复；
// 单文件多缺失副本在 repairFile 内并行拉取、一次性提交。
// 循环直到没有更多待修复文件或超时。
func (mds *MetadataServer) runReplication() {
	mds.logger.Debug("replication reconciler cycle started")
	start := time.Now()
	deadline := start.Add(replicateScanTimeout)

	for {
		if time.Now().After(deadline) {
			mds.logger.Warn("replication cycle timeout, stopping early", "elapsed", time.Since(start).Round(time.Second))
			break
		}

		// 一次 BFS 收集一批待修复任务 + 共享的存活节点集合
		tasks, aliveNodes := mds.findUnderReplicatedTasks(replicateBatchSize)
		if len(tasks) == 0 {
			break // 没有需要补副本的文件
		}

		for _, task := range tasks {
			// 修复前检查是否仍在 single-flight（同一轮内理论上不会重复，防御性检查）
			mds.repairingMu.Lock()
			if mds.repairing[task.fileID] {
				mds.repairingMu.Unlock()
				continue
			}
			mds.repairing[task.fileID] = true
			mds.repairingMu.Unlock()

			// 修复该文件
			if err := mds.repairFile(aliveNodes, task); err != nil {
				mds.logger.Warn("repair file failed, will retry next cycle",
					"path", task.path, "file_id", task.fileID, "error", err)
			}

			// 释放 single-flight 标记
			mds.repairingMu.Lock()
			delete(mds.repairing, task.fileID)
			mds.repairingMu.Unlock()
		}
	}

	mds.logger.Debug("replication reconciler cycle finished", "elapsed", time.Since(start).Round(time.Millisecond))
}

// repairTask 描述一个待补副本的文件。
type repairTask struct {
	path     string
	fileID   string
	size     int64
	replicas []types.Replica // 该文件当前的存活副本列表
}

// findUnderReplicatedTasks 一次 BFS 扫描元数据树，最多返回 k 个待补副本任务，
// 以及本轮扫描共享的存活 DataNode 集合。
// 只考虑 status == complete 且实际存活副本数 < replicaCount 的文件。
func (mds *MetadataServer) findUnderReplicatedTasks(k int) ([]*repairTask, map[string]bool) {
	mds.mu.RLock()
	defer mds.mu.RUnlock()

	// 当前存活的 DataNode 集合（本轮扫描共享）
	aliveNodes := make(map[string]bool, len(mds.dataNodes))
	for _, addr := range mds.dataNodes {
		aliveNodes[addr] = true
	}

	// single-flight 集合快照
	mds.repairingMu.Lock()
	repairing := make(map[string]bool, len(mds.repairing))
	for id, val := range mds.repairing {
		repairing[id] = val
	}
	mds.repairingMu.Unlock()

	var tasks []*repairTask
	type queueItem struct {
		entry *entry
		path  string
	}
	queue := []queueItem{{mds.root, "/"}}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		e := item.entry

		if e.isDir {
			for _, child := range e.children {
				queue = append(queue, queueItem{child, joinPath(item.path, child.name)})
			}
			continue
		}

		// 跳过 pending 文件（还在写，补了也不一致）
		if e.status == StatusPending {
			continue
		}
		// 跳过正在修复的文件（single-flight）
		if e.status == StatusRecovering {
			continue
		}
		if repairing[e.fileID] {
			continue
		}

		// 计算实际存活副本数
		aliveReplicas := 0
		var aliveReplicaList []types.Replica
		for _, r := range e.replicas {
			if aliveNodes[r.Addr] {
				aliveReplicas++
				aliveReplicaList = append(aliveReplicaList, r)
			}
		}

		// under-replicated：存活副本数 < desired replicaCount
		if aliveReplicas < mds.replicaCount {
			tasks = append(tasks, &repairTask{
				path:     item.path,
				fileID:   e.fileID,
				size:     e.size,
				replicas: aliveReplicaList,
			})
			if len(tasks) >= k {
				break // 达到批量上限，提前返回
			}
		}
	}
	return tasks, aliveNodes
}

// joinPath 拼接路径，避免使用 filepath.Join 的 OS 相关行为。
func joinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

// repairFile 执行单个文件的补副本。
// 状态机：complete → recovering → (parallel copy) → complete
// 若缺多个副本，并行拉取后一次性提交成功的副本；部分失败时提交成功的部分，
// 失败的副本下轮重试，不浪费已完成的工作。
func (mds *MetadataServer) repairFile(aliveNodes map[string]bool, task *repairTask) error {
	missing := mds.replicaCount - len(task.replicas)
	mds.logger.Info("start repairing under-replicated file",
		"path", task.path, "file_id", task.fileID,
		"alive_replicas", len(task.replicas), "desired", mds.replicaCount, "missing", missing)

	// 1. 切换状态为 recovering（Raft 提交，全局一致）
	if err := mds.submitCommand(&commandPayload{
		Op:        OpUpdateStatus,
		Path:      task.path,
		NewStatus: StatusRecovering,
	}); err != nil {
		return fmt.Errorf("switch to recovering: %w", err)
	}

	// 2. 选源副本（从存活副本中选一个）
	if len(task.replicas) == 0 {
		// 所有副本都死了，无法修复
		mds.logger.Error("all replicas dead, cannot repair", "file_id", task.fileID)
		mds.rollbackStatus(task.path)
		return fmt.Errorf("all replicas dead for file %s", task.fileID)
	}
	sourceReplica := task.replicas[0] // 选第一个存活副本

	// 3. 选多个目标 DN（排除已有副本的节点，排除 dead node）
	targetAddrs := mds.selectTargetNodes(task.replicas, aliveNodes, missing)
	if len(targetAddrs) == 0 {
		mds.logger.Warn("no available target data node, cannot repair", "file_id", task.fileID)
		mds.rollbackStatus(task.path)
		return fmt.Errorf("no available target data node")
	}

	// 4. 并行调用各目标 DN 的 ReplicateFile RPC（目标 DN 主动从源 DN 拉取）
	type copyResult struct {
		target types.Replica
		err    error
	}
	results := make([]copyResult, len(targetAddrs))
	var wg sync.WaitGroup
	for i, addr := range targetAddrs {
		wg.Add(1)
		go func(idx int, targetAddr string) {
			defer wg.Done()
			targetReplica := types.Replica{
				Addr:       targetAddr,
				RemotePath: "/data/" + task.fileID,
			}
			err := mds.triggerReplicate(sourceReplica, targetReplica, task.size)
			results[idx] = copyResult{target: targetReplica, err: err}
		}(i, addr)
	}
	wg.Wait()

	// 5. 收集成功复制的副本
	newReplicas := make([]types.Replica, 0, len(results))
	for _, r := range results {
		if r.err != nil {
			mds.logger.Warn("replicate to target failed, will retry next cycle",
				"file_id", task.fileID, "target", r.target.Addr, "error", r.err)
		} else {
			newReplicas = append(newReplicas, r.target)
		}
	}

	// 6. 数据先复制成功 → 提交 Raft 更新副本列表（只包含成功的副本）
	finalReplicas := append(task.replicas, newReplicas...)
	if err := mds.submitCommand(&commandPayload{
		Op:          OpUpdateReplicas,
		Path:        task.path,
		NewReplicas: finalReplicas,
	}); err != nil {
		return fmt.Errorf("update replicas: %w", err)
	}

	// 7. 切换状态回 complete
	if err := mds.submitCommand(&commandPayload{
		Op:        OpUpdateStatus,
		Path:      task.path,
		NewStatus: StatusComplete,
	}); err != nil {
		return fmt.Errorf("switch back to complete: %w", err)
	}

	mds.logger.Info("file repaired",
		"path", task.path, "file_id", task.fileID,
		"old_replicas", len(task.replicas), "new_replicas", len(finalReplicas),
		"copied", len(newReplicas), "failed", len(targetAddrs)-len(newReplicas))

	if len(newReplicas) == 0 {
		return fmt.Errorf("all %d target replicas failed to copy", len(targetAddrs))
	}
	return nil
}

// rollbackStatus 将文件状态从 recovering 回滚到 complete。
// 用于补副本中途失败的场景，避免文件永久卡在 recovering 状态。
func (mds *MetadataServer) rollbackStatus(path string) {
	if err := mds.submitCommand(&commandPayload{
		Op:        OpUpdateStatus,
		Path:      path,
		NewStatus: StatusComplete,
	}); err != nil {
		mds.logger.Error("rollback to complete failed", "path", path, "error", err)
	}
}

// selectTargetNodes 选择多个不同的目标 DataNode。
// 策略：排除已有副本的节点 + 排除 dead node，选最多 count 个可用节点。
func (mds *MetadataServer) selectTargetNodes(aliveReplicas []types.Replica, aliveNodes map[string]bool, count int) []string {
	if count <= 0 || len(aliveNodes) == 0 {
		return nil
	}

	// 已有副本的节点集合
	hasReplica := make(map[string]bool, len(aliveReplicas))
	for _, r := range aliveReplicas {
		hasReplica[r.Addr] = true
	}

	// 排除已有副本和 dead node，收集可用节点；凑够 count 个就提前返回
	candidates := make([]string, 0, count)
	for addr := range aliveNodes {
		if !hasReplica[addr] {
			candidates = append(candidates, addr)
			if len(candidates) >= count {
				break
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates
}

// triggerReplicate 调用目标 DN 的 ReplicateFile RPC，让目标 DN 主动从源 DN 拉取数据。
func (mds *MetadataServer) triggerReplicate(source, target types.Replica, fileSize int64) error {
	client, err := rpc.Dial("tcp", target.Addr)
	if err != nil {
		return fmt.Errorf("dial target data node %s: %w", target.Addr, err)
	}
	defer client.Close()

	args := &types.ReplicateFileArgs{
		SourceAddr:       source.Addr,
		SourceRemotePath: source.RemotePath,
		TargetRemotePath: target.RemotePath,
		FileSize:         fileSize,
	}
	var reply bool
	if err := client.Call("DataService.ReplicateFile", args, &reply); err != nil {
		return fmt.Errorf("replicate file RPC: %w", err)
	}
	if !reply {
		return fmt.Errorf("replicate file RPC returned false")
	}
	return nil
}
