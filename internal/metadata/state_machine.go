package metadata

import (
	"encoding/json"
	"fmt"
	"time"
)

// Apply 应用一条已提交的日志命令到本地状态机。
// 此方法由 Raft 保证在所有节点上以相同顺序调用，且仅做确定性的内存修改。
func (mds *MetadataServer) Apply(op string, data []byte) error {
	var payload commandPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("unmarshal command payload failed: %w", err)
	}

	mds.mu.Lock()
	defer mds.mu.Unlock()

	switch op {
	case OpHeartbeat:
		return mds.applyHeartbeat(&payload)
	case OpRemoveDataNode:
		return mds.applyRemoveDataNode(&payload)
	case OpMkdir:
		return mds.applyMkdir(&payload)
	case OpCreateFile:
		return mds.applyCreateFile(&payload)
	case OpCompleteFile:
		return mds.applyCompleteFile(&payload)
	case OpDelete:
		return mds.applyDelete(&payload)
	case OpRename:
		return mds.applyRename(&payload)
	case OpUpdateSize:
		return mds.applyUpdateSize(&payload)
	case OpUpdateReplicas:
		return mds.applyUpdateReplicas(&payload)
	case OpUpdateStatus:
		return mds.applyUpdateStatus(&payload)
	default:
		return fmt.Errorf("unknown command op: %s", op)
	}
}

// Snapshot 生成当前状态机的快照数据（供 Raft 压缩日志用）。
func (mds *MetadataServer) Snapshot() ([]byte, error) {
	mds.mu.RLock()
	defer mds.mu.RUnlock()
	snap := metaSnapshot{
		Root:          mds.root.toSnapshot(),
		DataNodes:     mds.dataNodes,
		LastHeartbeat: mds.lastHeartbeat,
	}
	return json.Marshal(snap)
}

// Restore 从快照数据恢复状态机（Raft 节点启动时调用）。
func (mds *MetadataServer) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var snap metaSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("unmarshal snapshot failed: %w", err)
	}
	mds.mu.Lock()
	defer mds.mu.Unlock()
	mds.root = snap.toEntry()
	mds.dataNodes = snap.DataNodes
	if snap.LastHeartbeat != nil {
		mds.lastHeartbeat = snap.LastHeartbeat
	} else {
		mds.lastHeartbeat = make(map[string]time.Time)
	}
	mds.logger.Info("state machine restored from snapshot", "data_nodes", len(mds.dataNodes))
	return nil
}
