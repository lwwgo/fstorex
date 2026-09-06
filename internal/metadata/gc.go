package metadata

import (
	"net/rpc"
	"strings"
	"time"

	"github.com/lwwgo/fstorex/internal/types"
)

const (
	// gcInterval is how often the leader scans for orphan data on DataNodes.
	gcInterval = 5 * time.Minute

	// gcSafetyWindow is the grace period: files modified within this window before
	// GC start are skipped to avoid deleting files created during the GC scan
	// (which may not yet appear in the validPaths snapshot).
	gcSafetyWindow = 30 * time.Second
)

// startGC starts the background garbage collection goroutine.
// Only the leader should run GC; followers do nothing.
func (mds *MetadataServer) startGC() {
	go func() {
		ticker := time.NewTicker(gcInterval)
		defer ticker.Stop()
		for range ticker.C {
			if !mds.IsLeader() {
				continue
			}
			mds.runGC()
		}
	}()
	mds.logger.Info("background orphan GC started", "interval", gcInterval)
}

// runGC performs one GC cycle: scan all DataNodes and delete orphan files.
func (mds *MetadataServer) runGC() {
	mds.logger.Debug("GC cycle started")

	// gcStart marks the beginning of this cycle. Files modified after
	// gcStart - gcSafetyWindow are protected to avoid deleting files
	// created during the GC scan (they may not yet be in validPaths).
	gcStart := time.Now()

	// 0. Health check: remove DataNodes that haven't heartbeat for too long
	mds.checkDataNodeHealth()

	// 1. Collect all valid FileIDs from metadata tree (identity model, not path-based)
	validFileIDs := mds.collectValidFileIDs()
	mds.logger.Debug("collected valid file ids", "count", len(validFileIDs))

	// 2. Get all registered DataNodes
	mds.mu.RLock()
	nodes := make([]string, len(mds.dataNodes))
	copy(nodes, mds.dataNodes)
	mds.mu.RUnlock()

	// 3. For each DataNode, list its files and delete orphans
	for _, addr := range nodes {
		mds.garbageCollectNode(addr, validFileIDs, gcStart)
	}

	mds.logger.Debug("GC cycle finished")
}

// checkDataNodeHealth removes DataNodes whose last heartbeat exceeds the timeout.
// Removal goes through Raft so all nodes agree on the cluster membership.
func (mds *MetadataServer) checkDataNodeHealth() {
	now := time.Now()
	cutoff := now.Add(-mds.heartbeatTimeout)

	mds.mu.RLock()
	deadNodes := make(map[string]time.Time)
	for addr, last := range mds.lastHeartbeat {
		if last.Before(cutoff) {
			deadNodes[addr] = last
		}
	}
	mds.mu.RUnlock()

	for addr, last := range deadNodes {
		mds.logger.Warn("data node heartbeat timeout, removing",
			"addr", addr,
			"last_heartbeat", last,
			"elapsed", now.Sub(last).Round(time.Second),
			"timeout", mds.heartbeatTimeout)
		payload := &commandPayload{Op: OpRemoveDataNode, Addr: addr}
		if err := mds.submitCommand(payload); err != nil {
			mds.logger.Warn("failed to remove dead data node via raft", "addr", addr, "error", err)
		}
	}
}

// collectValidFileIDs walks the metadata tree and returns all valid FileIDs.
// This is the identity-based GC model: DataNode physical files are named
// /data/{FileID}, so matching by FileID (not path) is the only correct way
// to identify orphans after rename/unlink operations.
func (mds *MetadataServer) collectValidFileIDs() map[string]bool {
	mds.mu.RLock()
	defer mds.mu.RUnlock()

	ids := make(map[string]bool)
	mds.collectFileIDsRecursive(mds.root, ids)
	return ids
}

func (mds *MetadataServer) collectFileIDsRecursive(e *entry, ids map[string]bool) {
	if e == nil {
		return
	}
	if !e.isDir {
		if e.fileID != "" {
			ids[e.fileID] = true
		}
		return
	}
	for _, child := range e.children {
		mds.collectFileIDsRecursive(child, ids)
	}
}

// garbageCollectNode connects to a DataNode, lists its files, and deletes orphans.
// Matching is by FileID (identity model): NodeFile.Path is /data/{FileID}, so we
// strip the prefix and compare against validFileIDs from the metadata tree.
// gcStart is the time the current GC cycle started; files modified after
// gcStart - gcSafetyWindow are skipped to protect files created during the scan.
func (mds *MetadataServer) garbageCollectNode(addr string, validFileIDs map[string]bool, gcStart time.Time) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		mds.logger.Warn("GC: cannot connect to data node, skipping", "node", addr, "error", err)
		return
	}
	defer client.Close()

	var nodeFiles []types.NodeFile
	if err := client.Call("DataService.ListAllPaths", struct{}{}, &nodeFiles); err != nil {
		mds.logger.Warn("GC: failed to list files from data node", "node", addr, "error", err)
		return
	}

	// Files modified after this cutoff are too new to be safe to delete.
	cutoff := gcStart.Add(-gcSafetyWindow)

	deleted := 0
	skippedNew := 0
	for _, nf := range nodeFiles {
		// Extract FileID from physical path: /data/{FileID}
		fileID := strings.TrimPrefix(nf.Path, "/data/")
		if fileID != "" && validFileIDs[fileID] {
			continue // still referenced by metadata, keep it
		}
		// mtime protection: skip recently modified files that may have been
		// created during the GC scan but not yet in validFileIDs.
		if nf.ModTime.After(cutoff) {
			skippedNew++
			continue
		}
		// Orphan: delete from this DataNode
		var reply bool
		if err := client.Call("DataService.DeleteData", nf.Path, &reply); err != nil {
			mds.logger.Warn("GC: failed to delete orphan", "node", addr, "path", nf.Path, "error", err)
			continue
		}
		deleted++
		mds.logger.Info("GC: deleted orphan file", "node", addr, "path", nf.Path)
	}

	mds.logger.Info("GC: data node cleaned", "node", addr, "orphans_deleted", deleted, "skipped_recent", skippedNew, "total_files", len(nodeFiles))
}

// TriggerGC is the RPC handler for manually triggering one GC cycle.
// Only the leader can run GC; followers return a redirect error.
func (mds *MetadataServer) TriggerGC(_ struct{}, reply *bool) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}
	go mds.runGC()
	*reply = true
	mds.logger.Info("manual GC triggered")
	return nil
}
