#!/usr/bin/env bash
# start.sh — 分布式文件系统演示脚本（Raft 高可用版）
# 启动 3 个 MDS（Raft 集群）+ 2 个 DataNode，然后运行一系列客户端命令演示完整流程。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"
DATA_ROOT="/tmp/distributed-fs-demo"

# 颜色
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${CYAN}[demo]${NC} $*"; }
success() { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }

# 清理旧进程
cleanup() {
    log "stopping all nodes..."
    [[ -f "$DATA_ROOT/.pids" ]] && while read -r pid; do kill "$pid" 2>/dev/null || true; done < "$DATA_ROOT/.pids"
    rm -rf "$DATA_ROOT"
    success "cleanup done"
}

if [[ "${1:-}" == "clean" ]]; then
    cleanup
    exit 0
fi

# 检查是否已构建
if [[ ! -f "$BIN_DIR/fstorex-mds" || ! -f "$BIN_DIR/fstorex-dn" || ! -f "$BIN_DIR/fstorex" ]]; then
    log "binaries not found, building..."
    mkdir -p "$BIN_DIR"
    (cd "$SCRIPT_DIR" && go build -o "$BIN_DIR/fstorex-mds" ./cmd/fstorex-mds)
    (cd "$SCRIPT_DIR" && go build -o "$BIN_DIR/fstorex-dn" ./cmd/fstorex-dn)
    (cd "$SCRIPT_DIR" && go build -o "$BIN_DIR/fstorex" ./cmd/fstorex)
fi

# 清理旧数据
rm -rf "$DATA_ROOT"
mkdir -p "$DATA_ROOT/mds1/wal" "$DATA_ROOT/mds1/snap"
mkdir -p "$DATA_ROOT/mds2/wal" "$DATA_ROOT/mds2/snap"
mkdir -p "$DATA_ROOT/mds3/wal" "$DATA_ROOT/mds3/snap"
mkdir -p "$DATA_ROOT/dn1" "$DATA_ROOT/dn2"
> "$DATA_ROOT/.pids"

# 启动 3 个 MDS 节点（Raft 集群）
log "starting metadata server 1 (localhost:9001)..."
"$BIN_DIR/fstorex-mds" -id=localhost:9001 -peers=localhost:9002,localhost:9003 \
    -waldir="$DATA_ROOT/mds1/wal" -snapdir="$DATA_ROOT/mds1/snap" > "$DATA_ROOT/mds1.log" 2>&1 &
echo $! >> "$DATA_ROOT/.pids"

log "starting metadata server 2 (localhost:9002)..."
"$BIN_DIR/fstorex-mds" -id=localhost:9002 -peers=localhost:9001,localhost:9003 \
    -waldir="$DATA_ROOT/mds2/wal" -snapdir="$DATA_ROOT/mds2/snap" > "$DATA_ROOT/mds2.log" 2>&1 &
echo $! >> "$DATA_ROOT/.pids"

log "starting metadata server 3 (localhost:9003)..."
"$BIN_DIR/fstorex-mds" -id=localhost:9003 -peers=localhost:9001,localhost:9002 \
    -waldir="$DATA_ROOT/mds3/wal" -snapdir="$DATA_ROOT/mds3/snap" > "$DATA_ROOT/mds3.log" 2>&1 &
echo $! >> "$DATA_ROOT/.pids"

# 等待 Raft 选举
log "waiting for Raft leader election..."
sleep 5

# 启动 2 个 DataNode（连任意 MDS 节点，会自动重定向到 leader）
log "starting data node 1 on :9101..."
"$BIN_DIR/fstorex-dn" -addr=:9101 -mds=localhost:9001 -datadir="$DATA_ROOT/dn1" > "$DATA_ROOT/dn1.log" 2>&1 &
echo $! >> "$DATA_ROOT/.pids"

log "starting data node 2 on :9102..."
"$BIN_DIR/fstorex-dn" -addr=:9102 -mds=localhost:9001 -datadir="$DATA_ROOT/dn2" > "$DATA_ROOT/dn2.log" 2>&1 &
echo $! >> "$DATA_ROOT/.pids"
sleep 2

# 客户端连任意一个 MDS 节点都行
MDS="localhost:9001"

echo ""
echo -e "${GREEN}========== Distributed File Storage Demo (Raft HA) ==========${NC}"
echo ""

# 1. 查看数据节点
log "1. List registered data nodes (via $MDS)"
"$BIN_DIR/fstorex" -mds="$MDS" nodes

# 2. 创建目录
echo ""
log "2. Create directories"
"$BIN_DIR/fstorex" -mds="$MDS" mkdir /docs
"$BIN_DIR/fstorex" -mds="$MDS" mkdir /images

# 3. 上传文件
echo ""
log "3. Upload files"
echo "Hello FStoreX! This is a Raft HA demo." > "$DATA_ROOT/test-upload.txt"
echo -n "FAKE_PNG_DATA_123456" > "$DATA_ROOT/test-image.bin"
"$BIN_DIR/fstorex" -mds="$MDS" put "$DATA_ROOT/test-upload.txt" /docs/readme.md
"$BIN_DIR/fstorex" -mds="$MDS" put "$DATA_ROOT/test-image.bin" /images/logo.png

# Give Raft time to replicate logs to followers before read tests
sleep 2

# 4. 列出目录
echo ""
log "4. List root directory"
"$BIN_DIR/fstorex" -mds="$MDS" ls /

echo ""
log "5. List /docs directory"
"$BIN_DIR/fstorex" -mds="$MDS" ls /docs

# 5. stat
echo ""
log "6. Stat a file"
"$BIN_DIR/fstorex" -mds="$MDS" stat /docs/readme.md

# 6. 查看副本分布
echo ""
log "7. Show replica locations for /docs/readme.md"
"$BIN_DIR/fstorex" -mds="$MDS" replicas /docs/readme.md

# 7. 下载
echo ""
log "8. Download file"
"$BIN_DIR/fstorex" -mds="$MDS" get /docs/readme.md "$DATA_ROOT/downloaded.txt"
echo "   Downloaded content: $(cat "$DATA_ROOT/downloaded.txt")"

# 8. 物理分布（验证文件存在于多个 DataNode）
echo ""
log "9. Physical distribution across data nodes (multi-replica verification)"
DN1_FILES=$(find "$DATA_ROOT/dn1" -type f 2>/dev/null | sed "s|$DATA_ROOT/dn1/||" | tr '\n' ' ')
DN2_FILES=$(find "$DATA_ROOT/dn2" -type f 2>/dev/null | sed "s|$DATA_ROOT/dn2/||" | tr '\n' ' ')
echo "   DataNode1 files: $DN1_FILES"
echo "   DataNode2 files: $DN2_FILES"
# 验证 readme.md 至少存在于 2 个节点（多副本）
REPLICA_COUNT=0
[[ "$DN1_FILES" == *readme.md* ]] && REPLICA_COUNT=$((REPLICA_COUNT+1))
[[ "$DN2_FILES" == *readme.md* ]] && REPLICA_COUNT=$((REPLICA_COUNT+1))
if [[ $REPLICA_COUNT -ge 2 ]]; then
    success "readme.md found on $REPLICA_COUNT data nodes (multi-replica OK)"
else
    warn "readme.md only found on $REPLICA_COUNT data node(s)"
fi

# 10. 删除
echo ""
log "10. Delete a file"
"$BIN_DIR/fstorex" -mds="$MDS" rm /images/logo.png
sleep 1
"$BIN_DIR/fstorex" -mds="$MDS" ls /images

# 11. 验证从不同 MDS 节点读（follower 也能读）
echo ""
log "11. Read from different MDS node (localhost:9002, should also work)"
"$BIN_DIR/fstorex" -mds=localhost:9002 ls /docs

echo ""
echo -e "${GREEN}========== Demo completed successfully ==========${NC}"
echo ""
log "running processes (use './start.sh clean' to stop):"
ps aux | grep -E 'fstorex-mds|fstorex-dn' | grep -v grep | awk '{print "  " $11, $12, $13, $14}'
