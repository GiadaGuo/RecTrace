#!/bin/bash
# restart.sh
# -----------
# 一键重启整个实时数据链路 Demo 环境
# 包含：停止旧环境 -> 启动基础设施 -> 创建 Topics -> 初始化维度数据 -> 提交 Flink Jobs
#
# Usage:
#   bash scripts/restart.sh            # 完整重启（停止 + 启动 + 提交 Job + 初始化数据）
#   bash scripts/restart.sh --skip-dim # 跳过维度数据初始化（维度数据没变时使用）
#   bash scripts/restart.sh --jobs-only # 只重新提交 Jobs，不重启基础设施

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR="$PROJECT_DIR/flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar"

SKIP_DIM=false
JOBS_ONLY=false

for arg in "$@"; do
  case $arg in
    --skip-dim)  SKIP_DIM=true ;;
    --jobs-only) JOBS_ONLY=true ;;
  esac
done

# ── 颜色输出 ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

step() { echo -e "\n${GREEN}[$(date '+%H:%M:%S')] >>> $1${NC}"; }
warn() { echo -e "${YELLOW}[WARN] $1${NC}"; }
die()  { echo -e "${RED}[ERROR] $1${NC}"; exit 1; }

# ── 检查 JAR 是否存在 ──────────────────────────────────────────────────────────
check_jar() {
  if [ ! -f "$JAR" ]; then
    warn "JAR not found: $JAR"
    step "Building JAR with Maven ..."
    cd "$PROJECT_DIR"
    mvn clean package -DskipTests -pl flink-jobs -q || die "Maven build failed"
    echo "  Build complete: $JAR"
  else
    echo "  Using existing JAR: $(basename $JAR)"
  fi
}

# ── 等待服务就绪 ───────────────────────────────────────────────────────────────
wait_for_kafka() {
  echo -n "  Waiting for Kafka"
  for i in $(seq 1 30); do
    if docker exec kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      echo " ready"
      return 0
    fi
    echo -n "."
    sleep 3
  done
  die "Kafka did not become ready in time"
}

wait_for_flink() {
  echo -n "  Waiting for Flink JobManager"
  for i in $(seq 1 20); do
    if docker exec flink-jobmanager flink list >/dev/null 2>&1; then
      echo " ready"
      return 0
    fi
    echo -n "."
    sleep 3
  done
  die "Flink JobManager did not become ready in time"
}

# ── 主流程 ────────────────────────────────────────────────────────────────────
cd "$PROJECT_DIR"

if [ "$JOBS_ONLY" = false ]; then
  # 1. 停止旧环境
  step "Stopping existing environment ..."
  docker compose down --remove-orphans
  echo "  Done"

  # 2. 启动基础设施
  step "Starting infrastructure (Kafka + Redis + Flink) ..."
  docker compose up -d
  echo "  Containers started"

  # 3. 等待 Kafka 就绪
  step "Waiting for services to be healthy ..."
  wait_for_kafka
  wait_for_flink

  # 4. 创建 Kafka Topics
  step "Creating Kafka topics ..."
  bash "$SCRIPT_DIR/create_topics.sh"

  # 5. 初始化维度数据
  if [ "$SKIP_DIM" = false ]; then
    step "Initializing dimension data (SQLite + Redis) ..."
    python3 "$SCRIPT_DIR/init_dim_data.py"
  else
    warn "Skipping dimension data initialization (--skip-dim)"
  fi
fi

# 6. 检查 Redis 维度数据（--jobs-only 时 Redis 可能因上次完整重启而被清空）
if [ "$JOBS_ONLY" = true ]; then
  REDIS_SIZE=$(docker exec redis redis-cli DBSIZE 2>/dev/null | tr -d '[:space:]')
  if [ "$REDIS_SIZE" = "0" ] || [ -z "$REDIS_SIZE" ]; then
    warn "Redis is empty! Running dimension data initialization ..."
    python3 "$SCRIPT_DIR/init_dim_data.py" || die "init_dim_data.py failed"
  else
    echo "  Redis has $REDIS_SIZE keys, skipping dim init"
  fi
fi

# 7. 检查 JAR
step "Checking Flink JAR ..."
check_jar

# 8. 复制 JAR 到 Flink 容器
step "Copying JAR to Flink JobManager ..."
docker cp "$JAR" flink-jobmanager:/opt/flink/usrlib/flink-jobs-1.0-SNAPSHOT.jar
echo "  Copied"

# 8. 取消已有的 Flink Jobs（如果有）
step "Cancelling any running Flink jobs ..."
RUNNING=$(docker exec flink-jobmanager flink list 2>/dev/null | grep -E "^\d{2}\.\d{2}" | awk '{print $4}' || true)
if [ -n "$RUNNING" ]; then
  for JOB_ID in $RUNNING; do
    echo "  Cancelling job: $JOB_ID"
    docker exec flink-jobmanager flink cancel "$JOB_ID" 2>/dev/null || true
  done
  sleep 3
else
  echo "  No running jobs to cancel"
fi

# 9. 提交三个 Flink Jobs
step "Submitting Flink Job1: DimJoinJob ..."
docker exec flink-jobmanager flink run -d \
  -c com.demo.job1.DimJoinJob \
  /opt/flink/usrlib/flink-jobs-1.0-SNAPSHOT.jar
echo "  Job1 submitted"

step "Submitting Flink Job2: WindowAggJob ..."
docker exec flink-jobmanager flink run -d \
  -c com.demo.job2.WindowAggJob \
  /opt/flink/usrlib/flink-jobs-1.0-SNAPSHOT.jar
echo "  Job2 submitted"

step "Submitting Flink Job3: RealtimeFeatureJob ..."
docker exec flink-jobmanager flink run -d \
  -c com.demo.job3.RealtimeFeatureJob \
  /opt/flink/usrlib/flink-jobs-1.0-SNAPSHOT.jar
echo "  Job3 submitted"

# 10. 完成
echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  All done! Pipeline is running.${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo "  Flink Web UI : http://localhost:8081"
echo ""
echo "  Start mock producer:"
echo "    python3 scripts/mock_producer.py"
echo "    python3 scripts/mock_producer.py --burst   # backpressure test"
echo ""
echo "  Verify output:"
echo "    docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \\"
echo "      --bootstrap-server localhost:9092 --topic dwd_behavior_with_dim \\"
echo "      --from-beginning --max-messages 5"
echo ""
