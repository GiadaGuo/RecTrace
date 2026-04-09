#!/bin/bash
# stop.sh
# -------
# 关停所有 Flink Jobs，然后停止 Docker 容器
#
# Usage:
#   bash scripts/stop.sh              # 取消 Jobs + 停止所有容器
#   bash scripts/stop.sh --jobs-only  # 只取消 Jobs，保留容器运行

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

JOBS_ONLY=false
for arg in "$@"; do
  case $arg in
    --jobs-only) JOBS_ONLY=true ;;
  esac
done

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

step() { echo -e "\n${GREEN}[$(date '+%H:%M:%S')] >>> $1${NC}"; }
warn() { echo -e "${YELLOW}[WARN] $1${NC}"; }

# 1. 取消所有运行中的 Flink Jobs
step "Cancelling running Flink jobs ..."
RUNNING=$(docker exec flink-jobmanager flink list 2>/dev/null | grep -E "^\d{2}\.\d{2}" | awk '{print $4}' || true)
if [ -n "$RUNNING" ]; then
  for JOB_ID in $RUNNING; do
    echo "  Cancelling job: $JOB_ID"
    docker exec flink-jobmanager flink cancel "$JOB_ID" 2>/dev/null || true
  done
  echo "  All jobs cancelled"
else
  echo "  No running jobs"
fi

# 2. 停止 Docker 容器（除非 --jobs-only）
if [ "$JOBS_ONLY" = false ]; then
  step "Stopping Docker containers ..."
  cd "$PROJECT_DIR"
  docker compose down --remove-orphans
  echo "  All containers stopped"
fi

echo ""
echo -e "${GREEN}Done. To restart: bash scripts/restart.sh${NC}"
echo ""
