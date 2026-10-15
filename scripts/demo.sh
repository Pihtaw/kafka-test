#!/usr/bin/env bash
# Демо всего цикла на локальной Kafka:
#   Kafka → Seeder (15 событий) → проверка и план → отправка по плану → побайтовая сверка → повторный запуск
#
#   scripts/demo.sh            из корня репозитория
#
# Нужны Docker и .NET 10 SDK.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPLAY="$ROOT/KafkaReplay/src/KafkaReplay"
OUT="$ROOT/KafkaReplay/src/KafkaReplay/run-demo-$(date +%Y%m%d-%H%M%S)"

echo "== 1. Kafka"
docker compose -f "$ROOT/docker-compose.yml" up -d kafka
# ждём, пока брокер начнёт отвечать
for _ in $(seq 1 30); do
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break
    sleep 2
done

echo "== 2. Seeder: 15 событий в orders-multi, топик-цель orders-copy"
FROM="$(date +'%Y-%m-%d %H:%M:%S%:z')"
dotnet build "$ROOT/Seeder" -v quiet --nologo
dotnet run --project "$ROOT/Seeder" --no-build >/dev/null
sleep 1
TO="$(date -d '+2 seconds' +'%Y-%m-%d %H:%M:%S%:z')"

echo "== 3. Проверка и план (ничего не отправляется)"
set +e
dotnet run --project "$REPLAY" -- --bootstrap localhost:9092 --topic orders-multi --from "$FROM" --to "$TO" \
    --routes "$ROOT/KafkaReplay/samples/routes.json" --target-bootstrap localhost:9092 --out "$OUT"
echo "   код выхода $? (ожидаем 1: в данных Seeder есть отклонённые записи)"

echo "== 4. Отправка по плану"
dotnet run --project "$REPLAY" -- --execute --plan "$OUT/plan.json" --target-bootstrap localhost:9092
echo "   код выхода $? (ожидаем 0: всё из плана записано и совпало байт в байт)"

echo "== 5. Повторный запуск того же плана: ничего не должно уйти второй раз"
dotnet run --project "$REPLAY" -- --execute --plan "$OUT/plan.json" --target-bootstrap localhost:9092
echo "   код выхода $?"
set -e

echo
echo "Отчёты: $OUT/report.md, $OUT/execution.md"
