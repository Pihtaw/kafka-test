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
# localhost:9092 должен быть локальной Kafka из Docker, а не туннелем к стенду
if pgrep -f "port-forward.*9092" >/dev/null; then
    echo "❌ работает kubectl port-forward на 9092 — localhost:9092 ведёт на стенд. Останови туннель и запусти демо снова" >&2
    exit 1
fi
docker compose -f "$ROOT/docker-compose.yml" up -d kafka
# ждём, пока брокер начнёт отвечать (до ~2 минут)
printf "   жду брокер"
ready=0
for _ in $(seq 1 30); do
    if docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then ready=1; break; fi
    printf "."
    sleep 2
done
echo
if [[ $ready != 1 ]]; then
    echo "❌ локальная Kafka не ответила — смотри: docker logs kafka" >&2
    exit 1
fi
echo "   брокер отвечает"

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
CHECK=$?
echo "   код выхода $CHECK (ожидаем 1: в данных Seeder есть отклонённые записи)"

echo "== 4. Отправка по плану"
dotnet run --project "$REPLAY" -- --execute --plan "$OUT/plan.json" --target-bootstrap localhost:9092
SEND=$?
echo "   код выхода $SEND (ожидаем 0: всё из плана записано и совпало байт в байт)"

echo "== 5. Повторный запуск того же плана: ничего не должно уйти второй раз"
dotnet run --project "$REPLAY" -- --execute --plan "$OUT/plan.json" --target-bootstrap localhost:9092 | tee "$OUT/repeat.log"
REPEAT=${PIPESTATUS[0]}
echo "   код выхода $REPEAT"
set -e

echo
echo "Отчёты: $OUT/report.md, $OUT/execution.md"

# итог: коды 1 → 0 → 0, и повтор ничего не отправил. Иначе демо провалено (в CI — красный тест)
if [[ "$CHECK" != 1 || "$SEND" != 0 || "$REPEAT" != 0 ]]; then
    echo "❌ демо: коды $CHECK → $SEND → $REPEAT, ожидали 1 → 0 → 0" >&2
    exit 1
fi
if ! grep -q "К отправке в этот запуск: 0" "$OUT/repeat.log"; then
    echo "❌ демо: повторный запуск снова что-то отправил" >&2
    exit 1
fi
echo "✅ демо: коды 1 → 0 → 0, повторный запуск ничего не отправил"
