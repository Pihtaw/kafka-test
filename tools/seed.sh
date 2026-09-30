#!/usr/bin/env bash
# Готовит данные для сравнения инструментов:
#   tools-topic (3 партиции) — сообщения с РАЗНЫМИ заголовками, двумя пачками с паузой 60 секунд
#   copy-topic  (3 партиции) — пустой, сюда копирует Redpanda Connect
set -euo pipefail

topics() {
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"
}
topics --create --if-not-exists --topic tools-topic --partitions 3
topics --create --if-not-exists --topic copy-topic --partitions 3

# send <ключ> <eventType> <version> <json>
send() {
  echo "$1:$4" | docker compose run --rm -T kcat -b kafka:19092 -P -t tools-topic -K: \
    -H "eventType=$2" -H "version=$3"
}

batch() {
  send "order-1" OrderCreated   1 "{\"orderId\":1,\"amount\":500,\"batch\":\"$1\"}"
  send "order-2" OrderCreated   2 "{\"orderId\":2,\"amount\":700,\"currency\":\"RUB\",\"batch\":\"$1\"}"
  send "order-1" OrderCancelled 1 "{\"orderId\":1,\"reason\":\"client\",\"batch\":\"$1\"}"
  send "order-3" OrderPaid      1 "{\"orderId\":3,\"batch\":\"$1\"}"
}

echo "Пачка A..."
A_START=$(date +%s)000
batch A
A_END=$(date +%s)000

echo "Ждём 60 секунд, чтобы у пачек было разное время..."
sleep 60

echo "Пачка B..."
B_START=$(date +%s)000
batch B
B_END=$(date +%s)000

cat <<INFO

Время в миллисекундах для чтения по времени:
  пачка A: ${A_START} .. ${A_END}
  пачка B: ${B_START} .. ${B_END}
  граница между пачками: $((A_END + 30000))
INFO
