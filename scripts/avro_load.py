#!/usr/bin/env python3

import argparse
import datetime as dt
import glob
import os
import sys

import fastavro
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

TIME_FORMATS = ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ")


def to_bytes(v):
    if v is None or isinstance(v, bytes):
        return v
    if isinstance(v, str):
        return v.encode("utf-8")
    return str(v).encode("utf-8")  # long / double


def capture_ms(rec) -> int:
    s = rec["EnqueuedTimeUtc"]
    for fmt in TIME_FORMATS:
        try:
            return int(dt.datetime.strptime(s, fmt).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
        except ValueError:
            pass
    raise ValueError(f"не понимаю формат времени: {s!r}")


def timestamp_ms(rec) -> tuple[int, bool]:
    """(время сообщения, совпало ли x-opt-enqueued-time с EnqueuedTimeUtc)"""
    cap = capture_ms(rec)
    t = (rec["SystemProperties"] or {}).get("x-opt-enqueued-time")
    if isinstance(t, int) and cap <= t < cap + 1000:
        return t, True
    return cap, not isinstance(t, int)


def kafka_key(rec):
    sysp = rec["SystemProperties"] or {}
    return to_bytes(sysp.get("x-opt-kafka-key", sysp.get("x-opt-partition-key")))


def records(root: str, limit: int | None):
    # имена папок — время с нулями впереди (08/01/00/04/13), поэтому сортировка по пути = по времени
    files = sorted(glob.glob(os.path.join(os.path.expanduser(root), "**", "*.avro"), recursive=True))
    n = 0
    for path in files:
        with open(path, "rb") as fh:
            for rec in fastavro.reader(fh):
                yield rec
                n += 1
                if limit is not None and n >= limit:
                    return


def ensure_topic(admin: AdminClient, topic: str, partitions: int) -> None:
    meta = admin.list_topics(timeout=10)
    if topic in meta.topics:
        have = len(meta.topics[topic].partitions)
        if have != partitions:
            sys.exit(f"топик {topic} уже есть, но в нём {have} партиций, а нужно {partitions} — удали его и запусти снова")
        print(f"топик {topic} уже есть ({have} партиций) — проверь, что он пустой и с retention.ms=-1")
        return
    fut = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1,
                                        config={"retention.ms": "-1"})])[topic]
    fut.result()
    print(f"создан топик {topic}: {partitions} партиций, retention.ms=-1")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("dump")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--dry", action="store_true", help="только прочитать и посчитать")
    ap.add_argument("--partitions", type=int, default=1)
    ap.add_argument("--partitioner", default="consistent_random", choices=["consistent_random", "murmur2_random"])
    a = ap.parse_args()

    producer = None
    if not a.dry:
        ensure_topic(AdminClient({"bootstrap.servers": a.bootstrap}), a.topic, a.partitions)
        producer = Producer({"bootstrap.servers": a.bootstrap, "acks": "all", "enable.idempotence": True,
                             "linger.ms": 20, "client.id": "avro-load", "partitioner": a.partitioner})

    sent = failed = with_key = time_mismatch = backwards = 0
    prev_ts = None
    first = last = None
    errors: list[str] = []
    per_partition: dict[int, int] = {}
    prev_in_partition: dict[int, int] = {}
    backwards_in_partition = 0

    def on_delivery(err, msg):
        nonlocal failed, backwards_in_partition
        if err is not None:
            failed += 1
            if len(errors) < 5:
                errors.append(str(err))
            return
        p = msg.partition()
        per_partition[p] = per_partition.get(p, 0) + 1
        ts = msg.timestamp()[1]
        if p in prev_in_partition and ts < prev_in_partition[p]:
            backwards_in_partition += 1
        prev_in_partition[p] = ts

    for rec in records(a.dump, a.limit):
        ts, same = timestamp_ms(rec)
        time_mismatch += not same
        backwards += prev_ts is not None and ts < prev_ts  # время идёт назад: порядок не по времени
        prev_ts = ts
        first = ts if first is None else min(first, ts)
        last = ts if last is None else max(last, ts)
        key = kafka_key(rec)
        with_key += key is not None
        headers = [(k, to_bytes(v)) for k, v in (rec["Properties"] or {}).items()]
        if producer is not None:
            while True:
                try:
                    producer.produce(a.topic, value=rec["Body"], key=key, headers=headers,
                                     timestamp=ts, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)  # очередь заполнена — подождать отправки
            producer.poll(0)
        sent += 1

    if producer is not None:
        left = producer.flush(60)
        if left:
            failed += left
    fmt = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")
    print(f"{'прочитано' if a.dry else 'отправлено'}: {sent}, с ключом: {with_key}, ошибок доставки: {failed}")
    print(f"x-opt-enqueued-time не совпало с EnqueuedTimeUtc (взято EnqueuedTimeUtc): {time_mismatch}")
    print(f"сообщений, время которых меньше предыдущего: {backwards}" +
          ("  ⚠ в топике время идёт не по порядку — чтение окном по времени пропустит часть сообщений" if backwards else ""))
    if per_partition:
        print("по партициям: " + ", ".join(f"p{p}={n}" for p, n in sorted(per_partition.items())) +
              f"  (партиционер {a.partitioner})")
        print(f"внутри партиций время меньше предыдущего: {backwards_in_partition}")
    if first is not None:
        print(f"время сообщений: {fmt(first)} … {fmt(last)}")
        print(f'окно для replay: --from "{fmt(first)}" --to "{fmt(last + 1000)}"')
    for e in errors:
        print(f"  ошибка: {e}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        sys.exit(f"Kafka: {e}")
