#!/usr/bin/env python3

import argparse
import collections
import json
import re
import sys
import zlib
import base64


def murmur2(data: bytes) -> int:
    m, r = 0x5BD1E995, 24
    h = (0x9747B28C ^ len(data)) & 0xFFFFFFFF
    i = 0
    while i + 4 <= len(data):
        k = int.from_bytes(data[i:i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
        i += 4
    rest = len(data) % 4
    if rest == 3:
        h ^= data[i + 2] << 16
    if rest >= 2:
        h ^= data[i + 1] << 8
    if rest >= 1:
        h ^= data[i]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def expected_partition(key: bytes, n: int, partitioner: str) -> int:
    if partitioner == "crc32":
        return zlib.crc32(key) % n  # librdkafka consistent_random, .NET по умолчанию
    return (murmur2(key) & 0x7FFFFFFF) % n  # Java DefaultPartitioner / librdkafka murmur2_random


def key_bytes(shown: str | None) -> bytes | None:
    """Ключ в report.json: текст, если это UTF-8, иначе «<не UTF-8: HEX>»."""
    if shown is None:
        return None
    m = re.fullmatch(r"<не UTF-8: ([0-9A-F]*)>", shown)
    return bytes.fromhex(m.group(1)) if m else shown.encode("utf-8")


def outcome(r) -> tuple:
    diffs = tuple(sorted((d["path"], d["kind"]) for d in r["differences"]))
    return (r["recordSha256"], r["verdict"], r["reason"], diffs)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--one", required=True, help="report.json прогона по топику с 1 партицией")
    ap.add_argument("--many", required=True, help="report.json прогона по топику с N партициями")
    ap.add_argument("--partitioner", default="crc32", choices=["crc32", "murmur2"],
                    help="каким партиционером разложен N-партиционный топик")
    ap.add_argument("--plan", help="plan.json, построенный утилитой для отправки в топик с N партициями")
    ap.add_argument("--plan-partitions", type=int, help="сколько партиций в целевом топике плана (по умолчанию N)")
    a = ap.parse_args()

    one = json.load(open(a.one, encoding="utf-8"))
    many = json.load(open(a.many, encoding="utf-8"))
    A, B = one["records"], many["records"]
    n = len(many["partitions"])
    problems = 0

    def check(ok: bool, text: str) -> None:
        nonlocal problems
        problems += not ok
        print(("✅ " if ok else "❌ ") + text)

    print(f"1 партиция: {len(A)} сообщений; {n} партиций: {len(B)} сообщений — " +
          ", ".join(f"p{p['partition']}={p['end'] - p['start']}" for p in many["partitions"]))
    print(f"сводка 1 партиции: {one['summary']['byVerdict']}, опасных отличий {one['summary']['warnings']}")
    print(f"сводка {n} партиций: {many['summary']['byVerdict']}, опасных отличий {many['summary']['warnings']}")
    for name, rep in (("1 партиция", one), (f"{n} партиций", many)):
        s = rep["summary"]
        check(s["allRead"] and s["sumMatches"], f"{name}: дочитаны все партиции и сводка сходится")

    # 1. те же сообщения
    ha = collections.Counter(r["recordSha256"] for r in A)
    hb = collections.Counter(r["recordSha256"] for r in B)
    only_a, only_b = ha - hb, hb - ha
    check(not only_a and not only_b,
          f"те же сообщения: только в 1 партиции {sum(only_a.values())}, только в {n} партициях {sum(only_b.values())}")

    # 2. те же решения и отличия
    oa = collections.Counter(outcome(r) for r in A)
    ob = collections.Counter(outcome(r) for r in B)
    differ = sum((oa - ob).values())
    check(differ == 0, f"те же вердикты, причины и отличия: расходятся у {differ} сообщений")

    # 3. ключ → партиция
    parts_by_key = collections.defaultdict(set)
    wrong_partition = no_key = 0
    for r in B:
        k = key_bytes(r["key"])
        if k is None:
            no_key += 1
            continue
        parts_by_key[k].add(r["partition"])
        wrong_partition += r["partition"] != expected_partition(k, n, a.partitioner)
    split = sum(1 for ps in parts_by_key.values() if len(ps) > 1)
    print(f"ключей: {len(parts_by_key)}, сообщений без ключа: {no_key}")
    check(split == 0, f"каждый ключ в одной партиции: ключей в нескольких партициях {split}")
    check(wrong_partition == 0,
          f"партиция = {a.partitioner}(ключ) % {n}: не совпало у {wrong_partition} сообщений")

    # 4. порядок по ключу
    def order(records):
        seq = collections.defaultdict(list)
        for r in sorted(records, key=lambda r: (r["partition"], r["offset"])):
            seq[r["key"]].append(r["recordSha256"])
        return seq
    sa, sb = order(A), order(B)
    reordered = sum(1 for k in sa if sa[k] != sb.get(k))
    check(reordered == 0, f"порядок сообщений внутри ключа тот же: отличается у {reordered} ключей")

    # 5. план отправки
    if a.plan:
        plan = json.load(open(a.plan, encoding="utf-8"))
        items = plan["items"] if "items" in plan else plan["Items"]
        get = (lambda it, k: it[k] if k in it else it[k[0].upper() + k[1:]])
        pn = a.plan_partitions or n
        mismatch = total = 0
        counts = collections.Counter()
        for it in items:
            kb64 = get(it, "key")
            if kb64 is None:
                continue
            key = base64.b64decode(kb64)
            tp = get(it, "targetPartition")
            counts[tp] += 1
            total += 1
            mismatch += tp != expected_partition(key, pn, a.partitioner)
        print(f"план: {len(items)} строк, целевые партиции: " + ", ".join(f"p{p}={c}" for p, c in sorted(counts.items())))
        check(mismatch == 0, f"утилита выбрала партицию как {a.partitioner}(ключ) % {pn}: не совпало у {mismatch} из {total}")

    print("\nИТОГ: " + ("всё совпало" if problems == 0 else f"проблем: {problems}"))
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
