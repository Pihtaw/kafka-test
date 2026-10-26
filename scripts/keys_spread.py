#!/usr/bin/env python3
"""Как разные ключи топика разошлись бы по N партициям при CRC32 (.NET) и murmur2 (Java).

    python3 -I keys_spread.py ~/dumps/aug/dc/price.v1

Считает различные ключи (x-opt-kafka-key) и для N = 2, 3, 4, 8, 16, 32 печатает, сколько ключей
попало в каждую партицию. Ни ключи, ни сообщения не печатаются — только количества.
"""
import collections
import glob
import os
import sys
import zlib

import fastavro

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from compare_runs import murmur2  # noqa: E402

NS = (2, 3, 4, 8, 16, 32)


def main(root: str) -> None:
    keys: collections.Counter[bytes] = collections.Counter()
    for path in sorted(glob.glob(os.path.join(os.path.expanduser(root), "**", "*.avro"), recursive=True)):
        with open(path, "rb") as fh:
            for rec in fastavro.reader(fh):
                k = (rec["SystemProperties"] or {}).get("x-opt-kafka-key")
                if k is not None:
                    keys[k if isinstance(k, bytes) else str(k).encode()] += 1
    lengths = collections.Counter(len(k) for k in keys)
    print(f"сообщений с ключом: {sum(keys.values())}, различных ключей: {len(keys)}, длины ключей: {dict(lengths)}")
    for name, h in (("crc32  ", lambda k: zlib.crc32(k)), ("murmur2", lambda k: murmur2(k) & 0x7FFFFFFF)):
        for n in NS:
            c = collections.Counter(h(k) % n for k in keys)
            used = len(c)
            cells = " ".join(f"{c.get(p, 0)}" for p in range(n))
            print(f"  {name} N={n:<2} занято партиций {used:>2} из {n:<2} | ключей по партициям: {cells}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("использование: keys_spread.py <папка топика в дампе>")
    main(sys.argv[1])
