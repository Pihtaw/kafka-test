#!/usr/bin/env python3

import collections
import datetime as dt
import glob
import os
import sys

import fastavro


def main(root: str) -> None:
    files = sorted(glob.glob(os.path.join(os.path.expanduser(root), "**", "*.avro"), recursive=True))
    empty = records = null_body = 0
    sys_keys: dict[str, set[str]] = collections.defaultdict(set)
    prop_keys: dict[str, set[str]] = collections.defaultdict(set)
    event_types: collections.Counter[str] = collections.Counter()
    versions: collections.Counter[str] = collections.Counter()
    first_time = last_time = None
    has_key = 0
    xopt_min = xopt_max = None
    xopt_mismatch = 0

    for path in files:
        with open(path, "rb") as fh:
            n = 0
            for rec in fastavro.reader(fh):
                n += 1
                cap = capture_ms(rec["EnqueuedTimeUtc"])
                first_time = cap if first_time is None else min(first_time, cap)
                last_time = cap if last_time is None else max(last_time, cap)
                t = (rec["SystemProperties"] or {}).get("x-opt-enqueued-time")
                if isinstance(t, int):
                    xopt_min = t if xopt_min is None else min(xopt_min, t)
                    xopt_max = t if xopt_max is None else max(xopt_max, t)
                    if not cap <= t < cap + 1000:
                        xopt_mismatch += 1
                if rec["Body"] is None:
                    null_body += 1
                for k, v in (rec["SystemProperties"] or {}).items():
                    sys_keys[k].add(type(v).__name__)
                if "x-opt-kafka-key" in (rec["SystemProperties"] or {}):
                    has_key += 1
                props = rec["Properties"] or {}
                for k, v in props.items():
                    prop_keys[k].add(type(v).__name__)
                et = props.get("Event-Type")
                event_types[as_text(et)] += 1
                versions[as_text(props.get("Event-PackageVersion"))] += 1
            records += n
            if n == 0:
                empty += 1

    print(f"файлов: {len(files)}, пустых: {empty}, записей: {records}, без Body (null): {null_body}")
    print(f"EnqueuedTimeUtc: {fmt(first_time)} … {fmt(last_time)}")
    print(f"x-opt-enqueued-time: {fmt(xopt_min)} … {fmt(xopt_max)}, не в той же секунде, что EnqueuedTimeUtc: {xopt_mismatch}")
    print(f"записей с ключом (x-opt-kafka-key): {has_key} из {records}")
    print("\nSystemProperties (имя: типы):")
    for k in sorted(sys_keys):
        print(f"  {k}: {', '.join(sorted(sys_keys[k]))}")
    print("\nProperties (имя: типы):")
    for k in sorted(prop_keys):
        print(f"  {k}: {', '.join(sorted(prop_keys[k]))}")
    print("\nEvent-Type (сколько):")
    for t, c in event_types.most_common():
        print(f"  {c:>8}  {t}")
    print("\nEvent-PackageVersion (сколько):")
    for v, c in versions.most_common():
        print(f"  {c:>8}  {v}")


def capture_ms(s: str) -> int:
    for f in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return int(dt.datetime.strptime(s, f).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
        except ValueError:
            pass
    raise ValueError(f"не понимаю формат времени: {s!r}")


def fmt(ms) -> str:
    return "—" if ms is None else dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+00:00"


def as_text(v) -> str:
    if v is None:
        return "<нет>"
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return f"<не UTF-8: {v[:8].hex()}…>"
    return str(v)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("использование: avro_survey.py <папка с дампом>")
    main(sys.argv[1])
