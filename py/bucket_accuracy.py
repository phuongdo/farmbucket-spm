#!/usr/bin/env python3
"""Compute BigQuery-compatible FarmHash buckets and measure accuracy.

This script mirrors `swift run BucketAccuracy`. It reads a CSV export where the
first column is the advertising ID (`adid`) and the final column is the
expected bucket (0-99). Intermediate columns are ignored.

Example:
    python bucket_accuracy.py            # uses ../test.csv by default
    python bucket_accuracy.py data.csv
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable, Tuple

MASK64 = (1 << 64) - 1
K0 = 0xC3A5C85C97CB3127
K1 = 0xB492B66FBE98F273
K2 = 0x9AE16A3B2F90404F
KMUL = 0x9DDFEA08EB382D69


def to_uint64(value: int) -> int:
    return value & MASK64


def to_int64(value: int) -> int:
    value &= MASK64
    if value & (1 << 63):
        return value - (1 << 64)
    return value


def rotate_right(value: int, shift: int) -> int:
    shift &= 63
    if shift == 0:
        return to_uint64(value)
    value = to_uint64(value)
    return to_uint64((value >> shift) | ((value << (64 - shift)) & MASK64))


def shift_mix(value: int) -> int:
    value = to_uint64(value)
    return to_uint64(value ^ (value >> 47))


def hash128to64(u: int, v: int) -> int:
    a = to_uint64((u ^ v) * KMUL)
    a ^= a >> 47
    b = to_uint64((v ^ a) * KMUL)
    b ^= b >> 47
    b = to_uint64(b * KMUL)
    return b


def hash_len16(u: int, v: int, mul: int) -> int:
    a = to_uint64((u ^ v) * mul)
    a ^= a >> 47
    b = to_uint64((v ^ a) * mul)
    b ^= b >> 47
    b = to_uint64(b * mul)
    return b


def fetch64(buf: bytes, offset: int) -> int:
    return int.from_bytes(buf[offset : offset + 8], "little")


def fetch32(buf: bytes, offset: int) -> int:
    return int.from_bytes(buf[offset : offset + 4], "little")


def hash_len0to16(buf: bytes) -> int:
    length = len(buf)
    if length >= 8:
        mul = K2 + length * 2
        a = fetch64(buf, 0) + K2
        b = fetch64(buf, length - 8)
        c = to_uint64(rotate_right(b, 37) * mul + a)
        d = to_uint64((rotate_right(a, 25) + b) * mul)
        return hash_len16(c, d, mul)
    if length >= 4:
        mul = K2 + length * 2
        a = fetch32(buf, 0)
        b = fetch32(buf, length - 4)
        return hash_len16(length + (a << 3), b, mul)
    if length > 0:
        a = buf[0]
        b = buf[length >> 1]
        c = buf[length - 1]
        y = a + (b << 8)
        z = length + (c << 2)
        return shift_mix(y * K2 ^ z * K0) * K2 & MASK64
    return K2


def hash_len17to32(buf: bytes) -> int:
    length = len(buf)
    mul = K2 + length * 2
    a = fetch64(buf, 0) * K1
    b = fetch64(buf, 8)
    c = fetch64(buf, length - 8) * mul
    d = fetch64(buf, length - 16) * K2
    return hash_len16(
        rotate_right(a + b, 43) + rotate_right(c, 30) + d,
        a + rotate_right(b + K2, 18) + c,
        mul,
    )


def weak_hash_len32_with_seeds(
    buf: bytes, offset: int, a: int, b: int
) -> Tuple[int, int]:
    w = fetch64(buf, offset)
    x = fetch64(buf, offset + 8)
    y = fetch64(buf, offset + 16)
    z = fetch64(buf, offset + 24)
    a = to_uint64(a + w)
    b = rotate_right(b + a + z, 21)
    c = a
    a = to_uint64(a + x + y)
    b = to_uint64(b + rotate_right(a, 44))
    return to_uint64(a + z), to_uint64(b + c)


def hash_len33to64(buf: bytes) -> int:
    length = len(buf)
    mul = K2 + length * 2
    a = fetch64(buf, 0) * K2
    b = fetch64(buf, 8)
    c = fetch64(buf, length - 8) * mul
    d = fetch64(buf, length - 16) * K2
    y = rotate_right(a + b, 43) + rotate_right(c, 30) + d
    y = to_uint64(y)
    z = hash_len16(y, a + rotate_right(b + K2, 18) + c, mul)
    e = fetch64(buf, 16) * mul
    f = fetch64(buf, 24)
    g = (y + fetch64(buf, length - 32)) * mul
    h = (z + fetch64(buf, length - 24)) * mul
    return hash_len16(
        rotate_right(e + f, 43) + rotate_right(g, 30) + h,
        e + rotate_right(f + a, 18) + g,
        mul,
    )


def farmhash_hash64(buf: bytes) -> int:
    length = len(buf)
    if length <= 32:
        if length <= 16:
            return hash_len0to16(buf)
        return hash_len17to32(buf)
    if length <= 64:
        return hash_len33to64(buf)

    seed = 81
    x = seed
    y = seed * K1 + 113
    z = shift_mix(y * K2 + 113) * K2 & MASK64
    v = (0, 0)
    w = (0, 0)
    x = to_uint64(x * K2 + fetch64(buf, 0))

    end = ((length - 1) // 64) * 64
    offset = 0
    while offset < end:
        x = to_uint64(rotate_right(x + y + v[0] + fetch64(buf, offset + 8), 37) * K1)
        y = to_uint64(rotate_right(y + v[1] + fetch64(buf, offset + 48), 42) * K1)
        x ^= w[1]
        y = to_uint64(y + v[0] + fetch64(buf, offset + 40))
        z = to_uint64(rotate_right(z + w[0], 33) * K1)
        v = weak_hash_len32_with_seeds(
            buf, offset, to_uint64(v[1] * K1), to_uint64(x + w[0])
        )
        w = weak_hash_len32_with_seeds(
            buf,
            offset + 32,
            to_uint64(z + w[1]),
            to_uint64(y + fetch64(buf, offset + 16)),
        )
        x, z = z, x
        offset += 64

    last64 = length - 64
    mul = to_uint64(K1 + ((z & 0xFF) << 1))
    offset = last64
    w0 = to_uint64(w[0] + ((length - 1) & 63))
    v0 = to_uint64(v[0] + w0)
    w0 = to_uint64(w0 + v0)
    w = (w0, w[1])
    v = (v0, v[1])

    x = to_uint64(rotate_right(x + y + v[0] + fetch64(buf, offset + 8), 37) * mul)
    y = to_uint64(rotate_right(y + v[1] + fetch64(buf, offset + 48), 42) * mul)
    x ^= to_uint64(w[1] * 9)
    y = to_uint64(y + v[0] * 9 + fetch64(buf, offset + 40))
    z = to_uint64(rotate_right(z + w[0], 33) * mul)
    v = weak_hash_len32_with_seeds(
        buf, offset, to_uint64(v[1] * mul), to_uint64(x + w[0])
    )
    w = weak_hash_len32_with_seeds(
        buf,
        offset + 32,
        to_uint64(z + w[1]),
        to_uint64(y + fetch64(buf, offset + 16)),
    )
    x, z = z, x

    result = hash_len16(
        hash_len16(v[0], w[0], mul) + shift_mix(y) * K0 + z,
        hash_len16(v[1], w[1], mul) + x,
        mul,
    )
    return to_uint64(result)


def farmhash_fingerprint64(buf: bytes) -> int:
    return farmhash_hash64(buf)


def abs_int64_as_uint64(value: int) -> int:
    if value >= 0:
        return value
    return to_uint64(-value)


def bigquery_bucket(adid: str) -> int:
    payload = (adid + "test:1" + "salt-2025").encode("utf-8")
    fingerprint = farmhash_fingerprint64(payload)
    signed = to_int64(fingerprint)
    abs_val = abs_int64_as_uint64(signed)
    return int(abs_val % 100)


def evaluate(csv_path: Path) -> Tuple[int, int, Iterable[str]]:
    matches = 0
    total = 0
    details = []
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return 0, 0, ["No rows to evaluate."]
        details.append(f"Using CSV header: {','.join(header)}")
        for row in reader:
            if not row:
                continue
            adid = row[0].strip()
            bucket_raw = row[-1].strip()
            if not adid or not bucket_raw:
                continue
            try:
                expected = int(bucket_raw)
            except ValueError:
                continue
            predicted = bigquery_bucket(adid)
            total += 1
            if predicted == expected:
                matches += 1
            details.append(
                f"adid={adid} expected={expected} predicted={predicted}"
            )
    return matches, total, details


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=str(Path(__file__).resolve().parents[1] / "test.csv"),
        help="Path to CSV export (default: ../test.csv)",
    )
    args = parser.parse_args()
    csv_path = Path(args.csv_path)
    matches, total, details = evaluate(csv_path)
    for line in details:
        print(line)
    if total == 0:
        print("No valid rows processed.")
        return
    accuracy = matches / total * 100.0
    print(f"\nMatches: {matches}/{total}  Accuracy: {accuracy:.2f}%")


if __name__ == "__main__":
    main()
