#!/usr/bin/python

import argparse
import random

import aerospike
from aerospike_helpers.operations import hll_operations
import generator


parser = argparse.ArgumentParser(description="HLL example")
parser.add_argument("--host", dest="host", default="127.0.0.1")
args = parser.parse_args()

HLL_BIN = "hll_bin"
HLL_INDEX_BITS = 12
HLL_MH_BITS = 0
MONTHS = 12


def init(host: str, port: int) -> aerospike.client:
    config = {"hosts": [(host, port)], "policies": {"timeout": 1000}}  # milliseconds

    client = aerospike.client(config).connect()
    return client


def getkey(name: str, time: int) -> tuple:
    return ("test", "hll", f"{name}:{time}")


# Ingest records with id in the range [start, end).
def ingest_month(client: aerospike.client, start: int, end: int, month: int) -> None:
    # We don't need to initialise HLL bins; performing a HLL add to an empty bin
    # will initialise it.
    print(f"Ingest ids {start}-{end} month {month}")

    for i in range(start, end):
        profile = generator.get_profile(i, month)
        # print("Profile {i}: {profile}")
        for tag in profile:
            ops = [hll_operations.hll_add(HLL_BIN, [str(i)], HLL_INDEX_BITS)]

            _, _, result = client.operate(getkey(tag, month), ops)


def ingest(client: aerospike.client) -> None:
    for i in range(MONTHS):
        ingest_month(client, 1, 20000, i)


def count(client: aerospike.client, tag: str, month: int) -> None:
    ops = [hll_operations.hll_get_count(HLL_BIN)]
    _, _, result = client.operate(getkey(tag, month), ops)
    print(f"tag:{tag} month:{month} count:{result[HLL_BIN]}")


def get_union_count(client: aerospike.client, tag: str, t0: int, months: int) -> None:
    times = range(t0 + 1, months)
    hlls = [
        record[2][HLL_BIN]
        for record in client.get_many([getkey(tag, time) for time in times])
    ]

    ops = [hll_operations.hll_get_union_count(HLL_BIN, hlls)]

    _, _, result = client.operate(getkey(tag, t0), ops)
    print(f"tag:{tag} months:{t0}-{t0 + months - 1} count:{result[HLL_BIN]}")


def get_intersect_count(client: aerospike.client, tags: list, month: int) -> None:
    hlls = [
        record[2][HLL_BIN]
        for record in client.get_many([getkey(tag, month) for tag in tags[1:]])
    ]

    ops = [hll_operations.hll_get_intersect_count(HLL_BIN, hlls)]

    _, _, result = client.operate(getkey(tags[0], month), ops)
    print(f"tags:{tags} month:{month} count:{result[HLL_BIN]}")


def main() -> None:
    client = init(args.host, 3000)
    ingest(client)

    # Get counts
    for i in range(MONTHS):
        count(client, "aerospike", i)

    # Get union
    get_union_count(client, "aerospike", 0, MONTHS)

    # Get intersection
    count(client, "vancouver", 0)
    count(client, "canada", 0)
    count(client, "washington", 0)
    get_intersect_count(client, ["vancouver", "washington"], 0)


if __name__ == "__main__":
    main()
