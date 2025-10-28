"""
Crawl the repos of users from dids.csv and output the records to jsonl files.
"""

import csv
import json
import os
import shutil
import typing as t

import requests
from libipld import decode_car  # type: ignore

STREAM_DIR = "./stream"
DID_PATH = "./dids.csv"

START_DATE_CUTOFF = "2022-11-16"  # Start of bsky network
END_DATE_CUTOFF = "2023-07-01"  # TODO: Extend

RECORD_TYPES = [
    "app.bsky.feed.like",
    "app.bsky.feed.post",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
    "app.bsky.graph.block",
]


class BlockOp(t.TypedDict):
    p: int  # Starting index of the previous key
    k: bytes  # Remaining part of the key (after the previous key)
    v: str  # Pointer to the block containing this op's data
    t: str  # Pointer to the block containing pointer to the right
    l: str  # Pointer to the block containing pointer to the left # noqa: E741


def download_repo(did: str, log: bool = True) -> None:
    try:
        res = requests.get(
            f"https://bsky.network/xrpc/com.atproto.sync.getRepo?did={did}",
        )

        if res.status_code != 200:
            print(f"Failed to fetch {did}: {res.status_code} {res.text}")
            return

        repo = t.cast(
            tuple[dict[str, t.Any], dict[str, dict[str, t.Any]]],
            decode_car(res.content),
        )
        root, tree = repo

    except Exception as e:
        print(f"Error fetching repo for {did}: {e}")
        return

    uris = set()
    total_records = 0
    total_errors = 0

    try:
        for block in tree.values():
            if "e" in block:
                ops: list[BlockOp] = block["e"]
                prev_key = ""

                for op in ops:
                    rkey = prev_key[: op["p"]] + op["k"].decode("utf-8")
                    prev_key = rkey
                    data_cid = op["v"]

                    if rkey not in uris:
                        uris.add(rkey)

                        if data_cid in tree:  # Data exists @ pointer
                            record = tree[data_cid]

                            if record["$type"] in RECORD_TYPES:
                                try:
                                    save_record(
                                        {
                                            "did": did,
                                            "$type": record.get("$type", ""),
                                            "createdAt": record.get("createdAt", ""),
                                            "uri": f"at://{did}/{rkey}",
                                            **record,
                                        }
                                    )
                                    total_records += 1
                                except Exception as e:
                                    print(f"Error saving record: {e}")
                                    total_errors += 1
        if log:
            print(f"Added {total_records} records for {did} ({total_errors} errors)")

    except Exception as e:
        print(f"Error processing repo for {did}: {e}")


def save_record(record: dict[str, t.Any]):
    file_idx = record["createdAt"][:10]  # YYYY-MM-DD
    if file_idx > END_DATE_CUTOFF:
        return

    if file_idx < START_DATE_CUTOFF:
        return

    with open(f"{STREAM_DIR}/{file_idx}.jsonl", "a") as f:
        json.dump(record, f)
        f.write("\n")


def process_user(row):
    did = row["did"]
    created_at = row["created_at"]

    if created_at[:10] > END_DATE_CUTOFF:
        return False

    save_record(
        {"did": did, "$type": "app.bsky.actor.profile", "createdAt": created_at}
    )
    download_repo(did)
    return True


if __name__ == "__main__":
    import argparse
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock

    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()

    if os.path.exists(STREAM_DIR):
        shutil.rmtree(STREAM_DIR)
    os.makedirs(STREAM_DIR)

    total_users = 0
    total_users_lock = Lock()

    with open(DID_PATH, "r") as csvfile:
        reader = list(csv.DictReader(csvfile))

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = []
        for row in reader:
            futures.append(executor.submit(process_user, row))

        for future in futures:
            if future.result():
                with total_users_lock:
                    total_users += 1

    print(f"Finished crawl to {END_DATE_CUTOFF}. Total users: {total_users}")
