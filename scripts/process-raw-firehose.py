"""Identify which users and posts were deleted and reinsert them into the firehose.

1. DETECT DELETED USER X:
   - Find interactions where User A references nonexistent User X through:
     * Likes on posts by User X
     * Replies to posts by User X
     * Quotes of posts by User X

2. DETECT DELETED POST Y:
   - Find interactions where User A references nonexistent Post Y through:
     * Likes on Post Y
     * Replies to Post Y
     * Quotes of Post Y

3. RECOVERY PROCESS:
   - Extract timestamp from post's rkey
   - Use timestamp to locate position in firehose
   - Re-insert reconstructed entity data at appropriate position
"""

# ==== Imports ====

import datetime
import heapq
import json
import os
import shutil
from pathlib import Path

from tqdm import tqdm

from utils import (
    Record,
    did_from_uri,
    get_quoted_uri,
    parse_rkey,
    records,
    rkey_from_uri,
)

# ==== Constants ====

END_DATE = datetime.datetime(2023, 7, 1, tzinfo=datetime.timezone.utc)
BATCH_SIZE = 1_000_000  # Number of records per file

IN_DIR = "./data/stream-2023-07-01"
TEMP_DIR = f"./data/firehose-temp-{END_DATE.date()}"
OUT_DIR = f"./data/firehose-{END_DATE.date()}"

# ==== Directory cleanup ====

# Clean up any existing temp/output directories
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)
if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)

# Recreate directories
os.makedirs(TEMP_DIR)
os.makedirs(OUT_DIR)


# ==== Helper functions ====


def calc_timestamp(record: Record) -> int | None:
    if record["$type"] == "app.bsky.actor.profile":
        ts = int(
            datetime.datetime.fromisoformat(
                record["createdAt"].replace("Z", "+00:00")
            ).timestamp()
            * 1_000
        )

    else:
        rkey = rkey_from_uri(record["uri"])
        if not rkey:
            return None

        ts, _ = parse_rkey(rkey)

    return ts


# ==== RE-ORDER RECORDS BASED ON RKEY ====


stream_dir = Path(IN_DIR)
files = sorted(stream_dir.glob("*.jsonl"))
record_buffer: list[tuple[int, int, Record]] = []
end_ts = END_DATE.timestamp() * 1_000  # Milliseconds

counter = 0  # For heap tie-breaker
total_records = 0
file_counter = 0

filtered_files = [
    file
    for file in files
    if datetime.datetime.strptime(file.stem, "%Y-%m-%d").date() < END_DATE.date()
]

for file in tqdm(filtered_files):
    with open(file) as f:
        for line in f:
            record: Record = json.loads(line)

            ts = calc_timestamp(record)

            # Invalid rkey, or TS is past END_DATE
            if ts is None or ts >= end_ts:
                continue

            record_with_ts: Record = {"ts": ts, **record}  # type: ignore

            # Add to heap, sorted by ts
            heapq.heappush(record_buffer, (ts, counter, record_with_ts))
            counter += 1

            # If buffer is full, write out the smallest N records
            if len(record_buffer) >= BATCH_SIZE * 2:
                with open(f"{TEMP_DIR}/{file_counter}.json", "w") as outf:
                    smallest_n = []
                    for _ in range(BATCH_SIZE):
                        if record_buffer:
                            _, _, record = heapq.heappop(record_buffer)
                            smallest_n.append(record)
                            total_records += 1
                    json.dump({"records": smallest_n}, outf)
                file_counter += 1

# Write remaining records
while record_buffer:
    batch = []
    while record_buffer and len(batch) < BATCH_SIZE:
        _, _, record = heapq.heappop(record_buffer)
        batch.append(record)
        total_records += 1

    if batch:
        with open(f"{TEMP_DIR}/{file_counter}.json", "w") as outf:
            json.dump({"records": batch}, outf)
        file_counter += 1


# ==== IDENTIFY DELETED POSTS AND USERS ====

users = set[str]()
posts = set[str]()
deleted_users = set[str]()
deleted_posts = set[str]()

# Iterate through each historical record in Bluesky's firehose
for record in records(TEMP_DIR, end_date=END_DATE):
    users.add(record["did"])

    match record["$type"]:
        case "app.bsky.feed.post":
            # if record["uri"] == inspected_uri:
            #     print("FOUND OG: ", record)

            posts.add(record["uri"])

            # If a user replied to a post, we know that user saw all parent posts in that thread
            if "reply" in record and record["reply"]:
                root_uri = record["reply"]["root"]["uri"]
                parent_uri = record["reply"]["parent"]["uri"]

                # if root_uri == inspected_uri:
                #     print("FOUND IN ROOT: ", record)

                # if parent_uri == inspected_uri:
                #     print("FOUND IN PARENT: ", record)

                if root_uri:  # Hacky, for one-off case
                    if root_uri not in posts:
                        deleted_posts.add(root_uri)

                    root_did = did_from_uri(root_uri)
                    if root_did not in users:
                        deleted_users.add(root_did)

                if parent_uri:  # Hacky, for one-off case
                    if parent_uri not in posts:
                        deleted_posts.add(parent_uri)

                    parent_did = did_from_uri(parent_uri)
                    if parent_did not in users:
                        deleted_users.add(parent_did)

            # If a user quoted a post, we know they saw the subject post
            quoted_uri = get_quoted_uri(record)
            if quoted_uri:
                # if quoted_uri == inspected_uri:
                #     print("FOUND IN QUOTE: ", record)

                if quoted_uri not in posts:
                    deleted_posts.add(quoted_uri)

                quoted_did = did_from_uri(quoted_uri)
                if quoted_did not in users:
                    deleted_users.add(quoted_did)

        case "app.bsky.graph.follow":
            if record["subject"] not in users:
                # Probably a deleted user? Maybe a bug?
                deleted_users.add(record["subject"])

        case "app.bsky.feed.like":
            subject_uri = record["subject"]["uri"]

            # if subject_uri == inspected_uri:
            #     print("FOUND IN LIKE: ", record)

            if subject_uri not in posts:
                deleted_posts.add(subject_uri)

            subect_did = did_from_uri(subject_uri)
            if subect_did not in users:
                deleted_users.add(subect_did)

        case "app.bsky.feed.repost":
            subject_uri = record["subject"]["uri"]

            # if subject_uri == inspected_uri:
            #     print("FOUND IN REPOST: ", record)

            if subject_uri not in posts:
                deleted_posts.add(subject_uri)

            subect_did = did_from_uri(subject_uri)
            if subect_did not in users:
                deleted_users.add(subect_did)

        case _:
            continue

# ==== VALIDATION ====

# Check for any users that are marked as both existing and deleted
user_overlap = deleted_users.intersection(set(users))
if user_overlap:
    print(
        f"\nWarning: Found {len(user_overlap)} users that are both deleted and existing:"
    )
    print(user_overlap)

# Check for any posts that are marked as both existing and deleted
post_overlap = deleted_posts.intersection(set(posts))
if post_overlap:
    print(
        f"\nWarning: Found {len(post_overlap)} posts that are both deleted and existing:"
    )
    print(post_overlap)

print(f"Total deleted posts: {len(deleted_posts)}")

total_posts = len(posts) + len(deleted_posts)
deletion_rate = (len(deleted_posts) / total_posts) * 100 if total_posts > 0 else 0
print(f"\nDeletion rate: {deletion_rate:.2f}% of all posts")


# ==== DELETE RE-INSERTION ====

# Filter out invalid rkeys from deleted posts
valid_deletes: list[str] = []
for uri in deleted_posts:
    rkey = rkey_from_uri(uri)
    if rkey:
        try:
            parse_rkey(rkey)
            valid_deletes.append(uri)
        except Exception as err:
            print("Error parsing rkey/uri!", uri)
            print("Error: ", err)

# Sort deletes
sorted_deletes = sorted(valid_deletes, key=lambda x: x.split("/")[-1])

delete_ts = -1
delete_idx = 0
last_ts = -1

stream_dir = Path(TEMP_DIR)
files = sorted(stream_dir.glob("*.json"), key=lambda x: int(x.stem))

# Insert deleted posts into firehose
for file in files:
    new_batch = []

    with open(file) as f:
        data: list[Record] = json.load(f)["records"]
        for record in data:
            ts = record["ts"]

            # If deleted record TS in between last and current, insert
            while (
                delete_idx < len(sorted_deletes)
                and delete_ts >= last_ts
                and delete_ts <= ts
            ):
                new_batch.append(
                    {
                        "$type": "app.bsky.feed.post",
                        "ts": ts,
                        "did": did_from_uri(sorted_deletes[delete_idx]),
                        "uri": sorted_deletes[delete_idx],
                        "deleted": True,
                    }
                )
                delete_idx += 1
                if delete_idx < len(sorted_deletes):
                    delete_ts, _ = parse_rkey(rkey_from_uri(sorted_deletes[delete_idx]))  # type: ignore

            new_batch.append(record)
            last_ts = ts

    with open(f"{OUT_DIR}/{file.name}", "w") as outf:
        print(f"New batch length: {len(new_batch)}")
        json.dump({"records": new_batch}, outf)


# Clean up temp directory
shutil.rmtree(TEMP_DIR)
