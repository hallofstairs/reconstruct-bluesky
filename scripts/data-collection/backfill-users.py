import csv
import json

import requests

write_threshold = 500_000
total_records = 0
after = ""
dids = []
seen_dids = set()
unsaved_dids = False

CSV_FILE = "dids.csv"
PLC_URL = "https://plc.directory"

# TODO: RATE LIMIT = 500 req. per 5 min.


def write_to_file(data):
    global unsaved_dids

    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["did", "created_at"])  # Write header
        for did_info in data:
            writer.writerow([did_info["did"], did_info["created_at"]])

    unsaved_dids = False
    print(f"Data written to {CSV_FILE}. Total DIDs: {len(dids)} After: {after}")


try:
    while True:
        res = requests.get(
            f"{PLC_URL}/export?limit=1000" + (f"&after={after}" if after else ""),
        )

        records = json.loads("[" + res.text.replace("\n", ",") + "]")

        if len(records) == 0:
            print("OUT OF RECORDS, ", res.text)
            break

        for record in records:
            unsaved_dids = True

            try:
                did = record["did"]
                if did in seen_dids:
                    continue

                dids.append(
                    {
                        "did": did,
                        "created_at": record["createdAt"],
                    }
                )

                seen_dids.add(did)
                total_records += 1

            except Exception as e:
                print(e, record)

            if total_records % write_threshold == 0:
                write_to_file(dids)

        after = records[-1]["createdAt"]

except Exception as e:
    print(f"An error occurred: {e}")
    if unsaved_dids:
        write_to_file(dids)

finally:
    if unsaved_dids:
        write_to_file(dids)

print(f"Data collection complete. All DIDs ({len(dids)}) have been written to dids.csv")
