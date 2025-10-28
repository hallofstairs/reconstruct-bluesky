# %% Imports

import json

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm

SESSIONS_PATH = "./data/sessions-2023-04-01.jsonl"

plt.rcParams["figure.dpi"] = 300
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 12
plt.rcParams["axes.labelsize"] = 12
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["xtick.labelsize"] = 10
plt.rcParams["ytick.labelsize"] = 10
plt.rcParams["legend.fontsize"] = 10
plt.rcParams["figure.titlesize"] = 16


# %% Read in data


# Read in sessions file
with open(SESSIONS_PATH, "r") as f:
    data = [json.loads(line.strip()) for line in f if line.strip()]
    sessions = sorted(data, key=lambda x: x["end_ts"])


# TODO: nRank scoring

interactive_users = set()
metrics: dict[str, list] = {
    "precisions": [],
    "recalls": [],
    "n_impressions": [],
    "n_interactions": [],
    "dids": [],
    "session_length_mins": [],
    "n_notifications": [],
}


for data in tqdm(sessions):
    # Number of total interactions
    interactions = [
        record["subject"]["uri"]
        for record in data["actions"]
        if record["$type"] in ["app.bsky.feed.like", "app.bsky.feed.repost"]
    ]

    interacted_uris = set(interactions)
    impression_uris = set(data["impressions"])
    metrics["n_notifications"].append(len(data["notifications"]))
    metrics["n_impressions"].append(len(impression_uris))
    metrics["n_interactions"].append(len(interactions))
    metrics["session_length_mins"].append((data["end_ts"] - data["start_ts"]) / 60_000)

    interactive_users.add(data["did"])
    metrics["dids"].append(data["did"])
    captured_uris = interacted_uris.intersection(impression_uris)

    recall = (
        len(captured_uris) / len(interacted_uris) if len(interacted_uris) > 0 else 0
    )
    precision = (
        len(captured_uris) / len(impression_uris) if len(impression_uris) > 0 else 0
    )
    metrics["precisions"].append(precision)
    metrics["recalls"].append(recall)

raw_df = pd.DataFrame(metrics)


# TODO: Try only tracking notifs at session start, not during
# Metric: recall, but penalize the number of impressions it takes to achieve that
# Question: how often, for a given user, does a session start with a follow?
#   - Was that triggered by a notification?
# HMMs
# Even if I have to take KGS offer, I can send in any BAAs that I want based on this research

# %% Plot session length against notification count

# TODO: Make sure this isn't accidentally counting that user's interactions
test_df = raw_df.copy()
test_df["n_notifications"] += 1

# Plot notifications vs session length
plt.figure(figsize=(10, 6))

# Create bins and calculate mean session length per bin
min_val = test_df["n_notifications"].min()
max_val = test_df["n_notifications"].max()

bins = np.logspace(
    np.log10(min_val),
    np.log10(max_val),
    20,
)
binned_data = pd.cut(test_df["n_notifications"], bins, include_lowest=True, right=True)
mean_session_len = test_df.groupby(binned_data)["session_length_mins"].mean()
bin_centers = (bins[:-1] + bins[1:]) / 2

plt.loglog(bin_centers, mean_session_len, "o-")

# Calculate slope of log-log line
x = np.log10(bin_centers)
y = np.log10(mean_session_len)
slope, _ = np.polyfit(x, y, 1)
# Plot the fitted line
plt.plot(
    bin_centers,
    10 ** (slope * np.log10(bin_centers) + list(y)[0]),
    "--",
    label=f"Slope: {slope:.2f}",
)
plt.legend()

plt.xlabel("Number of Notifications")
plt.ylabel("Average Session Length")
plt.title("Average Notifications vs Session Length")

# %%

plt.scatter(test_df["n_notifications"], test_df["session_length_mins"], alpha=0.5)
plt.yscale("log")
plt.ylabel("Session Length (mins)")
plt.xlabel("Incoming Notification Count")
plt.legend()
plt.show()
# plt.title("Avg. Session Length vs. Notification Count")


# %% Filter for users with a min number of total interactions

MIN_INTERACTIONS = 5

total_interactions = raw_df.groupby("dids")["n_interactions"].sum()
active_users = total_interactions[total_interactions >= MIN_INTERACTIONS].index
df = raw_df[raw_df["dids"].isin(active_users)]

print(
    f"\nNumber of users with {MIN_INTERACTIONS}+ interactions: {len(active_users)}/{len(total_interactions)}"
)


# %% Interaction counts

# Get value counts of interactions grouped by did
interaction_counts = (
    df.groupby("dids")["n_interactions"].sum().sort_values(ascending=False)
)
print("\n=== INTERACTIONS PER USER ===")
print(interaction_counts)

# %% Session time

total_session_time = (
    df.groupby("dids")["session_length_mins"].sum().sort_values(ascending=False)
)
print("\n=== TOTAL SESSION TIME BY USERS ===")
print(total_session_time)

print("Average session time (mins): ", df["session_length_mins"].mean())
print("Median session time (mins): ", df["session_length_mins"].median())

# TODO: Plot?

# %% Recall per user

recall_means = df.groupby("dids")["recalls"].mean().sort_values(ascending=False)
print("\n=== MEAN RECALL PER USER ===")
print(recall_means)

plt.figure(figsize=(10, 6))
plt.hist(recall_means, bins=20, edgecolor="black")
plt.title("Average Recall per User")
plt.xlabel("Mean Recall")
plt.ylabel("Number of Users")
plt.text(
    0.95,
    0.95,
    f"n={len(recall_means)}",
    transform=plt.gca().transAxes,
    horizontalalignment="right",
    verticalalignment="top",
    fontsize=12,
    bbox=dict(facecolor="white", alpha=0.8),
)

plt.show()

# %% Calc F1-score


# %% Plot sessions on an x axis, see if we can interpolate?

# TODO:


# %%
