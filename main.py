# Imports

import json
import os
import typing as t
from datetime import datetime, timezone

from utils import Like, Post, Record, get_quoted_uri, records

# Constants

IN_DIR = "./data/firehose-2023-05-01"

REFRESH_SIZE = 20
MAX_POSTS_PER_USER = 60
MAX_POSTS_PER_SESSION = REFRESH_SIZE * 1
END_DATE = datetime(2023, 5, 1, tzinfo=timezone.utc)

SESSION_IDLE_THRESHOLD = 30  # minutes
SESSIONS_PATH = f"./data/sessions-{END_DATE.date()}.jsonl"

FAKE_TEMP_CUTOFF = 20  # TODO: Remove

if os.path.exists(SESSIONS_PATH):
    os.remove(SESSIONS_PATH)


FeedType = t.Literal["following", "profile"]

# TODO: Make this more programmatic
BOTS = ["did:plc:fjsmdevv3mmzc3dpd36u5yxc"]

# TODO: Notifications


class Users:
    class Info(t.TypedDict):
        posts: list[str]
        following: list[str]
        last_interaction_ts: int | None
        feed: list[str]
        # notifications: list[Record]

    class FeedView(t.TypedDict):
        source: FeedType
        posts: list[Post]

    class Session(t.TypedDict):
        """Basic information about a session."""

        did: str
        session_num: int
        start_ts: int
        end_ts: int
        impressions: set[str]
        actions: list[Record]
        # notifications: list[Record]

    def __init__(self) -> None:
        self.info: dict[str, Users.Info] = {}
        self.sessions: dict[str, Users.Session] = {}

    def is_bot(self, did: str) -> bool:
        return did in BOTS

    def log_user(self, did: str) -> None:
        self.info[did] = {
            "following": [],
            "posts": [],
            "last_interaction_ts": -1,
            "feed": [],
            # "notifications": [],
        }

    def is_new_session(
        self, did: str, now_ts: int, idle_threshold: int = SESSION_IDLE_THRESHOLD
    ) -> bool:
        last_interaction_ts = self.info[did]["last_interaction_ts"]
        if last_interaction_ts is None:
            return True

        mins_since_last_record = (now_ts - last_interaction_ts) / 60_000
        return mins_since_last_record > idle_threshold

    def get_following_feed(self, did: str) -> list:
        """Reconstruct a user's chronolical Following feed at any given time."""
        feed: list[str] = []

        # If user has posted anything:
        if self.info[did]["posts"]:
            # Insert most recent K posts into their chron. feed
            feed.extend(list(self.info[did]["posts"])[-MAX_POSTS_PER_USER:])

        # Add posts from followings' timelines
        for following_did in self.info[did]["following"]:
            if self.info[following_did]["posts"]:
                feed.extend(
                    list(self.info[following_did]["posts"])[-MAX_POSTS_PER_USER:]
                )

        sorted_feed = sorted(feed, key=lambda x: x.split("/")[-1], reverse=True)[
            :MAX_POSTS_PER_SESSION
        ]

        # feed_with_threads = []

        # # TODO: Check the thread delivery implementation
        # for post in sorted_feed:
        #     thread = posts.gather_thread(post) + [post]
        #     feed_with_threads.extend(thread)

        # return list(dict.fromkeys(sorted_feed))[:MAX_POSTS_PER_SESSION]
        return sorted_feed

    # TODO: Verify, especially number of posts
    def get_profile_feed(self, subject_did: str) -> list:
        """Reconstruct a single user's timeline of posts."""
        return list(self.info[subject_did]["posts"])[-REFRESH_SIZE:][:REFRESH_SIZE]

    def log_session(self, did: str, now_ts: int) -> None:
        next_session_num = (
            self.sessions[did]["session_num"] + 1 if did in self.sessions else 0
        )

        # Write previous session to disk, if it exists
        if did in self.sessions:
            with open(SESSIONS_PATH, "a") as f:
                json.dump(
                    {
                        **self.sessions[did],
                        "impressions": list(self.sessions[did]["impressions"]),
                    },
                    f,
                )
                f.write("\n")

        self.info[did]["feed"] = self.get_following_feed(did)
        self.sessions[did] = {
            "session_num": next_session_num,
            "did": did,
            "start_ts": now_ts,
            "end_ts": now_ts,  # TODO: Below
            "impressions": set(self.info[did]["feed"][:FAKE_TEMP_CUTOFF]),
            "actions": [],
            # "notifications": [],
        }

    # def read_notifications(self, did: str) -> None:
    #     # TODO: Probably a limit on notifications? Idk, let's see
    #     self.sessions[did]["notifications"].extend(self.info[did]["notifications"])
    #     self.info[did]["notifications"] = []

    def dump_sessions(self) -> None:
        sorted_sessions = sorted(self.sessions.values(), key=lambda x: x["end_ts"])
        for session in sorted_sessions:
            with open(SESSIONS_PATH, "a") as f:
                json.dump({**session, "impressions": list(session["impressions"])}, f)
                f.write("\n")

    def get_idx(self, val: str, vals: list[str]) -> int | None:
        try:
            idx = vals.index(val)
        except ValueError:
            idx = None

        return idx

    def log_like(self, did: str, subject_uri: str, record: Like) -> None:
        return
        # session_id = self.get_session_id(did)

        # Log the record
        # self.sessions[session_id]["actions"].append(record)

        # user_feed = self.info[did]["feed"]

        # Update seen in following
        # idx = self.get_idx(subject_uri, user_feed)
        # if idx:
        #     self.sessions[session_id]["impressions"] = self.info[did]["feed"][: idx + 1]

    def log_post(self, did: str, post: Post) -> None:
        uri = post["uri"]

        if uri in self.info[did]["posts"]:  # This shouldn't happen, but precaution
            print(f"WARNING! Post {uri} already exists for user {did}")

        self.info[did]["posts"].append(uri)

        # if did not in BOTS:
        #     self.sessions[did]["impressions"].add(uri)
        #     # TODO: Uncomment
        #     self.sessions[did]["impressions"].update(posts.gather_thread(uri))
        #     subject_uri = posts.info[uri]["subject_uri"]
        #     if subject_uri:
        #         self.sessions[did]["impressions"].add(subject_uri)

    # def send_notification(self, subject_did: str, record: Record) -> None:
    #     users.info[subject_did]["notifications"].append(record)

    def log_follow(self, did: str, subject_did: str) -> None:
        if subject_did not in self.info:
            # TODO: I'm pretty sure this would be a deleted user?
            self.log_user(subject_did)

        # TODO:
        # Update session information
        # Start new profile view

        if subject_did in self.info[did]["following"]:
            return  # This happens semi-frequently, bug in data repos

        self.info[did]["following"].append(subject_did)
        # self.sessions[did]["impressions"].update(self.get_profile_feed(subject_did))


class Posts:
    class Info(t.TypedDict):
        parent_uri: str | None  # Allows thread reconstruction on-the-fly
        subject_uri: str | None

    def __init__(self) -> None:
        self.info: dict[str, Posts.Info] = {}
        self.deleted = set[str]()

    def log_post(self, record: Post) -> None:
        self.info[record["uri"]] = {
            "parent_uri": None,
            "subject_uri": None,
        }

        if "reply" in record and record["reply"]:
            self.info[record["uri"]]["parent_uri"] = record["reply"]["parent"]["uri"]

        quoted_uri = get_quoted_uri(record)
        if quoted_uri:
            self.info[record["uri"]]["subject_uri"] = quoted_uri

    def log_deleted(self, uri: str) -> None:
        self.deleted.add(uri)

    def gather_thread(self, uri: str) -> list[str]:
        """Gather the URIs of all posts in a thread from a single reply to its root."""
        post = self.info[uri]  # Info about current post
        uris: list[str] = []

        while True:
            if post["parent_uri"] is None:
                return uris

            if post["parent_uri"] not in self.info:
                return uris

            uris.append(post["parent_uri"])
            post = self.info[post["parent_uri"]]


users = Users()
posts = Posts()

# === Firehose Iteration ===

# Iterate through each historical record in Bluesky's firehose
for record in records(IN_DIR, end_date=END_DATE):
    ts = record["ts"]
    did = record["did"]

    if did not in users.info:
        users.log_user(did)

    # Session management
    if did not in BOTS and users.is_new_session(did, ts):
        users.log_session(did, ts)
        # users.read_notifications(did)

    if record["$type"] == "app.bsky.feed.post":
        # Skip replies and quotes, for now
        if "reply" in record or "embed" in record:
            continue

        posts.log_post(record)
        users.log_post(did, record)

        # if "reply" in record and record["reply"]:
        #     try:
        #         root_did = did_from_uri(record["reply"]["root"]["uri"])
        #         parent_did = did_from_uri(record["reply"]["parent"]["uri"])

        #         users.send_notification(root_did, record)
        #         users.send_notification(parent_did, record)
        #     except ValueError:
        #         print("Invalid URI, ", record)

    if record["$type"] == "app.bsky.feed.like":
        if did not in BOTS:
            users.log_like(did, record["subject"]["uri"], record)

            # subject_did = did_from_uri(record["subject"]["uri"])
            # users.send_notification(subject_did, record)

    if record["$type"] == "app.bsky.graph.follow":
        users.log_follow(did, record["subject"])
        # users.send_notification(record["subject"], record)

    users.info[did]["last_interaction_ts"] = ts

    if did not in BOTS:
        users.sessions[did]["end_ts"] = ts
        users.sessions[did]["actions"].append(record)
        # users.read_notifications(did)  # TODO: Add back

    # TODO: Blocks

    # Every time a user starts a new session, they land on their chronological feed screen
    # So, at the start of each session, we need to gather a user's feed up-to-date chron feed
    # Based on the actions they take during that session, we can guess how many of the posts
    #   from their chron feed they've seen, as well as other posts from other screens
    # If a user likes a post from their chronological feed:
    #   - Mark all posts until that idx as seen (TODO: Until end of refresh?)
    # If a user follows a user:
    #   - Mark the top N posts of that user as seen (TODO: What's N?) or last idx of liked, if liked
    # If a user replies to a post from their chronological feed, that is a new screen
    #   - Mark all parent replies in that thread as seen

    # If they take an action based on that feed, we:

users.dump_sessions()
