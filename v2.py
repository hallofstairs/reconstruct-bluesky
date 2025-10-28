# %% Imports

import datetime
import typing as t

import numpy as np

from utils import Post, did_from_uri, records

# Constants

RAW_STREAM_DIR = "../data/stream-2023-07-01"

REFRESH_SIZE = 20
SESSION_IDLE_THRESHOLD = 30 * 60  # 30 minutes
END_DATE = "2023-04-01"

# TODO: You can also reconstruct deleted post users, timestamps from URI

# === Prediction Tracking ===

# What posts do we predict that each user has seen?
predicted_seen: dict[str, list[tuple[str, str]]] = {}

# What posts do we know that each user has seen based on their interactions?
true_seen: dict[str, list[tuple[str, str]]] = {}

# === Network Information Management ===


# === Session Management ===


class Batch(t.TypedDict):
    served: list[tuple[str, str, str]]
    liked: list[tuple[str, str]]


def to_ts(created_at: str) -> int:
    return int(datetime.datetime.fromisoformat(created_at).timestamp())


def is_new_session(
    now_ts: int,
    last_record_ts: int | None,
    idle_threshold=30 * 60,  # 30 minutes
) -> bool:
    if last_record_ts is None:
        return True

    time_since_last_record = now_ts - last_record_ts
    return time_since_last_record > idle_threshold


# === Helper functions ===


def get_quoted_uri(post: Post) -> t.Optional[str]:
    if "embed" not in post or not post["embed"]:
        return None

    try:
        if post["embed"]["$type"] == "app.bsky.embed.record":
            return post["embed"]["record"]["uri"]
        elif post["embed"]["$type"] == "app.bsky.embed.recordWithMedia":
            return post["embed"]["record"]["record"]["uri"]
    except KeyError:
        return None


FeedType = t.Literal["following", "profile"]


class Users:
    class Info(t.TypedDict):
        posts: set[str]
        following: set[str]
        curr_session_num: int
        last_interaction_ts: int | None

    class FeedView(t.TypedDict):
        source: FeedType
        posts: list[Post]
        reactions: list

    class Session(t.TypedDict):
        """Basic information about a session."""

        did: str
        start_ts: int
        end_ts: int
        feeds: list["Users.FeedView"]

    def __init__(self) -> None:
        self.info: dict[str, Users.Info] = {}
        self.sessions: dict[str, Users.Session] = {}

    def add_user(self, did: str) -> None:
        self.info[did] = {
            "following": set(),
            "posts": set(),
            "curr_session_num": 0,
            "last_interaction_ts": -1,
        }

    def add_post(self, did: str, uri: str) -> None:
        self.info[did]["posts"].add(uri)

    def add_follow(self, did: str, subject_did: str) -> None:
        if subject_did not in self.info:
            self.add_user(did)  # TODO: I'm pretty sure this would be a deleted user?

        self.info[did]["following"].add(subject_did)

    def get_session_id(self, did: str) -> str:
        return f"{did}-{self.info[did]['curr_session_num']}"

    def is_new_session(
        self, did: str, now_ts: int, idle_threshold: int = SESSION_IDLE_THRESHOLD
    ) -> bool:
        last_interaction_ts = self.info[did]["last_interaction_ts"]
        if last_interaction_ts is None:
            return True

        time_since_last_record = now_ts - last_interaction_ts
        return time_since_last_record > idle_threshold

    def get_feed(self, did: str, source: FeedType) -> list:
        return

    def add_session(self, did: str, now_ts: int) -> None:
        self.info[did]["curr_session_num"] += 1
        session_id = self.get_session_id(did)

        feed = self.get_feed(did, "following")

        self.sessions[session_id] = {
            "did": did,
            "start_ts": now_ts,
            "end_ts": now_ts,
            "feeds": [{"source": "following", "posts": [], "reactions": []}],
        }

        return

    def update_session(self, did: str, record: str) -> None:
        match record:
            case "follow":
                return


class Feeds:
    def __init__(self) -> None:
        pass


class Posts:
    class Info(t.TypedDict):
        parent_uri: str | None  # Allows thread reconstruction on-the-fly
        subject_uri: str | None

    def __init__(self) -> None:
        self.info: dict[str, Posts.Info] = {}
        self.deleted = set[str]()

    def add_post(self, uri: str) -> None:
        self.info[uri] = {
            "parent_uri": None,
            "subject_uri": None,
        }

    def add_deleted(self, uri: str) -> None:
        self.deleted.add(uri)

    def gather_thread(self, uri: str) -> list[str]:
        """Gather the URIs of parents in the thread, including the root."""
        post = self.info[uri]  # Info about current post
        uris: list[str] = []

        while True:
            # Found root
            if post["parent_uri"] is None:
                return uris

            # Broken thread (TODO: Could also return root here?)
            if post["parent_uri"] not in self.info:
                self.add_deleted(post["parent_uri"])
                return uris

            uris.append(post["parent_uri"])
            post = self.info[post["parent_uri"]]


users = Users()
feeds = Feeds()
posts = Posts()


# === Firehose Iteration ===

END_DATE = "2023-04-01"

# Iterate through each historical record in Bluesky's firehose
for record in records(RAW_STREAM_DIR, end_date=END_DATE, log=True):
    now_ts = to_ts(record["createdAt"])
    did = record["did"]

    if did not in users.info:
        users.add_user(did)

    # Session management
    if users.is_new_session(did, now_ts):
        users.add_session(did, now_ts)

    if record["$type"] == "app.bsky.feed.post":
        users.add_post(did, record["uri"])
        posts.add_post(record["uri"])

    if record["$type"] == "app.bsky.feed.like":
        # Update reactions to current view
        users.update_session(did, record)
        # Update list of seen posts, based on current view
        # Update current view
        continue

    if record["$type"] == "app.bsky.graph.follow":
        users.add_follow(did, record["subject"])

        # Update session actions
        users.update_session(did, record)

        # Update session information
        # User followed another user, meaning they saw their profile
        # Start new profile view
        continue

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

# TODO: Evaluation
# ranking
# recall
# precision
