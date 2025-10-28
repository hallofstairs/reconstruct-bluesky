import mmap
from pathlib import Path
import typing as t
from datetime import datetime

import ujson as json
from tqdm import tqdm

T = t.TypeVar("T")

# === Iteration utils ===


class jsonl[T]:
    @classmethod
    def iter(cls, path: str) -> t.Generator[T, None, None]:
        with open(path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                for line in iter(mm.readline, b""):
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        print(f"JSONDecodeError: {line}")
                        continue


def records(
    stream_path: str, end_date: datetime | None = None, batch_size: int = 1_000_000
) -> t.Generator["Record", None, None]:
    """
    Generator that yields records from the stream for the given date range.

    End date is not incluive.
    """

    path = Path(stream_path)
    files = sorted(path.glob("*.json"), key=lambda x: int(x.stem))

    for fname in tqdm(files, total=len(files)):
        with open(fname, "r") as f:
            records: list[Record] = json.load(f)["records"]
            for record in records:
                if end_date and record["ts"] > end_date.timestamp() * 1000:
                    return
                yield record


# === Data types ===


class CidUri(t.TypedDict):
    cid: str
    uri: str


ExistingPost = t.TypedDict(
    "ExistingPost",
    {
        "$type": t.Literal["app.bsky.feed.post"],
        "ts": int,
        "did": str,
        "uri": str,
        "text": str,
        "createdAt": str,
        "reply": t.Optional[dict[t.Literal["root", "parent"], CidUri]],
        "embed": t.Optional[dict[str, t.Any]],
    },
)

DeletedPost = t.TypedDict(
    "DeletedPost",
    {
        "$type": t.Literal["app.bsky.feed.post"],
        "ts": int,
        "did": str,
        "uri": str,
        "deleted": t.Literal[True],
    },
)

Follow = t.TypedDict(
    "Follow",
    {
        "$type": t.Literal["app.bsky.graph.follow"],
        "ts": int,
        "did": str,  # DID of the follower
        "uri": str,  # URI of the follow record
        "createdAt": str,  # Timestamp of the follow
        "subject": str,  # DID of the followed user
    },
)

Repost = t.TypedDict(
    "Repost",
    {
        "$type": t.Literal["app.bsky.feed.repost"],
        "ts": int,
        "did": str,
        "uri": str,
        "createdAt": str,
        "subject": CidUri,
    },
)

Like = t.TypedDict(
    "Like",
    {
        "$type": t.Literal["app.bsky.feed.like"],
        "ts": int,
        "did": str,
        "uri": str,
        "createdAt": str,
        "subject": CidUri,
    },
)

Block = t.TypedDict(
    "Block",
    {
        "$type": t.Literal["app.bsky.graph.block"],
        "ts": int,
        "did": str,
        "uri": str,
        "createdAt": str,
        "subject": str,
    },
)

Profile = t.TypedDict(
    "Profile",
    {
        "$type": t.Literal["app.bsky.actor.profile"],
        "ts": int,
        "did": str,
        "createdAt": str,
    },
)

Post = ExistingPost | DeletedPost
Record = Post | Follow | Repost | Like | Block | Profile


# === Data utils ===


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


def rkey_from_uri(uri: str) -> str | None:
    rkey = uri.split("/")[-1]

    if not isinstance(rkey, str) or not rkey.isalnum() or len(rkey) != 13:
        return None

    return rkey


def parse_rkey(rev: str) -> tuple[int, int]:
    """Extract the data from the rkey of a URI. Returns (timestamp, clock_id) tuple.
    timestamp is Unix timestamp in microseconds."""

    timestamp = s32.decode(rev[:-2]) // 1000  # unix, milliseconds
    clock_id = s32.decode(rev[-2:])

    return timestamp, clock_id


def did_from_uri(uri: str) -> str:
    if not uri:
        raise ValueError("\nMisformatted URI (empty string)")

    try:
        return uri.split("/")[2]
    except Exception:
        raise ValueError(f"\nMisformatted URI: {uri}")


class s32:
    S32_CHAR = "234567abcdefghijklmnopqrstuvwxyz"

    @classmethod
    def encode(cls, i: int) -> str:
        s = ""
        while i:
            c = i % 32
            i = i // 32
            s = s32.S32_CHAR[c] + s
        return s

    @classmethod
    def decode(cls, s: str) -> int:
        i = 0
        for c in s:
            i = i * 32 + s32.S32_CHAR.index(c)
        return i
