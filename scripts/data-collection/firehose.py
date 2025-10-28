import asyncio
import json
from datetime import datetime

import websockets


async def connect_firehose():
    """Connect to the Bluesky firehose websocket stream"""
    uri = "wss://jetstream2.us-east.bsky.network/subscribe"

    async with websockets.connect(uri) as websocket:
        print(f"Connected to firehose at {datetime.now()}")

        try:
            while True:
                message = await websocket.recv()
                try:
                    # Message is already bytes, don't decode
                    data = json.loads(message)
                except UnicodeDecodeError:
                    print(f"Error: {message[:50]}... is not valid UTF-8")
                    continue

                print(json.dumps(data, indent=2))

        except websockets.exceptions.ConnectionClosed:
            print("Connection closed, attempting to reconnect...")
        except Exception as e:
            print(f"Error: {e}")


async def main():
    while True:
        try:
            await connect_firehose()
        except Exception as e:
            print(f"Connection failed: {e}")
            print("Retrying in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
