#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

from telethon import TelegramClient
from telethon.tl.types import Message


def env_int(name: str) -> int:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing env var: {name}")
    try:
        return int(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid integer for {name}: {value}") from exc


def env_str(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing env var: {name}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch messages you sent to a Telegram bot via Telethon."
    )
    parser.add_argument(
        "--bot",
        default=os.getenv("GMGN_BOT_ID", "@GMGN_bsc_bot"),
        help="Target bot username (default: GMGN_BOT_ID or @GMGN_bsc_bot).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max number of recent messages to scan (default: 50).",
    )
    parser.add_argument(
        "--session",
        default="telethon_gmgn_session",
        help="Session file name (default: telethon_gmgn_session).",
    )
    return parser.parse_args()


def format_message(msg: Message) -> str:
    timestamp = msg.date.strftime("%d/%m/%y %H:%M:%S") if msg.date else "unknown-time"
    text = msg.message or ""
    return f"[{timestamp}] {text}"


def main() -> None:
    args = parse_args()
    api_id = env_int("TG_APP_ID_TWEET")
    api_hash = env_str("TG_APP_HASH_TWEET")

    client = TelegramClient(args.session, api_id, api_hash)
    with client:
        entity = client.get_entity(args.bot)
        printed = 0
        for msg in client.iter_messages(entity, limit=args.limit):
            if msg.out:
                print(format_message(msg))
                printed += 1
        if printed == 0:
            print("No outgoing messages found in the scanned range.")


if __name__ == "__main__":
    main()
