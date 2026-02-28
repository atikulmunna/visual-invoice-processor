from __future__ import annotations

import argparse
import logging

from app.auth import get_google_credentials
from app.config import Settings, load_dotenv
from app.drive_service import DriveService
from app.logger import configure_logging
from app.replay import replay_failures


def run_poll_once() -> int:
    load_dotenv()
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    credentials = get_google_credentials(settings)
    drive = DriveService.from_credentials(credentials, settings)
    files = drive.list_inbox_files()
    logging.getLogger(__name__).info("Found %d candidate files in inbox", len(files))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Visual Invoice Processor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    poll = subparsers.add_parser("poll-once", help="Run one poll cycle")
    _ = poll

    replay = subparsers.add_parser("replay", help="Replay dead-letter items")
    replay.add_argument("--status", required=True, choices=["FAILED", "REVIEW_REQUIRED"])
    replay.add_argument("--dead-letter-path", default="logs/dead_letter.jsonl")
    replay.add_argument("--audit-path", default="logs/replay_audit.jsonl")
    replay.add_argument("--claim-db-path", default="data/metadata.db")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "poll-once":
        return run_poll_once()
    if args.command == "replay":
        summary = replay_failures(
            status=args.status,
            dead_letter_path=args.dead_letter_path,
            audit_path=args.audit_path,
            claim_db_path=args.claim_db_path,
        )
        logging.getLogger(__name__).info(
            "Replay summary queued=%d skipped_processed=%d skipped_invalid=%d",
            summary["queued"],
            summary["skipped_processed"],
            summary["skipped_invalid"],
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
