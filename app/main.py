from __future__ import annotations

import argparse
import logging

from app.auth import get_google_credentials
from app.config import Settings, load_dotenv
from app.drive_service import DriveService
from app.logger import configure_logging


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
    parser.add_argument("command", choices=["poll-once"], help="Execution command")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "poll-once":
        return run_poll_once()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

