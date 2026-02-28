from __future__ import annotations

import os

import pytest

from app.auth import get_google_credentials
from app.config import Settings
from app.drive_service import DriveService


@pytest.mark.integration
def test_list_inbox_files_integration() -> None:
    if os.getenv("RUN_DRIVE_INTEGRATION_TESTS", "").lower() not in {"1", "true", "yes"}:
        pytest.skip("Set RUN_DRIVE_INTEGRATION_TESTS=1 to run integration tests.")

    settings = Settings.from_env()
    credentials = get_google_credentials(settings)
    drive = DriveService.from_credentials(credentials, settings)
    files = drive.list_inbox_files()
    assert isinstance(files, list)

