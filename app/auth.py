from __future__ import annotations

from pathlib import Path
from typing import Any

from app.config import Settings


def get_google_credentials(settings: Settings) -> Any:
    if settings.google_auth_mode == "service_account":
        return _service_account_credentials(settings)
    return _oauth_credentials(settings)


def _service_account_credentials(settings: Settings) -> Any:
    try:
        from google.oauth2 import service_account
    except ImportError as exc:
        raise RuntimeError(
            "google-auth is required for service account authentication"
        ) from exc

    assert settings.google_service_account_file is not None
    return service_account.Credentials.from_service_account_file(
        settings.google_service_account_file,
        scopes=list(settings.google_scopes),
    )


def _oauth_credentials(settings: Settings) -> Any:
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError as exc:
        raise RuntimeError(
            "google-auth-oauthlib and google-auth are required for OAuth authentication"
        ) from exc

    assert settings.google_oauth_client_secret_file is not None
    token_path = Path(settings.google_oauth_token_file)
    token_path.parent.mkdir(parents=True, exist_ok=True)

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(
            str(token_path), scopes=list(settings.google_scopes)
        )

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                settings.google_oauth_client_secret_file, scopes=list(settings.google_scopes)
            )
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds

