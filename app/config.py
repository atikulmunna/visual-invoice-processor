from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _require(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


@dataclass(frozen=True)
class Settings:
    drive_inbox_folder_id: str
    google_auth_mode: str
    google_service_account_file: str | None
    google_oauth_client_secret_file: str | None
    google_oauth_token_file: str
    allowed_mime_types: tuple[str, ...]
    log_level: str
    ledger_backend: str = "sheets"
    ledger_spreadsheet_id: str | None = None
    ledger_range: str = "Ledger!A:Z"
    postgres_dsn: str | None = None
    postgres_table: str = "ledger_records"

    @property
    def google_scopes(self) -> tuple[str, ...]:
        return (
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        )

    @classmethod
    def from_env(cls) -> "Settings":
        auth_mode = os.getenv("GOOGLE_AUTH_MODE", "service_account").strip().lower()
        if auth_mode not in {"service_account", "oauth"}:
            raise ValueError(
                "GOOGLE_AUTH_MODE must be one of: service_account, oauth"
            )

        service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
        oauth_secret_file = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET_FILE")
        oauth_token_file = os.getenv("GOOGLE_OAUTH_TOKEN_FILE", ".tokens/google_token.json")

        if auth_mode == "service_account":
            if not service_account_file:
                raise ValueError(
                    "GOOGLE_SERVICE_ACCOUNT_FILE is required when GOOGLE_AUTH_MODE=service_account"
                )
            if not Path(service_account_file).exists():
                raise ValueError(
                    f"GOOGLE_SERVICE_ACCOUNT_FILE not found: {service_account_file}"
                )
        else:
            if not oauth_secret_file:
                raise ValueError(
                    "GOOGLE_OAUTH_CLIENT_SECRET_FILE is required when GOOGLE_AUTH_MODE=oauth"
                )
            if not Path(oauth_secret_file).exists():
                raise ValueError(
                    f"GOOGLE_OAUTH_CLIENT_SECRET_FILE not found: {oauth_secret_file}"
                )

        mime_env = os.getenv(
            "ALLOWED_MIME_TYPES",
            "image/jpeg,image/png,application/pdf",
        )
        allowed_mimes = tuple(v.strip() for v in mime_env.split(",") if v.strip())
        if not allowed_mimes:
            raise ValueError("ALLOWED_MIME_TYPES must contain at least one mime type")

        ledger_backend = os.getenv("LEDGER_BACKEND", "sheets").strip().lower()
        if ledger_backend not in {"sheets", "postgres"}:
            raise ValueError("LEDGER_BACKEND must be one of: sheets, postgres")

        return cls(
            drive_inbox_folder_id=_require("DRIVE_INBOX_FOLDER_ID"),
            google_auth_mode=auth_mode,
            google_service_account_file=service_account_file,
            google_oauth_client_secret_file=oauth_secret_file,
            google_oauth_token_file=oauth_token_file,
            allowed_mime_types=allowed_mimes,
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            ledger_backend=ledger_backend,
            ledger_spreadsheet_id=os.getenv("LEDGER_SPREADSHEET_ID"),
            ledger_range=os.getenv("LEDGER_RANGE", "Ledger!A:Z"),
            postgres_dsn=os.getenv("POSTGRES_DSN"),
            postgres_table=os.getenv("POSTGRES_TABLE", "ledger_records"),
        )


def load_dotenv(path: str | Path = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        entry = line.strip()
        if not entry or entry.startswith("#") or "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)
