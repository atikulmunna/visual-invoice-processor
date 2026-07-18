from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def load_aws_parameter_secrets(ssm_client: Any | None = None) -> None:
    parameter_names = {
        target: os.getenv(parameter_env, "").strip()
        for target, parameter_env in {
            "POSTGRES_DSN": "POSTGRES_PARAMETER_NAME",
            "MISTRAL_API_KEY": "MISTRAL_PARAMETER_NAME",
        }.items()
        if not os.getenv(target)
    }
    parameter_names = {target: name for target, name in parameter_names.items() if name}
    if not parameter_names:
        return

    if ssm_client is None:
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - included in the Lambda image
            raise RuntimeError("boto3 is required to load AWS parameter secrets") from exc
        ssm_client = boto3.client("ssm", region_name=os.getenv("AWS_REGION", "ap-southeast-1"))

    response = ssm_client.get_parameters(
        Names=sorted(set(parameter_names.values())),
        WithDecryption=True,
    )
    values = {
        item["Name"]: item["Value"]
        for item in response.get("Parameters", [])
        if item.get("Name") and item.get("Value")
    }
    missing = sorted(name for name in parameter_names.values() if name not in values)
    if missing:
        raise RuntimeError(f"AWS parameter secret(s) unavailable: {', '.join(missing)}")
    for target, name in parameter_names.items():
        os.environ[target] = values[name]


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
    ingestion_backend: str = "drive"
    drive_inbox_folder_id: str | None = None
    google_auth_mode: str = "service_account"
    google_service_account_file: str | None = None
    google_oauth_client_secret_file: str | None = None
    google_oauth_token_file: str = ".tokens/google_token.json"
    s3_bucket_name: str | None = None
    s3_region: str = "ap-southeast-1"
    s3_inbox_prefix: str = "inbox/"
    s3_archive_prefix: str = "archive/"
    allowed_mime_types: tuple[str, ...] = (
        "image/jpeg",
        "image/png",
        "application/pdf",
    )
    log_level: str = "INFO"
    ledger_backend: str = "sheets"
    ledger_spreadsheet_id: str | None = None
    ledger_range: str = "Ledger!A:Z"
    postgres_dsn: str | None = None
    postgres_table: str = "ledger_records"
    review_queue_backend: str = "auto"
    review_queue_table: str = "review_queue_items"
    normalization_rules_path: str = "config/normalization_rules.json"
    alpha_auth_enabled: bool = False
    alpha_max_users: int = 10
    alpha_document_limit: int = 20
    alpha_global_page_limit: int = 1000
    max_upload_bytes: int = 5 * 1024 * 1024
    max_pdf_pages: int = 5

    @property
    def google_scopes(self) -> tuple[str, ...]:
        return (
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        )

    @classmethod
    def from_env(cls) -> "Settings":
        ingestion_backend = os.getenv("INGESTION_BACKEND", "drive").strip().lower()
        if ingestion_backend not in {"drive", "s3"}:
            raise ValueError("INGESTION_BACKEND must be one of: drive, s3")

        auth_mode = os.getenv("GOOGLE_AUTH_MODE", "service_account").strip().lower()
        if auth_mode not in {"service_account", "oauth"}:
            raise ValueError("GOOGLE_AUTH_MODE must be one of: service_account, oauth")

        service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
        oauth_secret_file = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET_FILE")
        oauth_token_file = os.getenv("GOOGLE_OAUTH_TOKEN_FILE", ".tokens/google_token.json")

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

        if ingestion_backend == "drive" or ledger_backend == "sheets":
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

        drive_inbox_folder_id = os.getenv("DRIVE_INBOX_FOLDER_ID")
        if ingestion_backend == "drive" and (not drive_inbox_folder_id or not drive_inbox_folder_id.strip()):
            raise ValueError("DRIVE_INBOX_FOLDER_ID is required when INGESTION_BACKEND=drive")

        s3_bucket_name = os.getenv("S3_BUCKET_NAME")
        if ingestion_backend == "s3" and (not s3_bucket_name or not s3_bucket_name.strip()):
            raise ValueError("S3_BUCKET_NAME is required when INGESTION_BACKEND=s3")

        postgres_dsn = os.getenv("POSTGRES_DSN")
        if ledger_backend == "postgres" and (not postgres_dsn or not postgres_dsn.strip()):
            raise ValueError("POSTGRES_DSN is required when LEDGER_BACKEND=postgres")

        return cls(
            ingestion_backend=ingestion_backend,
            drive_inbox_folder_id=drive_inbox_folder_id,
            google_auth_mode=auth_mode,
            google_service_account_file=service_account_file,
            google_oauth_client_secret_file=oauth_secret_file,
            google_oauth_token_file=oauth_token_file,
            s3_bucket_name=s3_bucket_name,
            s3_region=os.getenv("S3_REGION", os.getenv("AWS_REGION", "ap-southeast-1")),
            s3_inbox_prefix=os.getenv("S3_INBOX_PREFIX", "inbox/"),
            s3_archive_prefix=os.getenv("S3_ARCHIVE_PREFIX", "archive/"),
            allowed_mime_types=allowed_mimes,
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            ledger_backend=ledger_backend,
            ledger_spreadsheet_id=os.getenv("LEDGER_SPREADSHEET_ID"),
            ledger_range=os.getenv("LEDGER_RANGE", "Ledger!A:Z"),
            postgres_dsn=postgres_dsn,
            postgres_table=os.getenv("POSTGRES_TABLE", "ledger_records"),
            review_queue_backend=os.getenv("REVIEW_QUEUE_BACKEND", "auto").strip().lower(),
            review_queue_table=os.getenv("REVIEW_QUEUE_TABLE", "review_queue_items"),
            normalization_rules_path=os.getenv(
                "NORMALIZATION_RULES_PATH", "config/normalization_rules.json"
            ),
            alpha_auth_enabled=_parse_bool(os.getenv("ALPHA_AUTH_ENABLED"), False),
            alpha_max_users=int(os.getenv("ALPHA_MAX_USERS", "10")),
            alpha_document_limit=int(os.getenv("ALPHA_DOCUMENT_LIMIT", "20")),
            alpha_global_page_limit=int(os.getenv("ALPHA_GLOBAL_PAGE_LIMIT", "1000")),
            max_upload_bytes=int(os.getenv("MAX_UPLOAD_BYTES", str(5 * 1024 * 1024))),
            max_pdf_pages=int(os.getenv("MAX_PDF_PAGES", "5")),
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
