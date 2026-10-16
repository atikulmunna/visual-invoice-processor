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
            "OPENROUTER_API_KEY": "OPENROUTER_PARAMETER_NAME",
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
    """Uploads arrive in S3 and everything else lives in Postgres."""

    s3_bucket_name: str | None = None
    s3_region: str = "ap-southeast-1"
    s3_inbox_prefix: str = "inbox/"
    s3_archive_prefix: str = "archive/"
    allowed_mime_types: tuple[str, ...] = ("image/jpeg", "image/png", "application/pdf")
    postgres_dsn: str | None = None
    normalization_rules_path: str = "config/normalization_rules.json"
    alpha_auth_enabled: bool = False
    alpha_max_users: int = 10
    alpha_document_limit: int = 20
    alpha_global_page_limit: int = 1000
    max_upload_bytes: int = 5 * 1024 * 1024
    max_pdf_pages: int = 5

    @classmethod
    def from_env(cls) -> "Settings":
        mime_env = os.getenv("ALLOWED_MIME_TYPES", "image/jpeg,image/png,application/pdf")
        allowed_mimes = tuple(v.strip() for v in mime_env.split(",") if v.strip())
        if not allowed_mimes:
            raise ValueError("ALLOWED_MIME_TYPES must contain at least one mime type")
        return cls(
            s3_bucket_name=_require("S3_BUCKET_NAME"),
            s3_region=os.getenv("S3_REGION", os.getenv("AWS_REGION", "ap-southeast-1")),
            s3_inbox_prefix=os.getenv("S3_INBOX_PREFIX", "inbox/"),
            s3_archive_prefix=os.getenv("S3_ARCHIVE_PREFIX", "archive/"),
            allowed_mime_types=allowed_mimes,
            postgres_dsn=_require("POSTGRES_DSN"),
            normalization_rules_path=os.getenv("NORMALIZATION_RULES_PATH", "config/normalization_rules.json"),
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
