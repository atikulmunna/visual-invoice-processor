from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from app.auth import get_google_credentials
from app.config import Settings, load_dotenv


class StorageError(RuntimeError):
    pass


_ROW_RANGE_RE = re.compile(r".*![A-Z]+(?P<row>\d+):[A-Z]+(?P=row)$")


def _extract_row_index(updated_range: str) -> int | None:
    match = _ROW_RANGE_RE.match(updated_range)
    if not match:
        return None
    return int(match.group("row"))


def _to_row(record: dict[str, Any], metadata: dict[str, Any]) -> list[Any]:
    now = datetime.now(timezone.utc).isoformat()
    return [
        metadata.get("document_id"),
        metadata.get("drive_file_id"),
        metadata.get("file_hash"),
        record.get("document_type"),
        record.get("vendor_name"),
        record.get("invoice_number"),
        record.get("invoice_date"),
        record.get("currency"),
        record.get("subtotal"),
        record.get("tax_amount"),
        record.get("total_amount"),
        record.get("model_confidence"),
        record.get("validation_score"),
        metadata.get("status", "STORED"),
        metadata.get("processed_at_utc", now),
    ]


class SheetsStorageService:
    """Google Sheets writer with in-process dedupe for MVP safety.

    Note: duplicate safety is process-local and not durable. P2-02 introduces
    durable idempotency via metadata storage and unique constraints.
    """

    def __init__(
        self,
        sheets_client: Any,
        spreadsheet_id: str,
        value_range: str = "Ledger!A:Z",
    ) -> None:
        self._sheets = sheets_client
        self._spreadsheet_id = spreadsheet_id
        self._range = value_range
        self._seen_dedupe_keys: set[str] = set()

    @classmethod
    def from_credentials(
        cls,
        credentials: Any,
        spreadsheet_id: str,
        value_range: str = "Ledger!A:Z",
    ) -> "SheetsStorageService":
        try:
            from googleapiclient.discovery import build
        except ImportError as exc:
            raise RuntimeError(
                "google-api-python-client is required for Sheets API access"
            ) from exc
        sheets = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        return cls(sheets_client=sheets, spreadsheet_id=spreadsheet_id, value_range=value_range)

    def append_record(self, record: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
        dedupe_key = metadata.get("file_hash") or metadata.get("idempotency_key")
        if isinstance(dedupe_key, str) and dedupe_key in self._seen_dedupe_keys:
            return {
                "status": "skipped_duplicate",
                "dedupe_key": dedupe_key,
                "spreadsheet_id": self._spreadsheet_id,
            }

        response = (
            self._sheets.spreadsheets()
            .values()
            .append(
                spreadsheetId=self._spreadsheet_id,
                range=self._range,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [_to_row(record, metadata)]},
            )
            .execute()
        )

        updates = response.get("updates", {})
        updated_range = updates.get("updatedRange", "")
        result = {
            "status": "appended",
            "spreadsheet_id": self._spreadsheet_id,
            "updated_range": updated_range,
            "updated_rows": updates.get("updatedRows", 0),
            "row_index": _extract_row_index(updated_range),
        }
        if isinstance(dedupe_key, str):
            self._seen_dedupe_keys.add(dedupe_key)
            result["dedupe_key"] = dedupe_key
        return result

def append_record(record: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    load_dotenv()
    settings = Settings.from_env()
    if not settings.ledger_spreadsheet_id:
        raise StorageError("LEDGER_SPREADSHEET_ID is required for Sheets storage.")

    credentials = get_google_credentials(settings)
    service = SheetsStorageService.from_credentials(
        credentials=credentials,
        spreadsheet_id=settings.ledger_spreadsheet_id,
        value_range=settings.ledger_range,
    )
    return service.append_record(record=record, metadata=metadata)
