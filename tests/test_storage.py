from __future__ import annotations

from typing import Any

from app.storage_service import SheetsStorageService


class _FakeAppendAPI:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def append(self, **kwargs: Any) -> "_FakeAppendAPI":
        self.calls.append(kwargs)
        return self

    def execute(self) -> dict[str, Any]:
        row_number = len(self.calls) + 5
        return {
            "updates": {
                "updatedRange": f"Ledger!A{row_number}:O{row_number}",
                "updatedRows": 1,
            }
        }


class _FakeValuesAPI:
    def __init__(self, append_api: _FakeAppendAPI) -> None:
        self._append_api = append_api

    def append(self, **kwargs: Any) -> _FakeAppendAPI:
        return self._append_api.append(**kwargs)


class _FakeSpreadsheetsAPI:
    def __init__(self, values_api: _FakeValuesAPI) -> None:
        self._values_api = values_api

    def values(self) -> _FakeValuesAPI:
        return self._values_api


class _FakeSheetsClient:
    def __init__(self) -> None:
        self.append_api = _FakeAppendAPI()
        self.values_api = _FakeValuesAPI(self.append_api)
        self.spreadsheets_api = _FakeSpreadsheetsAPI(self.values_api)

    def spreadsheets(self) -> _FakeSpreadsheetsAPI:
        return self.spreadsheets_api


def _record() -> dict[str, Any]:
    return {
        "document_type": "invoice",
        "vendor_name": "Acme Supplies",
        "invoice_number": "INV-100",
        "invoice_date": "2026-02-27",
        "currency": "USD",
        "subtotal": 50.0,
        "tax_amount": 5.0,
        "total_amount": 55.0,
        "model_confidence": 0.9,
        "validation_score": 0.91,
    }


def _metadata(file_hash: str = "abc123") -> dict[str, Any]:
    return {
        "document_id": "doc-1",
        "drive_file_id": "file-1",
        "file_hash": file_hash,
        "status": "STORED",
    }


def test_append_record_returns_row_reference() -> None:
    fake = _FakeSheetsClient()
    service = SheetsStorageService(fake, spreadsheet_id="sheet-id")
    result = service.append_record(_record(), _metadata())

    assert result["status"] == "appended"
    assert result["updated_rows"] == 1
    assert isinstance(result["row_index"], int)
    assert fake.append_api.calls


def test_append_record_skips_duplicate_in_same_process() -> None:
    fake = _FakeSheetsClient()
    service = SheetsStorageService(fake, spreadsheet_id="sheet-id")

    first = service.append_record(_record(), _metadata(file_hash="same-hash"))
    second = service.append_record(_record(), _metadata(file_hash="same-hash"))

    assert first["status"] == "appended"
    assert second["status"] == "skipped_duplicate"
    assert len(fake.append_api.calls) == 1

