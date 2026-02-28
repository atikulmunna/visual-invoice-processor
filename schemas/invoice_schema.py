from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field


class LineItem(BaseModel):
    description: str = Field(min_length=1)
    quantity: float = Field(gt=0)
    unit_price: float = Field(ge=0)
    line_total: float = Field(ge=0)
    category: str | None = None


class InvoiceRecord(BaseModel):
    document_type: Literal["invoice", "receipt"]
    vendor_name: str = Field(min_length=1)
    vendor_tax_id: str | None = None
    invoice_number: str | None = None
    invoice_date: date
    due_date: date | None = None
    currency: str = Field(min_length=3, max_length=3)
    subtotal: float = Field(ge=0)
    tax_amount: float = Field(ge=0)
    total_amount: float = Field(ge=0)
    payment_method: Literal["card", "cash", "bank", "unknown"] = "unknown"
    line_items: list[LineItem] = Field(default_factory=list)
    model_confidence: float = Field(ge=0, le=1)
    validation_score: float = Field(ge=0, le=1)

