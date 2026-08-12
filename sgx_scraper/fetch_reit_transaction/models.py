from dataclasses import dataclass


@dataclass
class ReitPropertyTransaction:
    symbol: str
    property_name: str
    transaction_type: str
    financial_year: int | None = None
    status: str = "completed"
    counterparty: str | None = None
    interest_pct: float | None = None
    completed_date: str | None = None
    deal_id: str | None = None
    basis_value: int | None = None
    basis: str | None = None
    transaction_price: int | None = None
    source_url: str | None = None
    ref_id: str | None = None
