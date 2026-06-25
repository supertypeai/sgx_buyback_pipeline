from dataclasses import dataclass, field


@dataclass
class SGXFilings:
    url: str
    symbol: str = ''
    time: str = ''
    tags: list[str] = field(default_factory=list)

    transaction_date: str = ''
    shareholder_name: str = ''
    number_of_stock: float = 0.0
    value: float = 0.0
    price_per_share: float = 0.0
    transaction_type: str = ''
    shares_before: float = 0.0
    shares_before_percentage: float = 0.0
    shares_after: float = 0.0
    shares_after_percentage: float = 0.0

    title: str = ''
    body: str = ''
    sector: str = ''
    sub_sector: str = ''
    holder_type: str = ''
    issuer_name: str = ''
    circumstances_desc: str = ''
    circumstances_raw: dict = field(default_factory=dict)

    source_is_manual: bool = False