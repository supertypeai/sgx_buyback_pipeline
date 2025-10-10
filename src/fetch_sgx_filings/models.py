from dataclasses import dataclass


@dataclass
class SGXFilings:
    url: str
    symbol: str = ''
    transaction_date: str = ''
    number_of_stock: float = 0.0
    value: float = 0.0
    price_per_share: float = 0.0
    transaction_type: str = ''
    shares_before: float = 0.0
    shares_before_percentage: float = 0.0
    shares_after: float = 0.0
    shares_after_percentage: float = 0.0
    