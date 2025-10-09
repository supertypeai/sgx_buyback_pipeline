from dataclasses import dataclass, field


@dataclass
class SGXFilings:
    url: str
    symbol: str = ''
    transaction_date: str = ''
    number_of_stock: str = ''
    total_value: str = ''
    # price_per_share: str = ''
    shares_before: str = ''
    shares_before_percentage: str = ''
    shares_after: str = ''
    shares_after_percentage: str = ''
    