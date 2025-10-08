from dataclasses import dataclass, field


@dataclass
class SGXAnnouncement:
    url: str
    symbol: str = ''
    purchase_date: str = ''
    type: str = ''
    price_per_share: dict[str, float] = field(default_factory=dict)
    total_value: float = 0.0
    total_shares_purchased: float = 0.0
    cumulative_purchased: float = 0.0
    treasury_shares_after_purchase: float = 0.0