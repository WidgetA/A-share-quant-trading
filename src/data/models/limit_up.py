# === MODULE PURPOSE ===
# Defines the LimitUpStock data model for storing daily limit-up stock information.
# Tracks stocks hitting daily price limit (+10% main board, +20% ChiNext/STAR).

# === KEY CONCEPTS ===
# - Limit-up (涨停): Stock reaches maximum daily price increase limit
# - open_count: Number of times the limit was broken and re-established
# - Composite ID: trade_date + stock_code ensures uniqueness per day

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class LimitUpStock:
    """
    Represents a limit-up stock record for a trading day.

    Data Flow:
        iFinD API -> IFinDLimitUpSource -> LimitUpStock -> LimitUpDatabase.save()

    Fields:
        - id: Composite key (trade_date_stock_code)
        - trade_date: Trading date (YYYY-MM-DD)
        - stock_code: Stock code with exchange suffix (e.g., "000001.SZ")
        - stock_name: Stock name in Chinese
        - limit_up_price: The limit-up price
        - limit_up_time: First time hitting limit-up (HH:MM:SS)
        - open_count: Number of times limit was opened (0 = sealed all day)
        - last_limit_up_time: Last time hitting limit-up if reopened
        - turnover_rate: Turnover rate percentage
        - amount: Total trading amount in yuan
        - circulation_mv: Circulating market value in yuan
        - reason: Limit-up reason/concept (e.g., "AI概念", "新能源")
        - industry: Industry classification
        - created_at: When this record was created
        - updated_at: When this record was last updated
    """

    trade_date: str
    stock_code: str
    stock_name: str
    limit_up_price: float
    limit_up_time: str
    open_count: int = 0
    last_limit_up_time: str | None = None
    turnover_rate: float | None = None
    amount: float | None = None
    circulation_mv: float | None = None
    reason: str | None = None
    industry: str | None = None
    id: str = field(default="")
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        """Generate composite ID if not provided."""
        if not self.id:
            self.id = f"{self.trade_date}_{self.stock_code}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            "id": self.id,
            "trade_date": self.trade_date,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "limit_up_price": self.limit_up_price,
            "limit_up_time": self.limit_up_time,
            "open_count": self.open_count,
            "last_limit_up_time": self.last_limit_up_time,
            "turnover_rate": self.turnover_rate,
            "amount": self.amount,
            "circulation_mv": self.circulation_mv,
            "reason": self.reason,
            "industry": self.industry,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LimitUpStock":
        """Create LimitUpStock from dictionary (database row)."""
        return cls(
            id=data["id"],
            trade_date=data["trade_date"],
            stock_code=data["stock_code"],
            stock_name=data["stock_name"],
            limit_up_price=float(data["limit_up_price"]),
            limit_up_time=data["limit_up_time"],
            open_count=int(data.get("open_count", 0)),
            last_limit_up_time=data.get("last_limit_up_time"),
            turnover_rate=float(data["turnover_rate"]) if data.get("turnover_rate") else None,
            amount=float(data["amount"]) if data.get("amount") else None,
            circulation_mv=float(data["circulation_mv"]) if data.get("circulation_mv") else None,
            reason=data.get("reason"),
            industry=data.get("industry"),
            created_at=(
                datetime.fromisoformat(data["created_at"])
                if data.get("created_at")
                else datetime.now()
            ),
            updated_at=(
                datetime.fromisoformat(data["updated_at"])
                if data.get("updated_at")
                else datetime.now()
            ),
        )
