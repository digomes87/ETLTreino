from pydantic import BaseModel, Field
from datetime import datetime
from typing import Literal


class Transaction(BaseModel):
    id: str = Field(..., min_length=1)
    product: Literal["credit_card", "personal_loan", "mortgage", "auto_loan"]
    amount: float = Field(..., gt=0)
    currency: Literal["USD", "EUR", "BRL", "GBP"]
    timestamp: datetime
    status: Literal["posted", "pending", "reversed"]
