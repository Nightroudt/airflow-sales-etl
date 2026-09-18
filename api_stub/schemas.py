from datetime import datetime

from pydantic import BaseModel


class ProductOut(BaseModel):
    product_id: int
    name: str
    category: str
    supplier: str
    unit_cost: float
    updated_at: datetime
