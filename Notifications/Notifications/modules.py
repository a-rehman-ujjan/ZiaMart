from typing import Optional
from pydantic import BaseModel
from sqlmodel import Field, SQLModel


class Notification(BaseModel):
    message: str

class Orders (SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    user_id: int = Field()
    user_name: str = Field()
    product_id: int = Field()
    product_name: str = Field()
    quantity: int = Field()
    total_price: float = Field()