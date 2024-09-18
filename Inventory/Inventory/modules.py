from typing import Optional
from sqlmodel import Field, SQLModel


class Products (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price: int = Field()
    quantity: int = Field()
    added_by:str = Field()

class Inventory_Item (SQLModel):
    product_id: int = Field()
    quantity: int = Field()

class Usertoken (SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    email: str = Field()
    user_type: int = Field()