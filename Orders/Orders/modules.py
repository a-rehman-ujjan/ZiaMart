from sqlmodel import SQLModel, Field
from typing import Optional
from sqlmodel import SQLModel


class Orders (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    user_id: int = Field()
    user_name: str = Field()
    product_id: int = Field()
    product_name: str = Field()
    quantity: int = Field()
    total_price: float = Field()

class Order (SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    product_id: int = Field()
    quantity: int = Field()

class Products (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price: int = Field()
    quantity: int = Field()
    added_by:str = Field()

class Usertoken (SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    email: str = Field()
    user_type: int = Field()