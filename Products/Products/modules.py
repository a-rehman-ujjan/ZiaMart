from typing import Optional
from sqlmodel import Field, SQLModel


class Products (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price: int = Field()
    quantity: int = Field()
    added_by:str = Field()

class Add_Product (SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price: int = Field()

class Update_Product (SQLModel):
    name: str = Field()
    price: int = Field()
    quantity: int = Field()

class token_data(SQLModel):
    username: str

class Usertoken (SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    email: str = Field()
    user_type: int = Field()