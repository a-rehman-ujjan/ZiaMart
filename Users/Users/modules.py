from typing import Optional
from sqlmodel import Field, SQLModel

class User (SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    email: str = Field()
    password:str = Field()

class Usertoken (SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    email: str = Field()
    user_type: int = Field()

class Users (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    email: str = Field()
    password:str = Field()
    user_type: int = Field()

class Update_User (SQLModel):
    name: str = Field()
    email: str = Field()

class Change_Password (SQLModel):
    password: str = Field()

class TokenResponse(SQLModel):
    access_token: str
    token_type: str