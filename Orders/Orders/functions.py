from sqlmodel import Session
from Orders.modules import Orders, Products

async def Get_Product_Quantity(id: int, session: Session):
    product = session.get(Products, id)
    if product:
        return product

def get_order(session: Session, order_id: int):
    return session.get(Orders, order_id)