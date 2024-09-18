from sqlmodel import Session
from Products.modules import Products


def get_product(session: Session, product_id: int):
    return session.get(Products, product_id)
