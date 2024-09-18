from typing import Optional
from sqlmodel import Session, select
from Users.authentication import verify_password
from Users.modules import Users


def get_user_by_username(session: Session, username: str) -> Optional[Users]:
    user = session.exec(select(Users).where(Users.name == username)).first()
    return user

def authenticate_user(session: Session, username: str, password: str) -> Optional[Users]:
    user = get_user_by_username(session, username=username)
    if not user:
        return None
    if not verify_password(password, user.password):
        return None
    return user