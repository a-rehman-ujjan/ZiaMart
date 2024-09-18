from passlib.context import CryptContext
from datetime import datetime, timedelta
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from Inventory import settings

SECRET_KEY = str(settings.SECRET_KEY)
ALGORITHM = str(settings.ALGORITHM)
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="http://localhost:8000/token")

def decode_token(token, SK, ALG):
    try:
        payload = jwt.decode(token, SK, algorithms=[ALG])
        return payload
    except JWTError:
        return None

def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    payload = decode_token(token, SECRET_KEY, ALGORITHM)
    if payload is None:
        raise credentials_exception
    return payload
