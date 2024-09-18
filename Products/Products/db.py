from sqlalchemy import create_engine
from sqlmodel import SQLModel, Session
from Products import settings


connection_string=str(settings.DATABASE_URL)
engine = create_engine(connection_string, echo=True, pool_pre_ping=True)

def create_tables():
    SQLModel.metadata.create_all(engine)
    
def get_session():
    with Session(engine) as session:
        yield session