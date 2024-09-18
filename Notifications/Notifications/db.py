from Notifications import settings
from sqlmodel import SQLModel, create_engine, Session


connection_string=str(settings.DATABASE_URL)
engine = create_engine(connection_string, echo=True, pool_pre_ping=True)

# def create_tables():
#     SQLModel.metadata.create_all(engine)

# Dependency to get the session
def get_session():
    with Session(engine) as session:
        yield session