from starlette.config import Config
from starlette.datastructures import Secret

try:
    config = Config(".env")

except FileNotFoundError:
    config = Config()

BOOTSTRAP_SERVER = config("KAFKA_SERVER", cast=str)
KAFKA_ORDER_TOPIC = config("KAFKA_ORDER_TOPIC", cast=str)
KAFKA_CONSUMER_GROUP_ID = config("KAFKA_CONSUMER_GROUP_ID", cast=str)
DATABASE_URL=config("DATABASE_URL", cast= Secret)
TEST_DATABASE_URL=config("TEST_DATABASE_URL",  cast= Secret)
ADMIN_PASSWORD=config("ADMIN_PASSWORD", cast= Secret)
SECRET_KEY = config("SECRET_KEY", cast=Secret)
ALGORITHM = config("ALGORITHM", cast=str)