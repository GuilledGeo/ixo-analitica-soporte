from sqlalchemy import create_engine
from sqlalchemy.engine import URL

DB_CONFIG = {
    "host": "10.0.1.6",
    "port": 31702,
    "dbname": "ixorigue",
    "user": "ixorigue_reader",
    "password": "4MDY7vqopVHjIcOk01ulJP75lBt9MsFEkRJiHq1DCSqsal9rQm",
    "sslmode": "require"
}
#88.99.66.93 host publico funcional api
#10.0.1.6 host privado no funcional api (de momento)

def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["dbname"],
        query={"sslmode": DB_CONFIG["sslmode"]}
    )
    return create_engine(url)
