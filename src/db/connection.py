import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Configuración de la base de datos (funcional)
DB_CONFIG = {
    "host": "88.99.66.93",
    "port": 31702,
    "dbname": "ixorigue",
    "user": "ixorigue_reader",
    "password": "4MDY7vqopVHjIcOk01ulJP75lBt9MsFEkRJiHq1DCSqsal9rQm",
    "sslmode": "require" 
}
#91.99.186.170
def conectar_db():
    try:
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["dbname"],
            query={"sslmode": DB_CONFIG["sslmode"]}
        )
        engine = create_engine(url)
        return engine
    except Exception as e:
        raise RuntimeError(f"❌ Error al conectar con la base de datos: {e}")
