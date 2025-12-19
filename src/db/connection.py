from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Configuración de la base de datos (funcional)
DB_CONFIG = {
    "host": "10.0.1.6",
    "port": 31702,
    "dbname": "ixorigue",
    "user": "ixorigue_reader",
    "password": "4MDY7vqopVHjIcOk01ulJP75lBt9MsFEkRJiHq1DCSqsal9rQm",
    "sslmode": "require" 
}

def conectar_db():
    """
    Crea conexión optimizada para réplica de lectura PostgreSQL
    """
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
        
        engine = create_engine(
            url,
            connect_args={
                "connect_timeout": 10,
                # Configuración para réplicas de lectura
                "options": "-c default_transaction_read_only=on -c statement_timeout=600000"
            },
            pool_pre_ping=True,  # Verifica conexiones antes de usarlas
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,  # Recicla conexiones cada hora
            isolation_level="REPEATABLE READ"  # Nivel de aislamiento más estable
        )
        
        return engine
    except Exception as e:
        raise RuntimeError(f"❌ Error al conectar con la base de datos: {e}")