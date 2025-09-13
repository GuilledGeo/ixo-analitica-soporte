# db_connection.py
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

DB_CONFIG = {
    "host": "10.0.1.6",
    "port": 31702,
    "dbname": "ixorigue",
    "user": "ixorigue_reader",
    "password": "4MDY7vqopVHjIcOk01ulJP75lBt9MsFEkRJiHq1DCSqsal9rQm",
    "sslmode": "require",
}

def get_engine():
    """
    Engine robusto para lecturas largas:
      - pool_pre_ping: detecta/rehace conexiones muertas
      - keepalives TCP: evita cierres por inactividad
      - pool_recycle: recicla conexiones cada 30 min
      - statement_timeout: corta SELECTs atascados (10 min por defecto)
      - default_transaction_read_only=on: deja claro que son lecturas
    Ajusta con variables de entorno si lo necesitas.
    """
    url = URL.create(
        "postgresql+psycopg2",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["dbname"],
    )

    statement_timeout_ms = int(os.getenv("PG_STATEMENT_TIMEOUT_MS", "600000"))  # 10 min
    idle_tx_timeout_ms = int(os.getenv("PG_IDLE_TX_TIMEOUT_MS", "30000"))       # 30 s
    pool_size = int(os.getenv("PG_POOL_SIZE", "5"))
    max_overflow = int(os.getenv("PG_MAX_OVERFLOW", "5"))
    pool_recycle = int(os.getenv("PG_POOL_RECYCLE", "1800"))  # 30 min
    pool_timeout = int(os.getenv("PG_POOL_TIMEOUT", "30"))

    connect_args = {
        "sslmode": DB_CONFIG.get("sslmode", "require"),
        # TCP keepalives (Linux; en Windows algunos drivers ignoran estos flags)
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 3,
        # Opciones de sesión en el servidor
        "options": (
            f"-c statement_timeout={statement_timeout_ms} "
            f"-c idle_in_transaction_session_timeout={idle_tx_timeout_ms} "
            f"-c default_transaction_read_only=on "
            f"-c application_name=ixo-lowactivity"
        ),
    }

    return create_engine(
        url,
        pool_pre_ping=True,         # ping antes de usar la conexión
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_recycle=pool_recycle,  # recicla conexiones viejas
        pool_timeout=pool_timeout,  # espera al pool antes de fallar
        connect_args=connect_args,
        # Nota: no ponemos AUTOCOMMIT; tus SELECTs ya se ejecutan rápido por segmento
    )
