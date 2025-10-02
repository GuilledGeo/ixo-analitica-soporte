import pandas as pd
from shapely import wkb
from shapely.errors import WKBReadingError

# === Función auxiliar para extraer lat/lon de WKB ===
def extraer_coords(geom_str):
    try:
        geom = wkb.loads(bytes.fromhex(geom_str))
        # Devolvemos en orden lon, lat
        return pd.Series([geom.x, geom.y], index=["lon", "lat"])
    except (WKBReadingError, TypeError, ValueError, AttributeError):
        return pd.Series([None, None], index=["lon", "lat"])

# === Función principal de enriquecimiento ===
def aplicar_clasificaciones_temporales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquecimiento:
    - KPIs básicos de recepción (con guardas /0).
    - Fechas a UTC tz-aware.
    - Clasificación de conexión desde `ultimo_mensaje_recibido` (alineado con el SQL).
    - Clasificación GPS desde `ultima_posicion_gps_valida`.
    - Extracción lon/lat desde WKB si procede.
    """
    df = df.copy()

    # ======== Fechas -> UTC tz-aware ========
    dt_cols = ["ultimo_mensaje_recibido", "ultima_posicion_gps_valida", "visto_ultima_vez"]
    for col in dt_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    ahora_utc = pd.Timestamp.now(tz="UTC")

    # ======== Forzar NaNs a 0 en métricas de conteo ========
    for col in ["mensajes_recibidos", "mensajes_sin_gps", "mensajes_esperados"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ======== KPIs derivados (con guardas /0) ========
    if (
        "pct_recibidos_vs_esperados" not in df.columns
        and {"mensajes_recibidos", "mensajes_esperados"}.issubset(df.columns)
    ):
        den = df["mensajes_esperados"].replace(0, pd.NA)
        df["pct_recibidos_vs_esperados"] = (df["mensajes_recibidos"] / den).astype(float)

    if (
        "pct_sin_gps_vs_esperados" not in df.columns
        and {"mensajes_sin_gps", "mensajes_esperados"}.issubset(df.columns)
    ):
        den = df["mensajes_esperados"].replace(0, pd.NA)
        df["pct_sin_gps_vs_esperados"] = (df["mensajes_sin_gps"] / den).astype(float)

    if (
        "pct_sin_gps_recibidos" not in df.columns
        and {"mensajes_sin_gps", "mensajes_recibidos"}.issubset(df.columns)
    ):
        den = df["mensajes_recibidos"].replace(0, pd.NA)
        df["pct_sin_gps_recibidos"] = (df["mensajes_sin_gps"] / den).astype(float)

    # ======== Clasificación CONEXIÓN (desde último mensaje) ========
    #  <=1d  -> Conectado hoy
    #  <=2d  -> Conexión 24-48h
    #  <=3d  -> Conexión 48-72h
    #  <=7d  -> Conexión 3-7 días
    #  <=15d -> Conexión 7-15 días
    #  <=30d -> Conexión 15 días - 1 mes
    #  <=90d -> Conexión 1-3 meses
    #  >90d/NaT -> Conexión >3 meses
    def clasif_conexion_from_ts(ts: pd.Timestamp) -> str:
        if pd.isna(ts):
            return "Conexión >3 meses"
        delta = ahora_utc - ts
        if delta <= pd.Timedelta(days=1):
            return "Conectado hoy"
        if delta <= pd.Timedelta(days=2):
            return "Conexión 24-48h"
        if delta <= pd.Timedelta(days=3):
            return "Conexión 48-72h"
        if delta <= pd.Timedelta(days=7):
            return "Conexión 3-7 días"
        if delta <= pd.Timedelta(days=15):
            return "Conexión 7-15 días"
        if delta <= pd.Timedelta(days=30):
            return "Conexión 15 días - 1 mes"
        if delta <= pd.Timedelta(days=90):
            return "Conexión 1-3 meses"
        return "Conexión >3 meses"

    if "ultimo_mensaje_recibido" in df.columns:
        df["clasificacion_conexion"] = df["ultimo_mensaje_recibido"].apply(clasif_conexion_from_ts)
    else:
        df["clasificacion_conexion"] = "Conexión >3 meses"

    # ======== Clasificación GPS (desde última posición GPS válida) ========
    def clasif_gps_from_hours(horas):
        if pd.isna(horas):
            return "Sin posición GPS válida"
        if horas <= 24:
            return "GPS activo hoy"
        if horas <= 48:
            return "GPS 24-48h"
        if horas <= 72:
            return "GPS 48-72h"
        if horas <= 168:
            return "GPS 3-7 días"
        if horas <= 360:
            return "GPS 7-15 días"
        if horas <= 720:
            return "GPS 15 días - 1 mes"
        if horas <= 2160:
            return "GPS 1-3 meses"
        return "GPS >3 meses"

    if "ultima_posicion_gps_valida" in df.columns:
        horas_gps = (ahora_utc - df["ultima_posicion_gps_valida"]).dt.total_seconds() / 3600
        df["clasificacion_gps"] = pd.Series(horas_gps, index=df.index).apply(clasif_gps_from_hours)
    else:
        df["clasificacion_gps"] = "Sin posición GPS válida"

    # ======== Extraer lon/lat desde WKB si no existen ========
    if "ultima_posicion_geom" in df.columns and ("lon" not in df.columns or "lat" not in df.columns):
        df[["lon", "lat"]] = df["ultima_posicion_geom"].apply(extraer_coords)

    return df
