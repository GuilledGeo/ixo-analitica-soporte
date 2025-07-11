import pandas as pd
from shapely import wkb
from shapely.errors import WKBReadingError

# === Función auxiliar para extraer lat/lon de WKB ===
def extraer_coords(geom_str):
    try:
        geom = wkb.loads(bytes.fromhex(geom_str))
        return pd.Series([geom.x, geom.y])
    except (WKBReadingError, TypeError, ValueError):
        return pd.Series([None, None])

# === Función principal de enriquecimiento ===
def aplicar_clasificaciones_temporales(df):
    ahora = pd.Timestamp.now()

    # Forzar NaNs a 0 en campos críticos
    df["mensajes_recibidos"] = df["mensajes_recibidos"].fillna(0)
    df["mensajes_sin_gps"] = df["mensajes_sin_gps"].fillna(0)
    df["mensajes_esperados"] = df["mensajes_esperados"].fillna(0)

    # Calcular métricas derivadas
    if "pct_recibidos_vs_esperados" not in df.columns:
        df["pct_recibidos_vs_esperados"] = (df["mensajes_recibidos"] / df["mensajes_esperados"]).round(2)

    if "pct_sin_gps_vs_esperados" not in df.columns:
        df["pct_sin_gps_vs_esperados"] = (df["mensajes_sin_gps"] / df["mensajes_esperados"]).round(2)

    if "pct_sin_gps_recibidos" not in df.columns:
        df["pct_sin_gps_recibidos"] = df.apply(
            lambda row: round(row["mensajes_sin_gps"] / row["mensajes_recibidos"], 3)
            if row["mensajes_recibidos"] > 0 else None,
            axis=1
        )

    # Convertir columnas datetime a naive
    df["ultimo_mensaje_recibido"] = pd.to_datetime(df["ultimo_mensaje_recibido"], errors='coerce').dt.tz_localize(None)
    df["ultima_posicion_gps_valida"] = pd.to_datetime(df["ultima_posicion_gps_valida"], errors='coerce').dt.tz_localize(None)
    df["visto_ultima_vez"] = pd.to_datetime(df["visto_ultima_vez"], errors='coerce').dt.tz_localize(None)

    # Calcular tiempos
    df["tiempo_desde_ultimo_mensaje"] = (ahora - df["ultimo_mensaje_recibido"]).dt.total_seconds() / 3600
    df["tiempo_desde_ultima_posicion_gps"] = (ahora - df["ultima_posicion_gps_valida"]).dt.total_seconds() / 3600
    df["tiempo_inactivo"] = (ahora - df["visto_ultima_vez"]).dt.total_seconds() / 3600

    # Clasificación GPS
    def clasif_gps(td_horas):
        if pd.isna(td_horas):
            return "Sin posición GPS válida"
        elif td_horas <= 24:
            return "GPS activo hoy"
        elif td_horas <= 48:
            return "GPS 24-48h"
        elif td_horas <= 72:
            return "GPS 48-72h"
        elif td_horas <= 168:
            return "GPS 3-7 días"
        elif td_horas <= 360:
            return "GPS 7-15 días"
        elif td_horas <= 720:
            return "GPS 15 días - 1 mes"
        elif td_horas <= 2160:
            return "GPS 1-3 meses"
        else:
            return "GPS >3 meses"

    df["clasificacion_gps"] = df["tiempo_desde_ultima_posicion_gps"].apply(clasif_gps)

    # Clasificación conexión
    def clasif_conexion(tiempo_inactivo, mensajes_recibidos):
        if mensajes_recibidos > 0:
            return "Conectado hoy"
        elif tiempo_inactivo <= 48:
            return "Conexión 24-48h"
        elif tiempo_inactivo <= 72:
            return "Conexión 48-72h"
        elif tiempo_inactivo <= 168:
            return "Conexión 3-7 días"
        elif tiempo_inactivo <= 360:
            return "Conexión 7-15 días"
        elif tiempo_inactivo <= 720:
            return "Conexión 15 días - 1 mes"
        elif tiempo_inactivo <= 2160:
            return "Conexión 1-3 meses"
        else:
            return "Conexión >3 meses"

    df["clasificacion_conexion"] = df.apply(
        lambda row: clasif_conexion(row["tiempo_inactivo"], row["mensajes_recibidos"]),
        axis=1
    )

    # Extraer lat/lon si existe geometría en formato WKB
    if "ultima_posicion_geom" in df.columns and "lon" not in df.columns and "lat" not in df.columns:
        df[["lon", "lat"]] = df["ultima_posicion_geom"].apply(extraer_coords)

    return df
