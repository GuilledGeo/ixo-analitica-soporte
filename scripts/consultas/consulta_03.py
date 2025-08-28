# -*- coding: utf-8 -*-
"""
consulta_03 – KPI de Disponibilidad GPS por Dispositivo (PROMEDIOS DIARIOS en rango fijo)
Salida alineada con CONSULTA 2 (mismos nombres de columnas).

- Rango fijo definido en este archivo (edita FECHA_INICIO / FECHA_FIN_INCLUSIVA).
- Estrategia CHUNKED por días -> evita 'conflict with recovery' en réplicas.
- Calcula promedios diarios y los expone con los mismos encabezados de CONSULTA 2.

Columnas devueltas (idénticas a la consulta 2 detallada):
  device_id, SerialNumber, Model, mensajes_esperados,
  mensajes_recibidos, mensajes_sin_gps, pct_recibidos_vs_esperados, pct_sin_gps_vs_esperados, pct_sin_gps_recibidos,
  ultimo_mensaje_recibido, ultima_posicion_gps_valida, ultima_posicion_geom, visto_ultima_vez,
  numero_reinicios, media_ttf, porcentaje_bateria, fecha_cambio_bateria, suma_total_uplinks,
  ranch_name, customer_name, animal_name, Country, Region,
  "Mensajes esperados (detallado)", "Mensajes recibidos (n)", "Mensajes recibidos (%)",
  "Mensaje con posición GPS (n)", "Mensaje con posición GPS (%)",
  "Posición GPS válida (n)", "Posición GPS válida (%)",
  "Baja precisión (n)", "Baja precisión (%)",
  "Posición GPS no válida (n)", "Posición GPS no válida (%)",
  "No válida por calidad GPS (n)", "No válida por calidad GPS (%)",
  "No válida por filtro velocidad (n)", "No válida por filtro velocidad (%)",
  "Posición válida vs esperadas (%)",
  "Dispositivo OK (>60% válidas vs esperadas)",
  "% dispositivos OK en ganadería",
  "Ganadería OK (>70% dispositivos OK)"
"""

import traceback
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

# =========================
#  RANGO FIJO (edita aquí)
# =========================
FECHA_INICIO = "2025-07-22"       # incluida, 00:00
FECHA_FIN_INCLUSIVA = "2025-08-21" # incluida, 23:59:59

# =========================
#  SQL: métricas de 1 día
# =========================
SQL_DIA = text("""
WITH active_devices AS (
  SELECT *
  FROM "Devices"
  WHERE "UplinksPerDay" IS NOT NULL
    AND "Disabled" = FALSE
    AND "StatusType" = 'shipped'
),
current_animals AS (
  SELECT "DeviceId", MAX("Name") AS animal_name
  FROM "Animals"
  WHERE "IsDeregistered" = FALSE
  GROUP BY "DeviceId"
),
base AS (
  SELECT d.*, ca.animal_name
  FROM active_devices d
  RIGHT JOIN current_animals ca ON ca."DeviceId" = d."Id"
  WHERE d."Id" IS NOT NULL
)
SELECT
  b."Id" AS device_id,
  b."UplinksPerDay" AS uplinks_per_day,

  COUNT(dl."Time")                                                     AS recibidos_n,
  COUNT(dl."Time") FILTER (WHERE NOT dl."HasLocation")                 AS sin_gps_n,
  COUNT(dl."Time") FILTER (WHERE dl."HasLocation")                     AS con_gps_n,
  COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND dl."IsValid")    AS validas_n,
  COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND dl."IsValid" AND dl."IsLowAccuracy") AS baja_precision_n,
  COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid") AS no_validas_n,
  COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid" AND dl."InvalidReason" = 'parameters') AS no_valida_calidad_gps_n,
  COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid" AND dl."InvalidReason" = 'distance')   AS no_valida_filtro_velocidad_n

FROM base b
LEFT JOIN "DeviceLocations" dl
  ON dl."DeviceId" = b."Id"
 AND dl."Time" >= :inicio
 AND dl."Time" <  :fin
GROUP BY b."Id", b."UplinksPerDay";
""")

# =========================
#  SQL: metadatos + últimos mensajes (como consulta 2)
# =========================
SQL_META = text("""
WITH active_devices AS (
  SELECT *
  FROM "Devices"
  WHERE "UplinksPerDay" IS NOT NULL
    AND "Disabled" = FALSE
    AND "StatusType" = 'shipped'
),
current_animals AS (
  SELECT "DeviceId", MAX("Name") AS animal_name
  FROM "Animals"
  WHERE "IsDeregistered" = FALSE
  GROUP BY "DeviceId"
),
base AS (
  SELECT d.*, ca.animal_name
  FROM active_devices d
  RIGHT JOIN current_animals ca ON ca."DeviceId" = d."Id"
  WHERE d."Id" IS NOT NULL
),
gps_stats_full AS (
  SELECT
    dl."DeviceId",
    MAX(dl."Time") AS ultimo_mensaje_recibido,
    (
      SELECT MAX(sub."Time")
      FROM "DeviceLocations" sub
      WHERE sub."DeviceId" = dl."DeviceId"
        AND sub."HasLocation" = TRUE
    ) AS ultima_posicion_gps_valida,
    (
      SELECT sub."Location"
      FROM "DeviceLocations" sub
      WHERE sub."DeviceId" = dl."DeviceId"
        AND sub."HasLocation" = TRUE
      ORDER BY sub."Time" DESC
      LIMIT 1
    ) AS ultima_posicion_geom
  FROM "DeviceLocations" dl
  GROUP BY dl."DeviceId"
)
SELECT
  b."Id"            AS device_id,
  b."SerialNumber"  AS "SerialNumber",
  b."Model"         AS "Model",
  b."UplinksPerDay" AS "UplinksPerDay",
  b."LastSeenOn"    AS "visto_ultima_vez",
  b."ResetsCount"   AS "numero_reinicios",
  b."AverageGpsTtf" AS "media_ttf",
  b."BatteryEstimation" AS "porcentaje_bateria",
  b."ChangedBatteryOn"  AS "fecha_cambio_bateria",
  b."SumUplinksCount"   AS "suma_total_uplinks",
  r."Name"          AS "ranch_name",
  c."Name"          AS "customer_name",
  b.animal_name     AS "animal_name",
  r."Country"       AS "Country",
  r."Region"        AS "Region",
  gf.ultimo_mensaje_recibido,
  gf.ultima_posicion_gps_valida,
  gf.ultima_posicion_geom
FROM base b
JOIN "Ranches" r  ON b."RanchId" = r."Id"
LEFT JOIN "Customers" c ON r."CustomerId" = c."Id"
LEFT JOIN gps_stats_full gf ON gf."DeviceId" = b."Id";
""")

def ejecutar(engine, set_timezone: str = "Europe/Madrid") -> pd.DataFrame:
    """
    Ejecuta consulta_03 en rango fijo (definido arriba) y devuelve un DataFrame
    con los mismos encabezados que CONSULTA 2.
    """
    start = datetime.fromisoformat(FECHA_INICIO)
    end   = datetime.fromisoformat(FECHA_FIN_INCLUSIVA)
    if end < start:
        raise ValueError("FECHA_FIN_INCLUSIVA no puede ser anterior a FECHA_INICIO")

    dias = (end - start).days + 1

    try:
        # --- 1) Extraer metadatos/últimos mensajes (1 única vez)
        with engine.connect() as con:
            if set_timezone:
                con.exec_driver_sql(f"SET TIME ZONE '{set_timezone}';")
            con.exec_driver_sql("SET LOCAL lock_timeout = '5s';")
            con.exec_driver_sql("SET LOCAL statement_timeout = '5min';")

            df_meta = pd.read_sql_query(SQL_META, con)

            # --- 2) Iterar por día para métricas (evita consultas largas en réplica)
            frames = []
            for i in range(dias):
                ini = (start + timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
                fin = ini + timedelta(days=1)
                df_day = pd.read_sql_query(SQL_DIA, con, params={
                    "inicio": ini.isoformat(sep=" "),
                    "fin":    fin.isoformat(sep=" "),
                })
                df_day["fecha"] = ini.date()
                frames.append(df_day)

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)

        # --- 3) Promedios diarios por dispositivo (valores base)
        #     (estos serán mapeados a los nombres de CONSULTA 2)
        grp = df.groupby(["device_id","uplinks_per_day"], as_index=False).agg({
            "recibidos_n":"mean",
            "sin_gps_n":"mean",
            "con_gps_n":"mean",
            "validas_n":"mean",
            "baja_precision_n":"mean",
            "no_validas_n":"mean",
            "no_valida_calidad_gps_n":"mean",
            "no_valida_filtro_velocidad_n":"mean",
        })

        # --- 4) Derivados (porcentajes, iguales a CONSULTA 2 en naming)
        # Evita divisiones por 0
        def safe_div(num, den, mult=100.0):
            return (num/den*mult) if den and den>0 else 0.0

        # % recibidos vs esperados (por día, usando promedio diario)
        grp["pct_recibidos_vs_esperados"] = grp.apply(
            lambda r: safe_div(r["recibidos_n"], r["uplinks_per_day"]), axis=1
        ).round(2)

        # % sin GPS vs esperados (por día)
        grp["pct_sin_gps_vs_esperados"] = grp.apply(
            lambda r: safe_div(r["sin_gps_n"], r["uplinks_per_day"]), axis=1
        ).round(2)

        # % sin GPS sobre recibidos (por día)
        grp["pct_sin_gps_recibidos"] = grp.apply(
            lambda r: safe_div(r["sin_gps_n"], r["recibidos_n"], mult=1.0), axis=1
        ).round(3)

        # Bloque detallado (mismos nombres de consulta 2)
        grp['"Mensajes esperados (detallado)"'] = grp["uplinks_per_day"]
        grp['"Mensajes recibidos (n)"'] = grp["recibidos_n"].round(2)
        grp['"Mensajes recibidos (%)"'] = grp.apply(
            lambda r: safe_div(r["recibidos_n"], r["uplinks_per_day"]), axis=1
        ).round(2)

        grp['"Mensaje con posición GPS (n)"'] = grp["con_gps_n"].round(2)
        grp['"Mensaje con posición GPS (%)"'] = grp.apply(
            lambda r: safe_div(r["con_gps_n"], r["recibidos_n"]), axis=1
        ).round(2)

        grp['"Posición GPS válida (n)"'] = grp["validas_n"].round(2)
        grp['"Posición GPS válida (%)"'] = grp.apply(
            lambda r: safe_div(r["validas_n"], r["recibidos_n"]), axis=1
        ).round(2)

        grp['"Baja precisión (n)"'] = grp["baja_precision_n"].round(2)
        grp['"Baja precisión (%)"'] = grp.apply(
            lambda r: safe_div(r["baja_precision_n"], r["validas_n"]), axis=1
        ).round(2)

        grp['"Posición GPS no válida (n)"'] = grp["no_validas_n"].round(2)
        grp['"Posición GPS no válida (%)"'] = grp.apply(
            lambda r: safe_div(r["no_validas_n"], r["recibidos_n"]), axis=1
        ).round(2)

        grp['"No válida por calidad GPS (n)"'] = grp["no_valida_calidad_gps_n"].round(2)
        grp['"No válida por calidad GPS (%)"'] = grp.apply(
            lambda r: safe_div(r["no_valida_calidad_gps_n"], r["no_validas_n"]), axis=1
        ).round(2)

        grp['"No válida por filtro velocidad (n)"'] = grp["no_valida_filtro_velocidad_n"].round(2)
        grp['"No válida por filtro velocidad (%)"'] = grp.apply(
            lambda r: safe_div(r["no_valida_filtro_velocidad_n"], r["no_validas_n"]), axis=1
        ).round(2)

        # KPI "Posición válida vs esperadas (%)"
        grp['"Posición válida vs esperadas (%)"'] = grp.apply(
            lambda r: safe_div(r["validas_n"], r["uplinks_per_day"]), axis=1
        ).round(2)

        # Dispositivo OK (>60% válidas vs esperadas)
        grp['"Dispositivo OK (>60% válidas vs esperadas)"'] = grp['"Posición válida vs esperadas (%)"'] > 60.0

        # --- 5) Merge con metadatos/últimos mensajes
        out = grp.merge(df_meta, how="left", left_on="device_id", right_on="device_id")

        # Alias/compatibilidad con consulta 2 (campos base)
        out["mensajes_esperados"] = out["uplinks_per_day"].round(2)
        out["mensajes_recibidos"] = out["recibidos_n"].round(2)
        out["mensajes_sin_gps"]  = out["sin_gps_n"].round(2)

        # % dispositivos OK en ganadería (y bandera de ganadería OK)
        if "ranch_name" in out.columns:
            ok_by_ranch = (
                out.groupby("ranch_name")[ '"Dispositivo OK (>60% válidas vs esperadas)"' ]
                .mean()
                .mul(100.0)
                .round(2)
                .rename("% dispositivos OK en ganadería")
            )
            out = out.merge(ok_by_ranch, left_on="ranch_name", right_index=True, how="left")
            out['"Ganadería OK (>70% dispositivos OK)"'] = out["% dispositivos OK en ganadería"] > 70.0
        else:
            out["% dispositivos OK en ganadería"] = 0.0
            out['"Ganadería OK (>70% dispositivos OK)"'] = False

        # --- 6) Reordenar columnas EXACTAS como en consulta 2
        cols = [
            "device_id", "SerialNumber", "Model",
            "mensajes_esperados",
            "mensajes_recibidos", "mensajes_sin_gps",
            "pct_recibidos_vs_esperados", "pct_sin_gps_vs_esperados", "pct_sin_gps_recibidos",
            "ultimo_mensaje_recibido", "ultima_posicion_gps_valida", "ultima_posicion_geom", "visto_ultima_vez",
            "numero_reinicios", "media_ttf", "porcentaje_bateria", "fecha_cambio_bateria", "suma_total_uplinks",
            "ranch_name", "customer_name", "animal_name",
            "Country", "Region",
            '"Mensajes esperados (detallado)"',
            '"Mensajes recibidos (n)"', '"Mensajes recibidos (%)"',
            '"Mensaje con posición GPS (n)"', '"Mensaje con posición GPS (%)"',
            '"Posición GPS válida (n)"', '"Posición GPS válida (%)"',
            '"Baja precisión (n)"', '"Baja precisión (%)"',
            '"Posición GPS no válida (n)"', '"Posición GPS no válida (%)"',
            '"No válida por calidad GPS (n)"', '"No válida por calidad GPS (%)"',
            '"No válida por filtro velocidad (n)"', '"No válida por filtro velocidad (%)"',
            '"Posición válida vs esperadas (%)"',
            '"Dispositivo OK (>60% válidas vs esperadas)"',
            "% dispositivos OK en ganadería",
            '"Ganadería OK (>70% dispositivos OK)"'
        ]
        # Mantén solo las que existen por seguridad
        cols = [c for c in cols if c in out.columns]
        out = out[cols].copy()

        # Redondeos: numéricas
        for c in out.select_dtypes(include=["float", "int"]).columns:
            out[c] = out[c].round(2)

        # Orden final por el KPI principal
        kpi = '"Posición válida vs esperadas (%)"'
        if kpi in out.columns:
            out = out.sort_values(kpi, ascending=True, na_position="last")

        print(f"✅ consulta_03 OK | Rango fijo: {start:%Y-%m-%d} → {end:%Y-%m-%d} | Días: {dias} | Filas: {out.shape[0]}")
        return out

    except Exception as e:
        print(f"❌ Error en consulta_03: {e}")
        traceback.print_exc()
        return pd.DataFrame()
