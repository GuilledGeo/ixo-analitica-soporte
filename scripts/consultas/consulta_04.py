# -*- coding: utf-8 -*-
import os
import sys
import time
from datetime import datetime, date, timedelta, timezone

import pandas as pd
from sqlalchemy import text

# Ruta a tu proyecto para importar get_engine()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from db_connection import get_engine  # noqa


# =========================
# Utilidades
# =========================
def _to_naive_utc(dt):
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _local_day_bounds_utc(local_day: date, tz_name: str):
    """Devuelve (inicio_utc, fin_utc) de un 'día natural' en la TZ dada."""
    # inicio local
    start_local = datetime(local_day.year, local_day.month, local_day.day, 0, 0, 0)
    # fin local
    end_local = start_local + timedelta(days=1)
    # convertir a UTC (sin tzinfo) para comparar con timestamptz
    start_utc = pd.Timestamp(start_local, tz=tz_name).tz_convert("UTC").to_pydatetime().replace(tzinfo=None)
    end_utc = pd.Timestamp(end_local, tz=tz_name).tz_convert("UTC").to_pydatetime().replace(tzinfo=None)
    return start_utc, end_utc


def _rollback(conn):
    try:
        conn.exec_driver_sql("ROLLBACK;")
    except Exception:
        pass


def _exec_read_sql(conn, sql_txt: str, params: dict, max_retries=3, sleep_base=1.2):
    """Ejecuta SQL con reintentos ante hot-standby (conflict with recovery) y limpia la transacción abortada."""
    for i in range(max_retries):
        try:
            return pd.read_sql_query(text(sql_txt), conn, params=params)
        except Exception as e:
            msg = str(e).lower()
            # Conflictos típicos en réplicas/hot standby o transacción abortada
            retriable = (
                "conflict with recovery" in msg
                or "could not serialize access" in msg
                or "deadlock detected" in msg
                or "infailedsqltransaction" in msg
                or "current transaction is aborted" in msg
                or "canceling statement due to conflict" in msg
            )
            if retriable and i < max_retries - 1:
                _rollback(conn)
                time.sleep(sleep_base * (2 ** i))
                continue
            raise


# =========================
# SQL – KPIs por DISPOSITIVO y DÍA (para TODOS)
# =========================
SQL_KPI_DIA_ALL = """
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

-- Antenas por rancho (global)
gw_agg AS (
  SELECT
    rlg."RanchId" AS ranch_id,
    COUNT(*)      AS total_gateways,
    COUNT(*) FILTER (
      WHERE lg."LastSeenAt" IS NOT NULL
        AND lg."LastSeenAt" >= NOW() - INTERVAL '3 hours'
    )             AS gateways_online
  FROM "RanchesLoraGateways" rlg
  JOIN "LoraGateways" lg ON lg."Id" = rlg."GatewayId"
  GROUP BY rlg."RanchId"
),
gw_derived AS (
  SELECT
    a.ranch_id,
    a.total_gateways,
    a.gateways_online,
    CASE
      WHEN COALESCE(a.total_gateways,0) = 0 THEN FALSE
      WHEN a.gateways_online = a.total_gateways THEN TRUE
      ELSE FALSE
    END AS all_gateways_online,
    CASE
      WHEN COALESCE(a.total_gateways,0) = 0 THEN 'sin_gateway'
      WHEN a.gateways_online = 0 THEN 'todas_desconectadas'
      WHEN a.gateways_online = a.total_gateways THEN 'todas_conectadas'
      ELSE 'algunas_desconectadas'
    END AS ranch_gateway_overall_status
  FROM gw_agg a
),
gw_latest AS (
  SELECT
    rlg."RanchId" AS ranch_id,
    lg."Id"       AS gateway_id,
    lg."Name"     AS gateway_name,
    lg."SerialNumber" AS gateway_serial,
    lg."LastSeenAt"   AS gateway_last_seen,
    lg."Location"     AS gateway_location,
    ROW_NUMBER() OVER (
      PARTITION BY rlg."RanchId"
      ORDER BY lg."LastSeenAt" DESC NULLS LAST, lg."CreatedAt" DESC NULLS LAST
    ) AS rn
  FROM "RanchesLoraGateways" rlg
  JOIN "LoraGateways" lg ON lg."Id" = rlg."GatewayId"
),

-- Registros del DÍA NATURAL (en TZ de sesión)
dl_dia AS (
  SELECT dl.*
  FROM "DeviceLocations" dl
  JOIN base b ON b."Id" = dl."DeviceId"
  WHERE dl."Time" >= :inicio
    AND dl."Time" <  :fin
),

-- Métricas detalladas del DÍA por dispositivo
gps_stats_periodo AS (
  SELECT
    b."Id" AS device_id,
    COUNT(dl."Time")                                                    AS recibidos_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation")                    AS con_gps_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND dl."IsValid")   AS validas_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND dl."IsValid" AND dl."IsLowAccuracy") AS baja_precision_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid") AS no_validas_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid" AND dl."InvalidReason" = 'parameters') AS no_valida_calidad_gps_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid" AND dl."InvalidReason" = 'distance')   AS no_valida_filtro_velocidad_n,
    COUNT(DISTINCT DATE(dl."Time"))                                     AS dias_con_datos
  FROM base b
  LEFT JOIN dl_dia dl ON dl."DeviceId" = b."Id"
  GROUP BY b."Id"
),

-- Métricas simples del DÍA (equivalente a gps_stats_24h_all)
gps_stats_24h_all AS (
  SELECT
    b."Id" AS device_id,
    COUNT(dl."Time") AS total_mensajes_24h,
    COUNT(dl."Time") FILTER (WHERE NOT dl."HasLocation") AS mensajes_sin_gps_24h
  FROM base b
  LEFT JOIN dl_dia dl ON dl."DeviceId" = b."Id"
  GROUP BY b."Id"
),

-- Derivados por dispositivo del DÍA (porcentajes igual que 24h)
derivados AS (
  SELECT
    b."Id" AS device_id,
    b."UplinksPerDay" AS uplinks_per_day,
    gp.recibidos_n, gp.con_gps_n, gp.validas_n, gp.baja_precision_n, gp.no_validas_n,
    gp.no_valida_calidad_gps_n, gp.no_valida_filtro_velocidad_n, gp.dias_con_datos,
    g24.total_mensajes_24h, g24.mensajes_sin_gps_24h,

    ROUND(CASE WHEN b."UplinksPerDay" > 0
           THEN gp.recibidos_n::numeric / b."UplinksPerDay" * 100 END, 2) AS pct_recibidos_vs_esperados,

    ROUND(CASE WHEN b."UplinksPerDay" > 0
           THEN g24.mensajes_sin_gps_24h::numeric / b."UplinksPerDay" * 100 END, 2) AS pct_sin_gps_vs_esperados,

    ROUND(CASE WHEN COALESCE(g24.total_mensajes_24h, 0) > 0
           THEN COALESCE(g24.mensajes_sin_gps_24h::numeric, 0) / g24.total_mensajes_24h END, 3) AS pct_sin_gps_recibidos,

    ROUND(CASE WHEN b."UplinksPerDay" > 0
           THEN COALESCE(gp.validas_n,0)::numeric / b."UplinksPerDay" * 100 END, 2) AS pos_valida_vs_esperadas_pct,

    CASE WHEN b."UplinksPerDay" > 0
           AND (COALESCE(gp.validas_n,0)::numeric / b."UplinksPerDay" * 100) >= 60
         THEN TRUE ELSE FALSE END AS device_ok_ge60
  FROM base b
  LEFT JOIN gps_stats_periodo gp ON gp.device_id = b."Id"
  LEFT JOIN gps_stats_24h_all g24 ON g24.device_id = b."Id"
),

-- %% dispositivos OK por rancho (por DÍA)
ok_ranch AS (
  SELECT
    r."Id"   AS ranch_id,
    r."Name" AS ranch_name,
    ROUND( 100.0 * AVG( CASE WHEN derv.device_ok_ge60 THEN 1 ELSE 0 END ), 2 ) AS pct_ok_ranch
  FROM derivados derv
  JOIN base b ON b."Id" = derv.device_id
  JOIN "Ranches" r ON b."RanchId" = r."Id"
  GROUP BY r."Id", r."Name"
),
ok_ranch_flag AS (
  SELECT
    ranch_id,
    CASE WHEN pct_ok_ranch >= 70 THEN TRUE ELSE FALSE END AS ranch_ok_ge70
  FROM ok_ranch
)

SELECT
  DATE(:dia_local)                              AS dia,         -- día natural (para el merge con mensajes)
  b."Id"                                        AS device_id,
  b."SerialNumber",
  b."Model",
  b."UplinksPerDay"                             AS mensajes_esperados,

  -- bloque simple (día)
  COALESCE(g24.total_mensajes_24h, 0)           AS mensajes_recibidos,
  COALESCE(g24.mensajes_sin_gps_24h, 0)         AS mensajes_sin_gps,

  derv.pct_recibidos_vs_esperados               AS pct_recibidos_vs_esperados,
  derv.pct_sin_gps_vs_esperados                 AS pct_sin_gps_vs_esperados,
  derv.pct_sin_gps_recibidos                    AS pct_sin_gps_recibidos,

  -- detallado (día)
  b."UplinksPerDay"                              AS "Mensajes esperados (detallado)",
  COALESCE(gp.recibidos_n, 0)                    AS "Mensajes recibidos (n)",
  derv.pct_recibidos_vs_esperados                AS "Mensajes recibidos (%%)",
  COALESCE(gp.con_gps_n, 0)                      AS "Mensaje con posición GPS (n)",
  ROUND(CASE WHEN gp.recibidos_n > 0 THEN gp.con_gps_n::numeric / gp.recibidos_n * 100 END, 2)
                                               AS "Mensaje con posición GPS (%%)",
  COALESCE(gp.validas_n, 0)                      AS "Posición GPS válida (n)",
  ROUND(CASE WHEN gp.recibidos_n > 0 THEN gp.validas_n::numeric / gp.recibidos_n * 100 END, 2)
                                               AS "Posición GPS válida (%%)",
  COALESCE(gp.baja_precision_n, 0)               AS "Baja precisión (n)",
  ROUND(CASE WHEN gp.validas_n > 0 THEN gp.baja_precision_n::numeric / gp.validas_n * 100 END, 2)
                                               AS "Baja precisión (%%)",
  COALESCE(gp.no_validas_n, 0)                   AS "Posición GPS no válida (n)",
  ROUND(CASE WHEN gp.recibidos_n > 0 THEN gp.no_validas_n::numeric / gp.recibidos_n * 100 END, 2)
                                               AS "Posición GPS no válida (%%)",
  COALESCE(gp.no_valida_calidad_gps_n, 0)        AS "No válida por calidad GPS (n)",
  ROUND(CASE WHEN gp.no_validas_n > 0 THEN gp.no_valida_calidad_gps_n::numeric / gp.no_validas_n * 100 END, 2)
                                               AS "No válida por calidad GPS (%%)",
  COALESCE(gp.no_valida_filtro_velocidad_n, 0)   AS "No válida por filtro velocidad (n)",
  ROUND(CASE WHEN gp.no_validas_n > 0 THEN gp.no_valida_filtro_velocidad_n::numeric / gp.no_validas_n * 100 END, 2)
                                               AS "No válida por filtro velocidad (%%)",

  -- KPI y banderas (día)
  derv.pos_valida_vs_esperadas_pct               AS "Posición válida vs esperadas (%%)",
  derv.device_ok_ge60                            AS "Dispositivo OK (≥60%% válidas vs esperadas)",

  ok.pct_ok_ranch                                AS "%% dispositivos OK en ganadería",
  okf.ranch_ok_ge70                              AS "Ganadería OK (≥70%% dispositivos OK)",

  -- Ranch / Customer
  r."Id"                                         AS ranch_id,
  r."Name"                                       AS ranch_name,
  c."Name"                                       AS customer_name,
  r."Country"                                    AS "Country",
  r."Region"                                     AS "Region",

  -- Gateways (info global)
  COALESCE(ga.total_gateways, 0)                 AS ranch_gateway_count,
  COALESCE(ga.gateways_online, 0)                AS gateways_online,
  COALESCE(ga.all_gateways_online, FALSE)        AS all_gateways_online,
  COALESCE(ga.ranch_gateway_overall_status, 'sin_gateway') AS ranch_gateway_overall_status,

  gl.gateway_id,
  gl.gateway_name,
  gl.gateway_serial,
  gl.gateway_last_seen,
  CASE WHEN gl.gateway_location IS NOT NULL THEN ST_Y(gl.gateway_location::geometry) END AS gateway_lat,
  CASE WHEN gl.gateway_location IS NOT NULL THEN ST_X(gl.gateway_location::geometry) END AS gateway_lon

FROM base b
JOIN "Ranches"  r  ON b."RanchId" = r."Id"
LEFT JOIN "Customers" c ON r."CustomerId" = c."Id"

LEFT JOIN gps_stats_periodo gp ON gp.device_id = b."Id"
LEFT JOIN gps_stats_24h_all g24 ON g24.device_id = b."Id"
LEFT JOIN derivados derv       ON derv.device_id = b."Id"
LEFT JOIN ok_ranch ok          ON ok.ranch_id = r."Id"
LEFT JOIN ok_ranch_flag okf    ON okf.ranch_id = r."Id"

LEFT JOIN gw_derived ga ON ga.ranch_id = r."Id"
LEFT JOIN gw_latest  gl ON gl.ranch_id = r."Id" AND gl.rn = 1
ORDER BY b."Id" ASC;
"""


# =========================
# SQL – Mensajes del DÍA por batches
# =========================
SQL_MENSAJES_BATCH = """
SELECT 
    dl."DeviceId"             AS device_id,
    d."SerialNumber"          AS serial_number,
    d."DevEui"                AS dev_eui,
    d."Model"                 AS model,
    d."UplinksPerDay"         AS uplinks_per_day,
    dl."Time"                 AS timestamp,
    DATE(dl."Time")           AS dia,  -- importante para el merge con KPIs del día
    dl."HasLocation",
    dl."IsValid",
    dl."IsLowAccuracy",
    dl."InvalidReason",
    dl."Location",
    CASE WHEN dl."Location" IS NOT NULL THEN ST_Y(dl."Location"::geometry) END AS lat,
    CASE WHEN dl."Location" IS NOT NULL THEN ST_X(dl."Location"::geometry) END AS lon,
    r."Id"                    AS ranch_id,
    r."Name"                  AS ranch_name,
    c."Name"                  AS customer_name,
    a."EarTag"                AS animal_eartag,
    a."Name"                  AS animal_name,
    a."ZoneId"                AS animal_zone_id,
    a."Sex"                   AS animal_sex,
    a."BirthDate"             AS animal_birthdate,
    a."Specie"                AS animal_specie
FROM "DeviceLocations" dl
LEFT JOIN "Devices" d 
       ON dl."DeviceId" = d."Id"
RIGHT JOIN "Animals" a 
       ON a."DeviceId" = d."Id"
LEFT JOIN "Ranches" r 
       ON d."RanchId" = r."Id"
LEFT JOIN "Customers" c 
       ON r."CustomerId" = c."Id"
WHERE dl."Time" >= :inicio
  AND dl."Time" <  :fin
  AND (dl."Time" >  :cursor OR (dl."Time" = :cursor AND dl."DeviceId" > :cursor_dev))
  AND d."StatusType" = 'shipped'
ORDER BY dl."Time", dl."DeviceId"
LIMIT :lim;
"""


# =========================
# SQL – Últimos mensajes / última posición válida (histórico) para un set de devices
# =========================
SQL_GF_POR_DEVICES = """
WITH ids AS (
  SELECT UNNEST(:device_ids::int[]) AS device_id
)
SELECT
  dl."DeviceId"                                              AS device_id,
  MAX(dl."Time")                                             AS ultimo_mensaje_recibido,
  (
    SELECT MAX(sub."Time")
    FROM "DeviceLocations" sub
    WHERE sub."DeviceId" = dl."DeviceId"
      AND sub."HasLocation" = TRUE
  )                                                          AS ultima_posicion_gps_valida,
  (
    SELECT sub."Location"
    FROM "DeviceLocations" sub
    WHERE sub."DeviceId" = dl."DeviceId"
      AND sub."HasLocation" = TRUE
    ORDER BY sub."Time" DESC
    LIMIT 1
  )                                                          AS ultima_posicion_geom
FROM "DeviceLocations" dl
JOIN ids ON ids.device_id = dl."DeviceId"
GROUP BY dl."DeviceId";
"""


# =========================
# FUNCIÓN PRINCIPAL (por días naturales + batches dentro del día)
# =========================
def get_messages_with_kpis_by_batches(
    start_datetime,
    end_datetime,
    tz_name="Europe/Madrid",
    rows_per_batch=50000,
    max_retries=3,
):
    """
    Devuelve un DataFrame con **un registro por mensaje** para TODOS los dispositivos,
    en el rango [start_datetime, end_datetime), calculando **todas las columnas KPI**
    (idénticas en forma a la consulta 24h) **por día natural** en la TZ indicada.

    Estrategia:
      - Bucle por DÍAS naturales (mitiga conflictos en hot-standby).
      - Para cada día:
          1) Se calculan los KPIs por DISPOSITIVO (día completo).
          2) Se extraen los mensajes del día en batches (cursor por timestamp+device_id).
          3) Se hace merge (device_id + dia) para añadir KPIs a cada mensaje.
          4) Se añaden campos "gf.*" (últimos mensajes) para los devices del día.
    """
    start_datetime = _to_naive_utc(start_datetime)
    end_datetime = _to_naive_utc(end_datetime)

    frames = []

    engine = get_engine()
    with engine.connect() as con:
        # Zona horaria de sesión para que DATE(dl."Time") sea "día natural" correcto
        con.exec_driver_sql(f"SET TIME ZONE '{tz_name}';")
        # Aislamiento seguro para réplicas
        con.exec_driver_sql("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ;")

        day = start_datetime.date()
        end_day = (end_datetime - timedelta(microseconds=1)).date()  # inclusivo si hay algo en el último día

        while day <= end_day:
            inicio_dia_utc, fin_dia_utc = _local_day_bounds_utc(day, tz_name)

            # Recorta al rango global
            inicio = max(inicio_dia_utc, start_datetime)
            fin = min(fin_dia_utc, end_datetime)
            if inicio >= fin:
                day += timedelta(days=1)
                continue

            # 1) KPIs por dispositivo (para TODO el día natural)
            params_kpi = {
                "inicio": inicio,
                "fin": fin,
                "dia_local": day.isoformat(),
            }
            kpi_df = _exec_read_sql(con, SQL_KPI_DIA_ALL, params_kpi, max_retries=max_retries)

            # Para joins rápidos
            # (device_id, dia) es la clave de merge en el lote de mensajes
            kpi_df["dia"] = pd.to_datetime(kpi_df["dia"]).dt.date

            # 2) Extraer MENSAJES del día en batches
            cursor_ts = inicio - timedelta(microseconds=1)  # para > cursor
            cursor_dev = 0
            mensajes_dia = []
            devices_dia = set()

            while True:
                params_batch = {
                    "inicio": inicio,
                    "fin": fin,
                    "cursor": cursor_ts,
                    "cursor_dev": int(cursor_dev),
                    "lim": int(rows_per_batch),
                }
                df_batch = _exec_read_sql(con, SQL_MENSAJES_BATCH, params_batch, max_retries=max_retries)
                if df_batch.empty:
                    break

                # Acumular devices del día (para GF)
                if "device_id" in df_batch.columns:
                    devices_dia.update(df_batch["device_id"].unique())

                # Merge KPI (device_id + dia)
                df_batch["dia"] = pd.to_datetime(df_batch["dia"]).dt.date
                out = df_batch.merge(
                    kpi_df,
                    how="left",
                    on=["device_id", "dia"],
                    suffixes=("", "_kpi"),
                )

                mensajes_dia.append(out)

                # Avanzar cursor (última fila del batch)
                last = df_batch.iloc[-1]
                cursor_ts = pd.to_datetime(last["timestamp"]).to_pydatetime()
                cursor_dev = int(last["device_id"])

                # Si ya alcanzamos el fin del día, salir
                if cursor_ts >= (fin - timedelta(microseconds=1)):
                    break

            if not mensajes_dia:
                # No hubo mensajes ese día → seguimos
                day += timedelta(days=1)
                continue

            df_mensajes = pd.concat(mensajes_dia, ignore_index=True)

            # 3) Añadir GF (últimos mensajes/última posición) para los devices del día
            try:
                if devices_dia:
                    ids_list = list(sorted(int(x) for x in devices_dia))
                    gf_df = _exec_read_sql(
                        con,
                        SQL_GF_POR_DEVICES,
                        {"device_ids": ids_list},
                        max_retries=max_retries,
                    )
                    df_mensajes = df_mensajes.merge(gf_df, how="left", on="device_id")
            except Exception as e:
                print(f"⚠️  No se pudo obtener 'gps_stats_full' del día {day}: {e}")

            frames.append(df_mensajes)
            day += timedelta(days=1)

    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


# =========================
# Ejemplo de uso directo
# =========================
if __name__ == "__main__":
    # Últimos 60 días naturales (en la TZ indicada)
    tz = "Europe/Madrid"
    today_local = pd.Timestamp.now(tz).normalize().to_pydatetime()
    start_local = (today_local - timedelta(days=60))
    end_local = (today_local + timedelta(days=1))  # hasta fin de hoy

    df = get_messages_with_kpis_by_batches(
        start_datetime=start_local,
        end_datetime=end_local,
        tz_name=tz,
        rows_per_batch=50000,   # ajusta según rendimiento
        max_retries=4,
    )

    print(f"✅ Filas: {len(df)} | Columnas: {len(df.columns)}")

    # Guardar
    outdir = os.path.join("data", "processed")
    os.makedirs(outdir, exist_ok=True)
    fname = f"consulta_06_detalle_por_mensaje_all_batches_{pd.Timestamp(start_local).date()}_{pd.Timestamp(end_local).date()}.csv"
    fpath = os.path.join(outdir, fname)
    df.to_csv(fpath, index=False)
    print(f"📁 Guardado en: {fpath}")
