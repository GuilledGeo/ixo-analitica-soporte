# -*- coding: utf-8 -*-
"""
consulta_05_detalle_por_mensaje – Últimos N días (por defecto 60) DETALLE POR MENSAJE,
replicando las MISMAS columnas calculadas del query 24h (DT01), pero evaluadas por DÍA NATURAL
del propio mensaje. Filtro por ganadería.

- Un registro por uplink de DeviceLocations.
- Para cada uplink, se adjuntan las métricas/porcentajes del DISPOSITIVO en su día natural:
  "Mensajes recibidos (n)", "Posición GPS válida (%)", "Posición válida vs esperadas (%)",
  "Dispositivo OK (≥50% válidas vs esperadas)", "% dispositivos OK en ganadería",
  "Ganadería OK (≥70% dispositivos OK)", etc.
- Incluye metadatos y gateways (igual que la 24h).
- Filtra por clientes ACTIVOS (Customers.Status = 'active').

Parámetros por defecto:
  days = 60
  ranch_name = "Daniel Arias González"

Notas:
- SQLAlchemy 2.x + pandas (usar sqlalchemy.text)
- TZ de sesión para días naturales (Europe/Madrid)
"""

import os
import inspect
import traceback
from datetime import datetime, timedelta, date
from typing import List

import pandas as pd
from sqlalchemy import text

DEFAULT_DAYS = 60
DEFAULT_RANCH_NAME = "Daniel Arias González"

# ============================================================
#  CTEs base + gateways (filtro por ganadería, clientes activos)
# ============================================================
SQL_BASE_Y_GATEWAYS = """
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
base_all AS (
  SELECT d.*, ca.animal_name
  FROM active_devices d
  RIGHT JOIN current_animals ca ON ca."DeviceId" = d."Id"
  WHERE d."Id" IS NOT NULL
),
ranches_filtrados AS (
  SELECT r."Id" AS ranch_id
  FROM "Ranches" r
  JOIN "Customers" c ON c."Id" = r."CustomerId" AND c."Status" = 'active'
  WHERE UPPER(TRIM(r."Name")) = UPPER(TRIM(:ranch_name))
),
base AS (
  SELECT b.*
  FROM base_all b
  JOIN ranches_filtrados rf ON rf.ranch_id = b."RanchId"
),

-- Gateways agregado por rancho (como en DT01)
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
  WHERE rlg."RanchId" IN (SELECT ranch_id FROM ranches_filtrados)
  GROUP BY rlg."RanchId"
),
gw_derived AS (
  SELECT
    a.ranch_id,
    a.total_gateways,
    a.gateways_online,
    CASE
      WHEN COALESCE(a.total_gateways,0) = 0                   THEN FALSE
      WHEN a.gateways_online = a.total_gateways               THEN TRUE
      ELSE FALSE
    END AS all_gateways_online,
    CASE
      WHEN COALESCE(a.total_gateways,0) = 0                   THEN 'sin_gateway'
      WHEN a.gateways_online = 0                              THEN 'todas_desconectadas'
      WHEN a.gateways_online = a.total_gateways               THEN 'todas_conectadas'
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
  WHERE rlg."RanchId" IN (SELECT ranch_id FROM ranches_filtrados)
),

-- Últimos mensajes / última posición válida (histórico total, no solo el día)
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
  JOIN base b ON b."Id" = dl."DeviceId"
  GROUP BY dl."DeviceId"
)
"""

# ============================================================
#  SQL POR DÍA NATURAL: métricas diarias + registros (uplinks)
#  Reglas DT01 (≥50% válidas vs esperadas)
# ============================================================
SQL_DIA_DETALLE = text(f"""
{SQL_BASE_Y_GATEWAYS},

-- Registros del día (para contar y también para listar al final)
dl_dia AS (
  SELECT dl.*
  FROM "DeviceLocations" dl
  JOIN base b ON b."Id" = dl."DeviceId"
  WHERE dl."Time" >= :inicio
    AND dl."Time" <  :fin
),

-- Métricas detalladas del DÍA por dispositivo (idéntico a DT01 pero acotado al día)
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

-- Métricas simples del DÍA (equivalente a gps_stats_24h_all de DT01)
gps_stats_24h_all AS (
  SELECT
    b."Id" AS device_id,
    COUNT(dl."Time") AS total_mensajes_24h,
    COUNT(dl."Time") FILTER (WHERE NOT dl."HasLocation") AS mensajes_sin_gps_24h
  FROM base b
  LEFT JOIN dl_dia dl ON dl."DeviceId" = b."Id"
  GROUP BY b."Id"
),

-- Derivados por dispositivo del DÍA (porcentajes vs UplinksPerDay) — REGLA DT01 ≥50%
derivados AS (
  SELECT
    b."Id" AS device_id,
    b."UplinksPerDay" AS uplinks_per_day,
    gp.recibidos_n, gp.con_gps_n, gp.validas_n, gp.baja_precision_n, gp.no_validas_n,
    gp.no_valida_calidad_gps_n, gp.no_valida_filtro_velocidad_n, gp.dias_con_datos,
    g24.total_mensajes_24h, g24.mensajes_sin_gps_24h,

    ROUND(
      CASE WHEN b."UplinksPerDay" > 0
           THEN gp.recibidos_n::numeric / b."UplinksPerDay" * 100
      END, 2
    ) AS pct_recibidos_vs_esperados,

    ROUND(
      CASE WHEN b."UplinksPerDay" > 0
           THEN g24.mensajes_sin_gps_24h::numeric / b."UplinksPerDay" * 100
      END, 2
    ) AS pct_sin_gps_vs_esperados,

    ROUND(
      CASE WHEN COALESCE(g24.total_mensajes_24h, 0) > 0
           THEN COALESCE(g24.mensajes_sin_gps_24h::numeric, 0) / g24.total_mensajes_24h
      END, 3
    ) AS pct_sin_gps_recibidos,

    -- KPI "Posición válida vs esperadas (%)"
    ROUND(
      CASE WHEN b."UplinksPerDay" > 0
           THEN COALESCE(gp.validas_n,0)::numeric / b."UplinksPerDay" * 100
      END, 2
    ) AS pos_valida_vs_esperadas_pct,

    -- Dispositivo OK (≥50% válidas vs esperadas)
    CASE
      WHEN b."UplinksPerDay" > 0
           AND (COALESCE(gp.validas_n,0)::numeric / b."UplinksPerDay" * 100) >= 50
      THEN TRUE ELSE FALSE
    END AS device_ok_ge50
  FROM base b
  LEFT JOIN gps_stats_periodo gp ON gp.device_id = b."Id"
  LEFT JOIN gps_stats_24h_all g24 ON g24.device_id = b."Id"
),

-- % dispositivos OK por ganadería (por DÍA) usando promedio por dispositivo (no por uplink)
ok_ranch AS (
  SELECT
    r."Name" AS ranch_name,
    ROUND( 100.0 * AVG( CASE WHEN derv.device_ok_ge50 THEN 1 ELSE 0 END ), 2 ) AS pct_ok_ranch
  FROM derivados derv
  JOIN base b ON b."Id" = derv.device_id
  JOIN "Ranches" r ON b."RanchId" = r."Id"
  GROUP BY r."Name"
),

-- Bandera de ganadería OK (≥70%) por DÍA
ok_ranch_flag AS (
  SELECT
    ranch_name,
    CASE WHEN pct_ok_ranch >= 70 THEN TRUE ELSE FALSE END AS ranch_ok_ge70
  FROM ok_ranch
)

SELECT
  -- Identificación y metadatos de dispositivo
  b."Id"               AS device_id,
  b."SerialNumber",
  b."Model",
  b."UplinksPerDay"    AS mensajes_esperados,

  -- Bloque simple 24h (del día natural)
  COALESCE(g24.total_mensajes_24h, 0)   AS mensajes_recibidos,
  COALESCE(g24.mensajes_sin_gps_24h, 0) AS mensajes_sin_gps,

  derv.pct_recibidos_vs_esperados,
  derv.pct_sin_gps_vs_esperados,
  derv.pct_sin_gps_recibidos,

  -- Últimos mensajes (histórico total) + metadatos
  gf.ultimo_mensaje_recibido,
  gf.ultima_posicion_gps_valida,
  gf.ultima_posicion_geom,
  b."LastSeenOn" AS visto_ultima_vez,

  b."ResetsCount" AS numero_reinicios,
  b."AverageGpsTtf" AS media_ttf,
  b."BatteryEstimation" AS porcentaje_bateria,
  b."ChangedBatteryOn" AS fecha_cambio_bateria,
  b."SumUplinksCount" AS suma_total_uplinks,

  r."Name"    AS ranch_name,
  c."Name"    AS customer_name,
  b.animal_name AS animal_name,
  r."Country" AS "Country",
  r."Region"  AS "Region",

  -- =============================
  -- BLOQUE DETALLADO (día natural)
  -- =============================
  b."UplinksPerDay"                              AS "Mensajes esperados (detallado)",
  COALESCE(gp.recibidos_n, 0)                    AS "Mensajes recibidos (n)",
  derv.pct_recibidos_vs_esperados                AS "Mensajes recibidos (%)",
  COALESCE(gp.con_gps_n, 0)                      AS "Mensaje con posición GPS (n)",
  ROUND( CASE WHEN gp.recibidos_n > 0 THEN gp.con_gps_n::numeric / gp.recibidos_n * 100 END, 2 )
                                                 AS "Mensaje con posición GPS (%)",
  COALESCE(gp.validas_n, 0)                      AS "Posición GPS válida (n)",
  ROUND( CASE WHEN gp.recibidos_n > 0 THEN gp.validas_n::numeric / gp.recibidos_n * 100 END, 2 )
                                                 AS "Posición GPS válida (%)",
  COALESCE(gp.baja_precision_n, 0)               AS "Baja precisión (n)",
  ROUND( CASE WHEN gp.validas_n > 0 THEN gp.baja_precision_n::numeric / gp.validas_n * 100 END, 2 )
                                                 AS "Baja precisión (%)",
  COALESCE(gp.no_validas_n, 0)                   AS "Posición GPS no válida (n)",
  ROUND( CASE WHEN gp.recibidos_n > 0 THEN gp.no_validas_n::numeric / gp.recibidos_n * 100 END, 2 )
                                                 AS "Posición GPS no válida (%)",
  COALESCE(gp.no_valida_calidad_gps_n, 0)        AS "No válida por calidad GPS (n)",
  ROUND( CASE WHEN gp.no_validas_n > 0 THEN gp.no_valida_calidad_gps_n::numeric / gp.no_validas_n * 100 END, 2 )
                                                 AS "No válida por calidad GPS (%)",
  COALESCE(gp.no_valida_filtro_velocidad_n, 0)   AS "No válida por filtro velocidad (n)",
  ROUND( CASE WHEN gp.no_validas_n > 0 THEN gp.no_valida_filtro_velocidad_n::numeric / gp.no_validas_n * 100 END, 2 )
                                                 AS "No válida por filtro velocidad (%)",

  -- KPI y banderas (REGLAS DT01 por día del mensaje)
  derv.pos_valida_vs_esperadas_pct               AS "Posición válida vs esperadas (%)",
  derv.device_ok_ge50                            AS "Dispositivo OK (≥50% válidas vs esperadas)",
  ok.pct_ok_ranch                                AS "% dispositivos OK en ganadería",
  okf.ranch_ok_ge70                              AS "Ganadería OK (≥70% dispositivos OK)",

  -- =============================
  -- INFO GATEWAYS (idéntico estilo)
  -- =============================
  COALESCE(ga.total_gateways, 0)              AS ranch_gateway_count,
  COALESCE(ga.gateways_online, 0)             AS gateways_online,
  COALESCE(ga.all_gateways_online, FALSE)     AS all_gateways_online,
  COALESCE(ga.ranch_gateway_overall_status, 'sin_gateway') AS ranch_gateway_overall_status,

  gl.gateway_id,
  gl.gateway_name,
  gl.gateway_serial,
  gl.gateway_last_seen,
  CASE WHEN gl.gateway_location IS NOT NULL THEN ST_Y(gl.gateway_location::geometry) END AS gateway_lat,
  CASE WHEN gl.gateway_location IS NOT NULL THEN ST_X(gl.gateway_location::geometry) END AS gateway_lon,
  CASE
    WHEN gl.gateway_last_seen IS NULL THEN NULL
    ELSE ROUND(EXTRACT(EPOCH FROM (NOW() - gl.gateway_last_seen)) / 3600.0, 2)
  END                                         AS horas_desde_ultimo_visto,

  -- =============================
  -- REGISTRO CRUDO (uplink DEL DÍA)
  -- =============================
  dl."Time"          AS "Time",
  dl."HasLocation"   AS "HasLocation",
  dl."IsValid"       AS "IsValid",
  dl."IsLowAccuracy" AS "IsLowAccuracy",
  dl."InvalidReason" AS "InvalidReason",
  dl."Location"      AS "Location",
  CASE WHEN dl."Location" IS NOT NULL THEN ST_Y(dl."Location"::geometry) END AS "lat",
  CASE WHEN dl."Location" IS NOT NULL THEN ST_X(dl."Location"::geometry) END AS "lon"

FROM base b
JOIN "Ranches" r  ON b."RanchId" = r."Id"
JOIN "Customers" c ON r."CustomerId" = c."Id" AND c."Status" = 'active'

LEFT JOIN gps_stats_periodo gp ON gp.device_id = b."Id"
LEFT JOIN gps_stats_24h_all g24 ON g24.device_id = b."Id"
LEFT JOIN derivados derv       ON derv.device_id = b."Id"
LEFT JOIN ok_ranch ok          ON ok.ranch_name = r."Name"
LEFT JOIN ok_ranch_flag okf    ON okf.ranch_name = r."Name"

LEFT JOIN gw_derived ga ON ga.ranch_id = r."Id"
LEFT JOIN gw_latest  gl ON gl.rn = 1 AND gl.ranch_id = r."Id"
LEFT JOIN gps_stats_full gf ON gf."DeviceId" = b."Id"

LEFT JOIN dl_dia dl ON dl."DeviceId" = b."Id"  -- Mensajes del día (uno por fila)

ORDER BY dl."Time" ASC, b."Id" ASC;
""")

# ============================================================
#  Utilidad fechas naturales (N días)
# ============================================================
def _start_end_dates(days: int):
    """
    Devuelve (inicio_00:00 de hace N-1 días, fin_24:00 de hoy, N) en fecha local.
    """
    days = int(days) if days and int(days) > 0 else DEFAULT_DAYS
    hoy = date.today()
    start = datetime.combine(hoy - timedelta(days=days-1), datetime.min.time())
    end   = datetime.combine(hoy + timedelta(days=1), datetime.min.time())  # exclusivo
    return start, end, days

# ============================================================
#  EJECUCIÓN
# ============================================================
def ejecutar(
    engine,
    days: int = DEFAULT_DAYS,
    ranch_name: str = DEFAULT_RANCH_NAME,
    set_timezone: str = "Europe/Madrid",
    save_csv: bool = False,
    outdir: str = "data/processed",
    filename_prefix: str = "consulta_05_detalle_por_mensaje"
) -> pd.DataFrame:
    """
    Devuelve UN REGISTRO POR MENSAJE de los últimos N días naturales (por defecto 60),
    para la ganadería dada, con REGLAS DT01 (≥50%) + metadatos y gateways.
    """
    try:
        try:
            frame_file = inspect.getfile(inspect.currentframe())
            nombre_script = os.path.splitext(os.path.basename(frame_file))[0]
        except Exception:
            nombre_script = "consulta_05_detalle_por_mensaje"

        start, end, ndays = _start_end_dates(days)

        frames: List[pd.DataFrame] = []
        with engine.connect() as con:
            if set_timezone:
                con.exec_driver_sql(f"SET TIME ZONE '{set_timezone}';")
            con.exec_driver_sql("SET LOCAL lock_timeout = '5s';")
            con.exec_driver_sql("SET LOCAL statement_timeout = '10min';")

            for i in range(ndays):
                ini = (start + timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
                fin = ini + timedelta(days=1)

                df_day = pd.read_sql_query(
                    SQL_DIA_DETALLE,
                    con,
                    params={
                        "ranch_name": ranch_name,
                        "inicio": ini.isoformat(sep=" "),
                        "fin":    fin.isoformat(sep=" "),
                    }
                )
                if not df_day.empty:
                    df_day["fecha_natural"] = ini.date()
                    frames.append(df_day)

        if not frames:
            print(f"⚠️ {nombre_script}: Sin mensajes para '{ranch_name}' en los últimos {ndays} días.")
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)

        # Rellenar solo numéricas (sin tocar fechas/bools/strings)
        num_cols = df.select_dtypes(include=["number"]).columns
        if len(num_cols) > 0:
            df[num_cols] = df[num_cols].fillna(0)

        # Orden estable (por Time y device)
        sort_cols = [c for c in ["Time", "device_id"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, ascending=[True]*len(sort_cols), na_position="last")

        # Guardado opcional
        if save_csv:
            os.makedirs(outdir, exist_ok=True)
            ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
            safe_ranch = ranch_name.replace(" ", "_")
            fname = f"{filename_prefix}_{ts}_rancho_{safe_ranch}_ndias_{ndays}.csv"
            path = os.path.join(outdir, fname)
            df.to_csv(path, index=False, encoding="utf-8-sig")
            print(f"📁 CSV guardado: {path}")

        print(
            f"✅ {nombre_script} OK | Ran: '{ranch_name}' | Últimos {ndays} días | "
            f"Filas: {len(df)} | Columnas: {len(df.columns)}"
        )
        return df

    except Exception as e:
        print(f"❌ Error en consulta_05_detalle_por_mensaje: {e}")
        traceback.print_exc()
        return pd.DataFrame()


# =========================
#  USO DE EJEMPLO (opcional)
# =========================
if __name__ == "__main__":
    """
    from sqlalchemy import create_engine
    engine = create_engine("postgresql+psycopg2://USER:PASS@HOST:PORT/DB")

    # Detalle por mensaje, 60 días, Daniel Arias González, guardando CSV
    # df = ejecutar(engine, days=60, ranch_name="Daniel Arias González", save_csv=True)
    """
    pass
