# -*- coding: utf-8 -*-
"""
CONSULTA DT01 – KPI de Disponibilidad GPS por Dispositivo (últimas 24h)
Con retry automático para conflictos con recovery en réplicas de lectura
"""

import os
import inspect
import traceback
import time
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

# =========================
#  SQL PRINCIPAL (24h, UTC)
# =========================
query = """
WITH active_devices AS (
  SELECT *
  FROM "Devices"
  WHERE "UplinksPerDay" IS NOT NULL
    AND "Disabled" = FALSE
    AND "StatusType" = 'shipped'
),

current_animals AS (
  SELECT
    "DeviceId",
    MAX("Name")   AS animal_name,
    MAX("Specie") AS animal_specie
  FROM "Animals"
  WHERE "IsDeregistered" = FALSE
  GROUP BY "DeviceId"
),

-- Dispositivos base: activos + shipped + con animal activo
base AS (
  SELECT
    d.*,
    ca.animal_name,
    ca.animal_specie
  FROM active_devices d
  RIGHT JOIN current_animals ca
    ON ca."DeviceId" = d."Id"
  WHERE d."Id" IS NOT NULL
),

-- 🔒 Filtro adicional (UTC):
--   IgnoreStatisticsUntil es DATE (UTC) y "incluye el día".
--   Se incluye el device sólo si es NULL o es ANTERIOR a la fecha UTC de hoy.
allowed_devices AS (
  SELECT d."Id" AS device_id
  FROM base d
  WHERE d."IgnoreStatisticsUntil" IS NULL
     OR d."IgnoreStatisticsUntil" < (NOW() AT TIME ZONE 'UTC')::date
),

-- A partir de aquí, todo el pipeline usa base_filtered
base_filtered AS (
  SELECT d.*
  FROM base d
  JOIN allowed_devices a
    ON a.device_id = d."Id"
),

gps_stats_24h_all AS (
  SELECT
    d."Id" AS device_id,
    COUNT(dl."Time") AS total_mensajes_24h,
    COUNT(dl."Time") FILTER (WHERE NOT dl."HasLocation") AS mensajes_sin_gps_24h
  FROM base_filtered d
  LEFT JOIN "DeviceLocations" dl
    ON dl."DeviceId" = d."Id"
   AND dl."Time" >= NOW() - INTERVAL '24 HOURS'
  GROUP BY d."Id"
),

-- Nota: calculado para todos los DeviceLocations, luego se cruza con base_filtered
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
),

gps_stats_periodo AS (
  SELECT
    d."Id" AS device_id,
    COUNT(dl."Time")                                                    AS recibidos_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation")                    AS con_gps_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND dl."IsValid")   AS validas_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND dl."IsValid" AND dl."IsLowAccuracy") AS baja_precision_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid") AS no_validas_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid" AND dl."InvalidReason" = 'parameters') AS no_valida_calidad_gps_n,
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid" AND dl."InvalidReason" = 'distance')   AS no_valida_filtro_velocidad_n,
    COUNT(DISTINCT DATE(dl."Time"))                                     AS dias_con_datos
  FROM base_filtered d
  LEFT JOIN "DeviceLocations" dl
    ON dl."DeviceId" = d."Id"
   AND dl."Time" >= NOW() - INTERVAL '24 HOURS'
  GROUP BY d."Id"
),

gw_agg AS (
  SELECT
    rlg."RanchId"                                   AS ranch_id,
    COUNT(*)                                        AS total_gateways,
    COUNT(*) FILTER (
      WHERE lg."LastSeenAt" IS NOT NULL
        AND lg."LastSeenAt" >= NOW() - INTERVAL '3 hours'
    )                                               AS gateways_online
  FROM "RanchesLoraGateways" rlg
  JOIN "LoraGateways" lg
    ON lg."Id" = rlg."GatewayId"
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
    END                                                       AS all_gateways_online,
    CASE
      WHEN COALESCE(a.total_gateways,0) = 0                   THEN 'sin_gateway'
      WHEN a.gateways_online = 0                              THEN 'todas_desconectadas'
      WHEN a.gateways_online = a.total_gateways               THEN 'todas_conectadas'
      ELSE 'algunas_desconectadas'
    END                                                       AS ranch_gateway_overall_status
  FROM gw_agg a
),

gw_latest AS (
  SELECT
    rlg."RanchId"                           AS ranch_id,
    lg."Id"                                 AS gateway_id,
    lg."Name"                               AS gateway_name,
    lg."SerialNumber"                       AS gateway_serial,
    lg."LastSeenAt"                         AS gateway_last_seen,
    lg."Location"                           AS gateway_location,
    ROW_NUMBER() OVER (
      PARTITION BY rlg."RanchId"
      ORDER BY lg."LastSeenAt" DESC NULLS LAST, lg."CreatedAt" DESC NULLS LAST
    ) AS rn
  FROM "RanchesLoraGateways" rlg
  JOIN "LoraGateways" lg
    ON lg."Id" = rlg."GatewayId"
),

device_flags AS (
  SELECT
    d."Id"         AS device_id,
    r."Id"         AS ranch_id,
    d."UplinksPerDay",
    CASE
      WHEN d."UplinksPerDay" > 0
           AND (COALESCE(gp.validas_n,0)::numeric / d."UplinksPerDay" * 100) >= 50
      THEN TRUE ELSE FALSE
    END AS device_ok_base,
    COALESCE(gf.ultimo_mensaje_recibido >= NOW() - INTERVAL '3 days', FALSE) AS communicated_3d
  FROM base_filtered d
  JOIN "Ranches" r ON d."RanchId" = r."Id"
  LEFT JOIN gps_stats_periodo gp ON gp.device_id = d."Id"
  LEFT JOIN gps_stats_full    gf ON gf."DeviceId" = d."Id"
),

ranch_rollup AS (
  SELECT
    ranch_id,
    COUNT(*) AS ranch_devices,
    SUM(CASE WHEN device_ok_base THEN 1 ELSE 0 END) AS ok_base_count,
    SUM(CASE WHEN NOT device_ok_base THEN 1 ELSE 0 END) AS non_ok_count,
    SUM(CASE WHEN NOT device_ok_base AND communicated_3d THEN 1 ELSE 0 END) AS non_ok_comm3d_count
  FROM device_flags
  GROUP BY ranch_id
),

ranch_status AS (
  SELECT
    rr.*,
    ROUND(100.0 * ok_base_count / NULLIF(ranch_devices,0), 2) AS pct_ok_base,
    (100.0 * ok_base_count / NULLIF(ranch_devices,0)) >= 50 AS ranch_ok_base,
    CASE
      WHEN (100.0 * ok_base_count / NULLIF(ranch_devices,0)) < 50
           AND non_ok_count > 0
           AND non_ok_comm3d_count = non_ok_count
      THEN rr.ranch_devices
      ELSE ok_base_count
    END AS ok_adjusted_count,
    CASE
      WHEN (100.0 * ok_base_count / NULLIF(ranch_devices,0)) < 50
           AND non_ok_count > 0
           AND non_ok_comm3d_count = non_ok_count
      THEN TRUE ELSE FALSE
    END AS ajuste_por_comunicacion_3d_aplicado
  FROM ranch_rollup rr
),

ranch_status_final AS (
  SELECT
    rs.*,
    ROUND(100.0 * ok_adjusted_count / NULLIF(ranch_devices,0), 2) AS pct_ok_ajustada,
    (100.0 * ok_adjusted_count / NULLIF(ranch_devices,0)) >= 50   AS ranch_ok_ajustada
  FROM ranch_status rs
)

SELECT
  d."Id" AS device_id,
  d."SerialNumber",
  d."Model",
  d."UplinksPerDay" AS mensajes_esperados,

  COALESCE(g24.total_mensajes_24h, 0)   AS mensajes_recibidos,
  COALESCE(g24.mensajes_sin_gps_24h, 0) AS mensajes_sin_gps,

  ROUND(
    CASE WHEN d."UplinksPerDay" > 0
         THEN COALESCE(g24.total_mensajes_24h::numeric, 0) / d."UplinksPerDay" * 100
    END, 2
  ) AS pct_recibidos_vs_esperados,
  ROUND(
    CASE WHEN d."UplinksPerDay" > 0
         THEN COALESCE(g24.mensajes_sin_gps_24h::numeric, 0) / d."UplinksPerDay" * 100
    END, 2
  ) AS pct_sin_gps_vs_esperados,
  ROUND(
    CASE WHEN COALESCE(g24.total_mensajes_24h, 0) > 0
         THEN COALESCE(g24.mensajes_sin_gps_24h::numeric, 0) / g24.total_mensajes_24h
    END, 3
  ) AS pct_sin_gps_recibidos,

  gf.ultimo_mensaje_recibido,
  gf.ultima_posicion_gps_valida,
  gf.ultima_posicion_geom,
  d."LastSeenOn" AS visto_ultima_vez,

  d."ResetsCount" AS numero_reinicios,
  d."AverageGpsTtf" AS media_ttf,
  d."BatteryEstimation" AS porcentaje_bateria,
  d."ChangedBatteryOn" AS fecha_cambio_bateria,
  d."SumUplinksCount" AS suma_total_uplinks,

  r."Name"    AS ranch_name,
  c."Name"    AS customer_name,
  d.animal_name   AS animal_name,
  d.animal_specie AS animal_specie,
  r."Country" AS "Country",
  r."Region"  AS "Region",

  -- =============================
  -- MÉTRICAS DETALLADAS (24h)
  -- =============================
  d."UplinksPerDay"                                              AS "Mensajes esperados (detallado)",
  COALESCE(gp.recibidos_n, 0)                                    AS "Mensajes recibidos (n)",
  ROUND(
    CASE WHEN d."UplinksPerDay" > 0
         THEN gp.recibidos_n::numeric / d."UplinksPerDay" * 100
    END, 2
  )                                                              AS "Mensajes recibidos (%)",
  COALESCE(gp.con_gps_n, 0)                                      AS "Mensaje con posición GPS (n)",
  ROUND(
    CASE WHEN gp.recibidos_n > 0
         THEN gp.con_gps_n::numeric / gp.recibidos_n * 100
    END, 2
  )                                                              AS "Mensaje con posición GPS (%)",
  COALESCE(gp.validas_n, 0)                                      AS "Posición GPS válida (n)",
  ROUND(
    CASE WHEN gp.recibidos_n > 0
         THEN gp.validas_n::numeric / gp.recibidos_n * 100
    END, 2
  )                                                              AS "Posición GPS válida (%)",
  COALESCE(gp.baja_precision_n, 0)                               AS "Baja precisión (n)",
  ROUND(
    CASE WHEN gp.validas_n > 0
         THEN gp.baja_precision_n::numeric / gp.validas_n * 100
    END, 2
  )                                                              AS "Baja precisión (%)",
  COALESCE(gp.no_validas_n, 0)                                   AS "Posición GPS no válida (n)",
  ROUND(
    CASE WHEN gp.recibidos_n > 0
         THEN gp.no_validas_n::numeric / gp.recibidos_n * 100
    END, 2
  )                                                              AS "Posición GPS no válida (%)",
  COALESCE(gp.no_valida_calidad_gps_n, 0)                        AS "No válida por calidad GPS (n)",
  ROUND(
    CASE WHEN gp.no_validas_n > 0
         THEN gp.no_valida_calidad_gps_n::numeric / gp.no_validas_n * 100
    END, 2
  )                                                              AS "No válida por calidad GPS (%)",
  COALESCE(gp.no_valida_filtro_velocidad_n, 0)                   AS "No válida por filtro velocidad (n)",
  ROUND(
    CASE WHEN gp.no_validas_n > 0
         THEN gp.no_valida_filtro_velocidad_n::numeric / gp.no_validas_n * 100
    END, 2
  )                                                              AS "No válida por filtro velocidad (%)",

  ROUND(
    CASE WHEN d."UplinksPerDay" > 0
         THEN COALESCE(gp.validas_n,0)::numeric / d."UplinksPerDay" * 100
    END, 2
  )                                                              AS "Posición válida vs esperadas (%)",
  CASE
    WHEN d."UplinksPerDay" > 0
         AND (COALESCE(gp.validas_n,0)::numeric / d."UplinksPerDay" * 100) >= 50
    THEN TRUE ELSE FALSE
  END                                                            AS "Dispositivo OK (≥50% válidas vs esperadas)",

  rsf.pct_ok_base                                                AS "% dispositivos OK en ganadería",
  rsf.ranch_ok_base                                              AS "Ganadería OK",
  rsf.non_ok_count                                               AS "Dispositivos NO OK",
  rsf.non_ok_comm3d_count                                        AS "NO OK que comunicaron en 3 días",
  rsf.ajuste_por_comunicacion_3d_aplicado                        AS "Ajuste aplicado (todos NO OK comunicaron 3d)",
  rsf.pct_ok_ajustada                                            AS "% dispositivos OK en ganadería (ajustada)",
  rsf.ranch_ok_ajustada                                          AS "Ganadería OK (ajustada)",

  COALESCE(ga.total_gateways, 0)              AS ranch_gateway_count,
  COALESCE(ga.gateways_online, 0)             AS gateways_online,
  COALESCE(ga.all_gateways_online, FALSE)     AS all_gateways_online,
  COALESCE(ga.ranch_gateway_overall_status, 'sin_gateway') AS ranch_gateway_overall_status,

  gl.gateway_id,
  gl.gateway_name,
  gl.gateway_serial,
  gl.gateway_last_seen,
  ST_Y(gl.gateway_location::geometry)         AS gateway_lat,
  ST_X(gl.gateway_location::geometry)         AS gateway_lon,
  CASE
    WHEN gl.gateway_last_seen IS NULL THEN NULL
    ELSE ROUND(EXTRACT(EPOCH FROM (NOW() - gl.gateway_last_seen)) / 3600.0, 2)
  END                                         AS horas_desde_ultimo_visto

FROM base_filtered d
JOIN "Ranches" r ON d."RanchId" = r."Id"
JOIN "Customers" c ON r."CustomerId" = c."Id" AND c."Status" = 'active'
LEFT JOIN gps_stats_24h_all g24 ON g24.device_id = d."Id"
LEFT JOIN gps_stats_full    gf  ON gf."DeviceId" = d."Id"
LEFT JOIN gps_stats_periodo gp  ON gp.device_id = d."Id"
LEFT JOIN gw_derived ga ON ga.ranch_id = r."Id"
LEFT JOIN gw_latest  gl ON gl.ranch_id = r."Id" AND gl.rn = 1
LEFT JOIN ranch_status_final rsf ON rsf.ranch_id = r."Id"
ORDER BY rsf.pct_ok_ajustada ASC NULLS LAST, r."Name", d."SerialNumber";
"""

# =========================
#  FUNCIÓN CON RETRY
# =========================
def ejecutar(engine, set_timezone: str = "UTC", max_reintentos: int = 3):
    """
    Ejecuta la consulta con retry automático para conflictos con recovery.
    - Implementa backoff exponencial
    - Detecta específicamente errores de réplica
    """
    try:
        frame_file = inspect.getfile(inspect.currentframe())
        nombre_script = os.path.splitext(os.path.basename(frame_file))[0]
    except Exception:
        nombre_script = "consulta_01"

    for intento in range(max_reintentos):
        try:
            with engine.connect() as con:
                # Configurar sesión para réplica de lectura
                if set_timezone:
                    con.exec_driver_sql(f"SET TIME ZONE '{set_timezone}';")
                con.exec_driver_sql("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
                
                # Ejecutar query
                df = pd.read_sql_query(text(query), con)

            # Rellenar NaN solo en columnas numéricas
            num_cols = df.select_dtypes(include=["number"]).columns
            if len(num_cols) > 0:
                df[num_cols] = df[num_cols].fillna(0)

            print(f"✅ Consulta {nombre_script} ejecutada. Filas: {len(df)} | Columnas: {len(df.columns)}")
            return df

        except OperationalError as e:
            error_msg = str(e)
            
            # Detectar conflicto con recovery
            if "conflict with recovery" in error_msg or "SerializationFailure" in error_msg:
                if intento < max_reintentos - 1:
                    tiempo_espera = 3 * (intento + 1)  # 3s, 6s, 9s
                    print(f"⚠️ Conflicto con recovery detectado. Reintentando en {tiempo_espera}s... (Intento {intento + 1}/{max_reintentos})")
                    time.sleep(tiempo_espera)
                else:
                    print(f"❌ Error tras {max_reintentos} intentos: {e}")
                    traceback.print_exc()
                    return pd.DataFrame()
            else:
                # Otro tipo de error
                print(f"❌ Error al ejecutar la consulta {nombre_script}: {e}")
                traceback.print_exc()
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Error inesperado en {nombre_script}: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    return pd.DataFrame()