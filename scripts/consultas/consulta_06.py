# -*- coding: utf-8 -*-
"""
CONSULTA DT01 – KPI de Disponibilidad GPS por Dispositivo (últimas 24h)
Con filtro severo: Animal activo + Device shipped (RIGHT JOIN sobre Animals)

Incluye:
- "Posición válida vs esperadas (%)"
- "Dispositivo OK (>50% válidas vs esperadas)"
- "% dispositivos OK en ganadería" (base y ajustada por ventana dinámica N días)
- "Ganadería OK (>50% dispositivos OK)"
- Clasificación de conexión por último mensaje
- Campos de Ranches: Country, Region

Notas:
- Usa SQLAlchemy 2.x + pandas → envolver SIEMPRE la SQL con sqlalchemy.text(query)
- Fija la zona horaria de sesión para evitar desfases con pgAdmin
- Rellena NaN solo en columnas numéricas (no tocar fechas/bools/strings)
"""

import os
import inspect
import traceback
import pandas as pd
from sqlalchemy import text

# =========================
#  SQL PRINCIPAL (24h) con ventana dinámica :dias_ventana
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
    MAX("Name") AS animal_name
  FROM "Animals"
  WHERE "IsDeregistered" = FALSE
  GROUP BY "DeviceId"
),
base AS (
  SELECT
    d.*,
    ca.animal_name
  FROM active_devices d
  RIGHT JOIN current_animals ca
    ON ca."DeviceId" = d."Id"
  WHERE d."Id" IS NOT NULL
),
gps_stats_24h_all AS (
  SELECT
    d."Id" AS device_id,
    COUNT(dl."Time") AS total_mensajes_24h,
    COUNT(dl."Time") FILTER (WHERE NOT dl."HasLocation") AS mensajes_sin_gps_24h
  FROM base d
  LEFT JOIN "DeviceLocations" dl
    ON dl."DeviceId" = d."Id"
   AND dl."Time" >= NOW() - INTERVAL '24 HOURS'
  GROUP BY d."Id"
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
  FROM base d
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

-- ===== Flags por dispositivo y rollup por ganadería (ventana dinámica) =====
device_flags AS (
  SELECT
    d."Id"         AS device_id,
    r."Id"         AS ranch_id,
    d."UplinksPerDay",
    -- Dispositivo OK (criterio base 24h ≥ 50% válidas vs esperadas)
    CASE
      WHEN d."UplinksPerDay" > 0
           AND (COALESCE(gp.validas_n,0)::numeric / d."UplinksPerDay" * 100) >= 50
      THEN TRUE ELSE FALSE
    END AS device_ok_base,
    -- ¿Ha comunicado en los últimos :dias_ventana días?
    COALESCE(gf.ultimo_mensaje_recibido >= (NOW() - make_interval(days => :dias_ventana)), FALSE) AS communicated_window
  FROM base d
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
    SUM(CASE WHEN NOT device_ok_base AND communicated_window THEN 1 ELSE 0 END) AS non_ok_comm_window_count
  FROM device_flags
  GROUP BY ranch_id
),

ranch_status AS (
  SELECT
    rr.*,
    ROUND(100.0 * ok_base_count / NULLIF(ranch_devices,0), 2) AS pct_ok_base,
    (100.0 * ok_base_count / NULLIF(ranch_devices,0)) >= 50 AS ranch_ok_base,
    -- Si la ganadería es NO OK y TODOS los NO OK comunicaron dentro de la ventana, ajustamos
    CASE
      WHEN (100.0 * ok_base_count / NULLIF(ranch_devices,0)) < 50
           AND non_ok_count > 0
           AND non_ok_comm_window_count = non_ok_count
      THEN rr.ranch_devices
      ELSE ok_base_count
    END AS ok_adjusted_count,
    -- Flag de ajuste aplicado
    CASE
      WHEN (100.0 * ok_base_count / NULLIF(ranch_devices,0)) < 50
           AND non_ok_count > 0
           AND non_ok_comm_window_count = non_ok_count
      THEN TRUE ELSE FALSE
    END AS ajuste_por_comunicacion_window_aplicado
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
  d.animal_name AS animal_name,
  r."Country" AS "Country",
  r."Region"  AS "Region",

  -- =============================
  -- Clasificación de estado de conexión (desde último mensaje)
  -- =============================
  CASE
    WHEN gf.ultimo_mensaje_recibido >= NOW() - INTERVAL '1 day'    THEN 'Conectado hoy'
    WHEN gf.ultimo_mensaje_recibido >= NOW() - INTERVAL '2 days'   THEN 'Conexión 24-48h'
    WHEN gf.ultimo_mensaje_recibido >= NOW() - INTERVAL '3 days'   THEN 'Conexión 48-72h'
    WHEN gf.ultimo_mensaje_recibido >= NOW() - INTERVAL '7 days'   THEN 'Conexión 3-7 días'
    WHEN gf.ultimo_mensaje_recibido >= NOW() - INTERVAL '15 days'  THEN 'Conexión 7-15 días'
    WHEN gf.ultimo_mensaje_recibido >= NOW() - INTERVAL '1 month'  THEN 'Conexión 15 días - 1 mes'
    WHEN gf.ultimo_mensaje_recibido >= NOW() - INTERVAL '3 months' THEN 'Conexión 1-3 meses'
    ELSE 'Conexión >3 meses'
  END AS clasificacion_conexion,

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

  -- Dispositivo OK (≥50% válidas vs esperadas)
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

  -- ===== Métricas OK por ganadería (base vs ajustada por ventana dinámica) =====
  rsf.pct_ok_base                                                AS "% dispositivos OK en ganadería",
  rsf.ranch_ok_base                                              AS "Ganadería OK",
  rsf.non_ok_count                                               AS "Dispositivos NO OK",
  rsf.non_ok_comm_window_count                                   AS "NO OK que comunicaron en ventana",
  rsf.ajuste_por_comunicacion_window_aplicado                    AS "Ajuste aplicado (todos NO OK comunicaron en ventana)",
  rsf.pct_ok_ajustada                                            AS "% dispositivos OK en ganadería (ajustada)",
  rsf.ranch_ok_ajustada                                          AS "Ganadería OK (ajustada)",

  -- (Opcional) expón la ventana aplicada por SQL
  :dias_ventana::int                                             AS ventana_dias_sql,

  -- =============================
  -- INFO GATEWAYS (informativo)
  -- =============================
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

FROM base d
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
#  EJECUCIÓN DESDE PYTHON
# =========================
def ejecutar(engine, set_timezone: str = "Europe/Madrid", dias_ventana: int = 3):
    """
    Ejecuta la consulta usando SQLAlchemy 2.x y pandas.
    - Fija la zona horaria de sesión (por defecto Europe/Madrid).
    - Parámetro dias_ventana controla la ventana de ajuste por comunicación reciente.
    - Envuélvela con sqlalchemy.text() para evitar problemas con CTEs/ventanas.
    - Rellena NaN solo en columnas numéricas.
    """
    try:
        frame_file = inspect.getfile(inspect.currentframe())
        nombre_script = os.path.splitext(os.path.basename(frame_file))[0]
    except Exception:
        nombre_script = "consulta_dt01"

    try:
        with engine.connect() as con:
            if set_timezone:
                con.exec_driver_sql(f"SET TIME ZONE '{set_timezone}';")

            df = pd.read_sql_query(
                text(query),
                con,
                params={"dias_ventana": int(dias_ventana)}
            )

        num_cols = df.select_dtypes(include=["number"]).columns
        if len(num_cols) > 0:
            df[num_cols] = df[num_cols].fillna(0)

        print(
            f"✅ Consulta {nombre_script} ejecutada. "
            f"Ventana ajuste: {dias_ventana} días | Filas: {len(df)} | Columnas: {len(df.columns)}"
        )
        return df

    except Exception as e:
        print(f"❌ Error al ejecutar la consulta {nombre_script}: {e}")
        traceback.print_exc()
        return pd.DataFrame()
