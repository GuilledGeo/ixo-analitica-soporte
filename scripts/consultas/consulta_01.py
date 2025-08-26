# -*- coding: utf-8 -*-
"""
CONSULTA DT01 – KPI de Disponibilidad GPS por Dispositivo (últimas 24h)
Incluye métricas detalladas y condicionante de ganadería:
- "Posición válida vs esperadas (%)"
- "Dispositivo OK (>60% válidas vs esperadas)"
- "% dispositivos OK en ganadería"
- "Ganadería OK (>70% dispositivos OK)"

Notas de ejecución:
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
#  SQL PRINCIPAL (24h)
# =========================
query = """
WITH active_devices AS (
  SELECT *
  FROM "Devices"
  WHERE "UplinksPerDay" IS NOT NULL
    AND "Disabled" = FALSE
    AND "StatusType" = 'shipped'
),

-- Métricas simples 24h (tu CTE original)
gps_stats_24h_all AS (
  SELECT
    d."Id" AS device_id,
    COUNT(dl."Time") AS total_mensajes_24h,
    COUNT(dl."Time") FILTER (WHERE NOT dl."HasLocation") AS mensajes_sin_gps_24h
  FROM active_devices d
  LEFT JOIN "DeviceLocations" dl
    ON dl."DeviceId" = d."Id"
   AND dl."Time" >= NOW() - INTERVAL '24 HOURS'
  GROUP BY d."Id"
),

-- Últimos mensajes / última posición válida (sin límite temporal)
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

-- Métricas detalladas 24h (denominadores exactos de tu definición)
gps_stats_periodo AS (
  SELECT
    d."Id" AS device_id,
    COUNT(dl."Time")                                                    AS recibidos_n,             -- Mensajes recibidos (n)
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation")                    AS con_gps_n,               -- Mensaje con posición GPS (n)
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND dl."IsValid")   AS validas_n,               -- Posición GPS válida (n)
    COUNT(dl."Time") FILTER (
      WHERE dl."HasLocation" AND dl."IsValid" AND dl."IsLowAccuracy"
    )                                                                   AS baja_precision_n,        -- Baja precisión (n)
    COUNT(dl."Time") FILTER (WHERE dl."HasLocation" AND NOT dl."IsValid") AS no_validas_n,          -- Posición GPS no válida (n)
    COUNT(dl."Time") FILTER (
      WHERE dl."HasLocation" AND NOT dl."IsValid" AND dl."InvalidReason" = 'parameters'
    )                                                                   AS no_valida_calidad_gps_n, -- No válida por calidad GPS (n)
    COUNT(dl."Time") FILTER (
      WHERE dl."HasLocation" AND NOT dl."IsValid" AND dl."InvalidReason" = 'distance'
    )                                                                   AS no_valida_filtro_velocidad_n, -- No válida por filtro velocidad (n)
    COUNT(DISTINCT DATE(dl."Time"))                                     AS dias_con_datos           -- no se usa en 24h, pero queda por si amplías periodo
  FROM active_devices d
  LEFT JOIN "DeviceLocations" dl
    ON dl."DeviceId" = d."Id"
   AND dl."Time" >= NOW() - INTERVAL '24 HOURS'
  GROUP BY d."Id"
)

SELECT
  d."Id" AS device_id,
  d."SerialNumber",
  d."Model",
  d."UplinksPerDay" AS mensajes_esperados,

  COALESCE(g24.total_mensajes_24h, 0) AS mensajes_recibidos,
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

  r."Name" AS ranch_name,
  c."Name" AS customer_name,
  a."Name" AS animal_name,

  -- =============================
  -- MÉTRICAS DETALLADAS (24h)
  -- =============================
  d."UplinksPerDay"                                              AS "Mensajes esperados (detallado)",  -- = UplinksPerDay en 24h
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

  -- =========================================================
  -- CONDICIONANTE DE GANADERÍA (ranch_name)
  -- =========================================================
  ROUND(
    CASE WHEN d."UplinksPerDay" > 0
         THEN COALESCE(gp.validas_n,0)::numeric / d."UplinksPerDay" * 100
    END, 2
  )                                                              AS "Posición válida vs esperadas (%)",

  CASE
    WHEN d."UplinksPerDay" > 0
         AND (COALESCE(gp.validas_n,0)::numeric / d."UplinksPerDay" * 100) > 60
    THEN TRUE ELSE FALSE
  END                                                            AS "Dispositivo OK (>60% válidas vs esperadas)",

  ROUND(
    100.0 * AVG(
      CASE
        WHEN d."UplinksPerDay" > 0
             AND (COALESCE(gp.validas_n,0)::numeric / d."UplinksPerDay" * 100) > 60
        THEN 1 ELSE 0
      END
    ) OVER (PARTITION BY r."Name")
  , 2)                                                           AS "% dispositivos OK en ganadería",

  CASE
    WHEN (
      100.0 * AVG(
        CASE
          WHEN d."UplinksPerDay" > 0
               AND (COALESCE(gp.validas_n,0)::numeric / d."UplinksPerDay" * 100) > 60
          THEN 1 ELSE 0
        END
      ) OVER (PARTITION BY r."Name")
    ) > 70
    THEN TRUE ELSE FALSE
  END                                                            AS "Ganadería OK (>70% dispositivos OK)"

FROM active_devices d
LEFT JOIN gps_stats_24h_all g24 ON g24.device_id = d."Id"
LEFT JOIN gps_stats_full gf ON gf."DeviceId" = d."Id"
LEFT JOIN gps_stats_periodo gp ON gp.device_id = d."Id"
JOIN "Ranches" r ON d."RanchId" = r."Id"
LEFT JOIN "Customers" c ON r."CustomerId" = c."Id"
JOIN "Animals" a ON a."DeviceId" = d."Id" AND a."IsDeregistered" = FALSE

WHERE (c."Status" = 'active' OR r."Name" IS NOT NULL)
ORDER BY pct_recibidos_vs_esperados ASC NULLS LAST;
"""

# =========================
#  EJECUCIÓN DESDE PYTHON
# =========================
def ejecutar(engine, set_timezone: str = "Europe/Madrid"):
    """
    Ejecuta la consulta usando SQLAlchemy 2.x y pandas.
    - Fija la zona horaria de sesión (por defecto Europe/Madrid).
    - Envuélvela con sqlalchemy.text() para evitar problemas con CTEs/ventanas.
    - Rellena NaN solo en columnas numéricas.
    """
    # Nombre del script (para logs)
    try:
        frame_file = inspect.getfile(inspect.currentframe())
        nombre_script = os.path.splitext(os.path.basename(frame_file))[0]
    except Exception:
        nombre_script = "consulta_dt01"

    try:
        with engine.connect() as con:
            # Alinea la sesión a tu TZ habitual (ajusta si usas UTC en servidor)
            if set_timezone:
                con.exec_driver_sql(f"SET TIME ZONE '{set_timezone}';")

            # Ejecutar la SQL (IMPORTANTE: text(query))
            df = pd.read_sql_query(text(query), con)

        # Rellenar solo columnas numéricas
        num_cols = df.select_dtypes(include=["number"]).columns
        if len(num_cols) > 0:
            df[num_cols] = df[num_cols].fillna(0)

        print(f"✅ Consulta {nombre_script} ejecutada. Filas: {len(df)} | Columnas: {len(df.columns)}")
        return df

    except Exception as e:
        print(f"❌ Error al ejecutar la consulta {nombre_script}: {e}")
        traceback.print_exc()
        return pd.DataFrame()

# =========================
#  (Opcional) uso de ejemplo
# =========================
# from your_project.db_connection import get_engine
# engine = get_engine()
# df = ejecutar(engine)
# df.to_csv("dt01_disponibilidad_24h.csv", index=False, encoding="utf-8")
