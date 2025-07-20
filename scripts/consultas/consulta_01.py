import pandas as pd
import inspect

### CONSULTA DT01 – Cálculo de KPI de Disponibilidad GPS por Dispositivo
### ENLACE DOC TÉCNICO:
# https://docs.google.com/document/d/1gUYgAl1l55f3G2AsynTLDZ7wLaBS9hhU4Sdvp_28eaQ/edit?tab=t.0#heading=h.jwis7nmqfhyx

# Consulta SQL
query = """
WITH active_devices AS (
  SELECT *
  FROM "Devices"
  WHERE "UplinksPerDay" IS NOT NULL
    AND "Disabled" = FALSE
),

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
  d."Id" AS device_id,
  d."SerialNumber",
  d."Model",
  d."UplinksPerDay" AS mensajes_esperados,

  COALESCE(g24.total_mensajes_24h, 0) AS mensajes_recibidos,
  COALESCE(g24.mensajes_sin_gps_24h, 0) AS mensajes_sin_gps,

  ROUND(COALESCE(g24.total_mensajes_24h::numeric, 0) / NULLIF(d."UplinksPerDay", 0), 2) AS pct_recibidos_vs_esperados,
  ROUND(COALESCE(g24.mensajes_sin_gps_24h::numeric, 0) / NULLIF(d."UplinksPerDay", 0), 2) AS pct_sin_gps_vs_esperados,
  ROUND(CASE
            WHEN COALESCE(g24.total_mensajes_24h, 0) = 0 THEN NULL
            ELSE COALESCE(g24.mensajes_sin_gps_24h::numeric, 0) / g24.total_mensajes_24h
        END, 3) AS pct_sin_gps_recibidos,

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
  a."Name" AS animal_name

FROM active_devices d
LEFT JOIN gps_stats_24h_all g24 ON g24.device_id = d."Id"
LEFT JOIN gps_stats_full gf ON gf."DeviceId" = d."Id"
JOIN "Ranches" r ON d."RanchId" = r."Id"
LEFT JOIN "Customers" c ON r."CustomerId" = c."Id"
JOIN "Animals" a ON a."DeviceId" = d."Id" AND a."IsDeregistered" = FALSE

WHERE (
    c."Status" = 'active'
    OR r."Name" IS NOT NULL
)

ORDER BY pct_recibidos_vs_esperados ASC NULLS LAST;
"""

# Función ejecutable desde main o desde Streamlit
def ejecutar(engine):
    nombre_script = inspect.getfile(inspect.currentframe()).split("/")[-1].replace(".py", "")
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        print(f"✅ Consulta {nombre_script} ejecutada correctamente.")
        return df
    except Exception as e:
        print(f"❌ Error al ejecutar la consulta {nombre_script}: {e}")
        return pd.DataFrame()
