import pandas as pd
import inspect

### CONSULTA DT02: Análisis de disponibilidad GPS + estado técnico del dispositivo (últimas 24h)

### ENLACE DOC TECNICO: https://docs.google.com/document/d/1KvX30okGAnoTZSe45aHTd-Kzw1JzT9uVdgD-0H7V9XI/edit?tab=t.0

# Consulta SQL (como string multilínea con triple comilla)
query = """


SELECT
    d."Id" AS device_id,
    d."UplinksPerDay",
    COUNT(al."Time") AS mensajes_con_gps_ult_24h,
    d."UplinksPerDay" AS mensajes_esperados_ult_24h,
    ROUND(COUNT(al."Time")::numeric / NULLIF(d."UplinksPerDay", 0), 2) AS ratio_gps_kpi,
    r."Name" AS ranch_name,
    c."Name" AS customer_name,
    
    -- Columnas ampliadas
    d."Model",
    d."SerialNumber",
    d."ResetsCount",
    d."AverageGpsTtf",
    d."BatteryEstimation",
    d."BatteryEstimationDate",
    d."SumUplinksCount",
    NOW() - d."LastSeenOn" AS visto_hace

FROM
    "AnimalLocations" al
JOIN
    "Devices" d ON al."DeviceId" = d."Id"
JOIN
    "Ranches" r ON d."RanchId" = r."Id"
JOIN
    "Customers" c ON r."CustomerId" = c."Id"
WHERE
    al."Time" >= NOW() - INTERVAL '24 HOURS'
    AND al."Location" IS NOT NULL
GROUP BY
    d."Id", d."UplinksPerDay", r."Name", c."Name",
    d."Model", d."SerialNumber", d."ResetsCount", d."AverageGpsTtf",
    d."BatteryEstimation", d."BatteryEstimationDate", d."SumUplinksCount", d."LastSeenOn"
ORDER BY
    ratio_gps_kpi ASC;


"""

# 🔁 Función ejecutable desde main
def ejecutar(conn):
    nombre_script = inspect.getfile(inspect.currentframe()).split("/")[-1].replace(".py", "")
    try:
        df = pd.read_sql_query(query, conn)  # conn es ahora un engine SQLAlchemy
        print(f"✅ Consulta {nombre_script} ejecutada correctamente.")
        return df
    except Exception as e:
        print(f"❌ Error al ejecutar la consulta {nombre_script}: {e}")
        return pd.DataFrame()