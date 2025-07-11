import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import inspect
from datetime import datetime
from matplotlib.colors import ListedColormap, BoundaryNorm

plt.style.use("ggplot")

def ejecutar():
    nombre_script = os.path.splitext(os.path.basename(inspect.getfile(inspect.currentframe())))[0]
    carpeta_csv = "data/processed"
    carpeta_figs = os.path.join("outputs", "figures", nombre_script)

    subdir_top = os.path.join(carpeta_figs, "Top Clientes Inactivos y Ratio")
    os.makedirs(subdir_top, exist_ok=True)

    archivos = sorted(
        [f for f in os.listdir(carpeta_csv) if f.startswith(nombre_script) and f.endswith(".csv")],
        reverse=True
    )

    if not archivos:
        print(f"⚠️ No hay CSVs para {nombre_script}")
        return

    for archivo_csv in archivos:
        nombre_base = archivo_csv.replace(".csv", "")
        ruta_csv = os.path.join(carpeta_csv, archivo_csv)
        ruta_fig = os.path.join(subdir_top, f"{nombre_base}_heatmap_inactivos48h_y_ratios.png")

        if os.path.exists(ruta_fig):
            print(f"✅ Heatmap ya generado para {archivo_csv}")
            continue

        print(f"📄 Procesando CSV: {archivo_csv}")
        df = pd.read_csv(ruta_csv)

        try:
            fecha_str, hora_str = archivo_csv.split("_")[2:4]
            dt_full = datetime.strptime(f"{fecha_str}_{hora_str}", "%Y-%m-%d_%H-%M")
            fecha_hora_titulo = dt_full.strftime("%d/%m/%Y %H:%M")
        except Exception:
            fecha_hora_titulo = "fecha desconocida"

        columnas_necesarias = {"clasificacion_conexion", "pct_recibidos_vs_esperados", "customer_name"}
        if not columnas_necesarias.issubset(df.columns):
            print(f"⚠️ Saltando heatmap: faltan columnas necesarias en {archivo_csv}")
            continue

        # Calcular ratio y tramo
        df["ratio_mensajes"] = pd.to_numeric(df["pct_recibidos_vs_esperados"], errors="coerce")

        def tramo_ratio_detallado(r):
            if pd.isna(r): return "Desconocido"
            elif r < 0.2: return "<20%"
            elif r < 0.4: return "20–40%"
            elif r < 0.6: return "40–60%"
            elif r < 0.8: return "60–80%"
            elif r <= 1.0: return "80–100%"
            else: return ">100%"

        df["tramo_ratio_detallado"] = df["ratio_mensajes"].apply(tramo_ratio_detallado)

        # ✅ Clasificación excluyente por dispositivo
        def categoria_final(row):
            if row["clasificacion_conexion"] in [
                "Conexión 24-48h", "Conexión 48-72h",
                "Inactivo 1 semana", "Inactivo 15 días", "Inactivo 1 mes",
                "Inactivo 3 meses", "Inactivo > 3 meses"
            ]:
                return row["clasificacion_conexion"]
            else:
                return row["tramo_ratio_detallado"]

        df["categoria_final"] = df.apply(categoria_final, axis=1)

        # 🎯 Seleccionar TOP 10 clientes con más inactivos recientes (24–72h)
        df_2472 = df[df["categoria_final"].isin(["Conexión 24-48h", "Conexión 48-72h"])]
        top_clientes = (
            df_2472.groupby("customer_name")
            .size().sort_values(ascending=False)
            .head(10).index
        )

        df_top = df[df["customer_name"].isin(top_clientes)].copy()

        # 🔢 Crear tabla de categorías excluyentes
        tabla = df_top.groupby(["customer_name", "categoria_final"])["device_id"].nunique().unstack(fill_value=0)

        # 🔀 Asegurar columnas ordenadas (rellenar con 0 si faltan)
        columnas_orden = ["Conexión 48-72h", "Conexión 24-48h", "<20%", "20–40%", "40–60%", "60–80%", "80–100%", ">100%"]
        for col in columnas_orden:
            if col not in tabla.columns:
                tabla[col] = 0
        tabla = tabla[columnas_orden]

        # 🏷 Añadir etiqueta de inactivos >72h en el nombre del cliente
        total_por_cliente = df_top.groupby("customer_name").size()
        inactivos_72 = df_top[df_top["categoria_final"].isin([
            "Inactivo 1 semana", "Inactivo 15 días", "Inactivo 1 mes",
            "Inactivo 3 meses", "Inactivo > 3 meses"
        ])].groupby("customer_name").size()

        tabla.index = [
            f"{cliente} (Inact. >72h: {inactivos_72.get(cliente, 0)} ud, {round((inactivos_72.get(cliente, 0)/total_por_cliente.get(cliente,1))*100)}%)"
            for cliente in tabla.index
        ]

        tabla = tabla.sort_values("Conexión 24-48h", ascending=False)

        # 🎨 Crear heatmap (lo puedes dejar igual que antes)


        # Configurar heatmap
        cmap = ListedColormap(["#ffffcc", "#c7e9b4", "#7fcdbb", "#41b6c4", "#1d91c0", "#225ea8", "#9e3b9e"])
        boundaries = [0, 1, 21, 41, 61, 81, 101, tabla.values.max() + 1]
        norm = BoundaryNorm(boundaries, cmap.N, clip=True)

        plt.figure(figsize=(14, 7))
        sns.heatmap(
            tabla,
            annot=True,
            fmt=".0f",
            cmap=cmap,
            norm=norm,
            cbar_kws={"label": "Nº de dispositivos"}
        )
        plt.axvline(2, color='gray', linestyle='--', linewidth=1.2)
        plt.title(f"Análisis últimas 24h: Top 10 clientes con dispositivos inactivos (>48h) + ratios\n{fecha_hora_titulo}", fontsize=13)
        plt.xlabel("Categoría")
        plt.ylabel("Cliente")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(ruta_fig, bbox_inches="tight")
        plt.clf()
        print(f"✅ Heatmap generado: {ruta_fig}")
