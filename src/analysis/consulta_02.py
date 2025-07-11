import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import inspect
from datetime import datetime
from matplotlib.patches import Patch

plt.style.use("ggplot")

def ejecutar():
    nombre_script = os.path.splitext(os.path.basename(inspect.getfile(inspect.currentframe())))[0]
    carpeta_csv = "data/processed"
    carpeta_figs = os.path.join("outputs", "figures", nombre_script)

    # Subcarpetas temáticas
    subdir_hist = os.path.join(carpeta_figs, "Distribución ratio de mensajes recibidos")
    subdir_clasif = os.path.join(carpeta_figs, "Clasificación de fallos por dispositivo")
    subdir_inactividad = os.path.join(carpeta_figs, "Clasificación por inactividad")
    os.makedirs(subdir_hist, exist_ok=True)
    os.makedirs(subdir_clasif, exist_ok=True)
    os.makedirs(subdir_inactividad, exist_ok=True)

    archivos = sorted(
        [f for f in os.listdir(carpeta_csv) if f.startswith(nombre_script) and f.endswith(".csv")],
        reverse=True
    )

    if not archivos:
        print(f"⚠️ No hay CSVs para {nombre_script}")
        return

    nuevos_generados = 0

    for archivo_csv in archivos:
        nombre_base = archivo_csv.replace(".csv", "")
        ruta_csv = os.path.join(carpeta_csv, archivo_csv)

        ruta_fig1 = os.path.join(subdir_hist, f"{nombre_base}_hist_ratio_mensajes.png")
        ruta_fig2 = os.path.join(subdir_clasif, f"{nombre_base}_clasificacion_fallos.png")
        ruta_fig3 = os.path.join(subdir_inactividad, f"{nombre_base}_inactividad.png")
        ruta_fig4 = os.path.join(subdir_inactividad, f"{nombre_base}_heatmap_top10clientes_disp_.png")

        # Lista de rutas de figuras requeridas
        figuras_requeridas = [ruta_fig1, ruta_fig2, ruta_fig3, ruta_fig4]

        # Si todas las figuras existen, se omite el procesamiento
        if all(os.path.exists(ruta) for ruta in figuras_requeridas):
            print(f"✅ Figuras ya generadas para {archivo_csv}")
            continue


        print(f"📄 Procesando CSV: {archivo_csv}")
        df = pd.read_csv(ruta_csv)

        try:
            partes = archivo_csv.split("_")
            fecha_str = partes[2]
            hora_str = partes[3]
            dt_full = datetime.strptime(f"{fecha_str}_{hora_str}", "%Y-%m-%d_%H-%M")
            fecha_hora_titulo = dt_full.strftime("%d/%m/%Y %H:%M")
        except Exception as e:
            print("⚠️ No se pudo extraer la fecha y hora:", e)
            fecha_hora_titulo = "fecha desconocida"

        if "pct_recibidos_vs_esperados" in df.columns:
            df = df.rename(columns={"pct_recibidos_vs_esperados": "ratio_mensajes"})

         # === Gráfico 1: Histograma (solo ratio_mensajes válidos ≥ 0) ===

        # === Gráfico 1: Histograma (excluyendo ratio_mensajes <= 0) ===

        # Renombrar si viene como 'pct_recibidos_vs_esperados'
        if "pct_recibidos_vs_esperados" in df.columns:
            df = df.rename(columns={"pct_recibidos_vs_esperados": "ratio_mensajes"})

        # Convertir a numérico forzando errores a NaN
        df["ratio_mensajes"] = pd.to_numeric(df["ratio_mensajes"], errors="coerce")

        # Filtrar solo los valores > 0
        df_filtrado = df[df["ratio_mensajes"] > 0].copy()
        total = len(df_filtrado)

        print(f"✅ Total de dispositivos usados en el histograma (ratio_mensajes > 0): {total}")

        # === Conteo por tramos ===
        conteo_tramos = {
            "<40%": df_filtrado[df_filtrado["ratio_mensajes"] < 0.4],
            "40–60%": df_filtrado[(df_filtrado["ratio_mensajes"] >= 0.4) & (df_filtrado["ratio_mensajes"] < 0.6)],
            "60–80%": df_filtrado[(df_filtrado["ratio_mensajes"] >= 0.6) & (df_filtrado["ratio_mensajes"] < 0.8)],
            "80–100%": df_filtrado[(df_filtrado["ratio_mensajes"] >= 0.8) & (df_filtrado["ratio_mensajes"] <= 1.0)],
            ">100%": df_filtrado[df_filtrado["ratio_mensajes"] > 1.0]
        }

        colores_tramos = {
            "<40%": "#d73027",
            "40–60%": "#fc8d59",
            "60–80%": "#fee08b",
            "80–100%": "#1a9850",
            ">100%": "#9e3b9e"
        }

        # === Crear histograma ===
        fig, ax = plt.subplots(figsize=(10, 5))
        n, bins, patches = ax.hist(df_filtrado["ratio_mensajes"], bins=30, edgecolor="black")

        # === Asignar colores por tramo ===
        for patch, left in zip(patches, bins[:-1]):
            if left < 0.4:
                patch.set_facecolor(colores_tramos["<40%"])
            elif left < 0.6:
                patch.set_facecolor(colores_tramos["40–60%"])
            elif left < 0.8:
                patch.set_facecolor(colores_tramos["60–80%"])
            elif left <= 1.0:
                patch.set_facecolor(colores_tramos["80–100%"])
            else:
                patch.set_facecolor(colores_tramos[">100%"])

        # === Leyenda con porcentajes ===
        legend_elements = []
        for tramo, df_tramo in conteo_tramos.items():
            porcentaje = round(len(df_tramo) / total * 100, 1)
            label = f"{tramo} → {len(df_tramo)} dispositivos ({porcentaje}%)"
            legend_elements.append(Patch(facecolor=colores_tramos[tramo], label=label))

        # === Etiquetas y guardado ===
        ax.set_title(f"Distribución del ratio de mensajes recibidos (> 0)\n{total} dispositivos analizados – últimas 24h del {fecha_hora_titulo}")
        ax.set_xlabel("Ratio de mensajes recibidos (recibidos / esperados)")
        ax.set_ylabel("Nº de dispositivos")
        ax.legend(handles=legend_elements, title="Tramos de ratio")
        plt.tight_layout()
        plt.savefig(ruta_fig1)
        plt.clf()



        # === Gráfico 2: Clasificación de fallos (usando clasificacion_conexion) ===

        # Aseguramos que 'visto_ultima_vez' sea datetime tz-naive
        df["visto_ultima_vez"] = (
            pd.to_datetime(df["visto_ultima_vez"], errors="coerce")
            .dt.tz_localize(None)
        )

        # Función de clasificación actualizada
        def clasificar_dispositivo(row):
            kpi       = row.get("ratio_mensajes", None)
            bat       = row.get("porcentaje_bateria", 1)
            ttf       = row.get("media_ttf", 0)
            rein      = row.get("numero_reinicios", 0)
            inact     = row.get("clasificacion_conexion", "")

            # 1) Sin conexión si marca explícita de inactividad 48h
            if inact in ["Conexión 24-48h", "Conexión 48-72h"]:
                return "Sin conexión (>48h)"
            # 2) Baja recepción 0–40%
            if kpi is not None and 0 < kpi < 0.40:
                return "Baja recepción (0–40%)"
            # 3) Sin anomalías
            if (kpi or 0) >= 0.80 and bat >= 0.20 and ttf <= 45 and rein <= 20:
                return "Sin anomalías (≥ 80%)"
            # 4) Batería crítica
            if bat < 0.20:
                return "Batería crítica (< 20%)"
            # 5) GPS lento
            if ttf > 45:
                return "Problemas de posicionamiento GPS (TTF > 45s)"
            # 6) Reinicios frecuentes
            if rein > 20:
                return "Reinicios frecuentes (> 20)"
            # 7) Otros fallos leves
            return "Otros fallos leves"

        # Aplicar clasificación
        df["categoria_fallo"] = df.apply(clasificar_dispositivo, axis=1)

        # Contar por categoría y eliminar vacío
        agrupado = (
            df["categoria_fallo"]
            .value_counts()
            .rename_axis("Categoría")
            .reset_index(name="Nº de dispositivos")
        )
        agrupado = agrupado[agrupado["Nº de dispositivos"] > 0].copy()

        # Añadir porcentaje y etiqueta
        agrupado["%"] = (agrupado["Nº de dispositivos"] / len(df) * 100).round(1)
        agrupado["Etiqueta"] = agrupado.apply(
            lambda r: f'{int(r["Nº de dispositivos"])} dispositivos\n({r["%"]}%)',
            axis=1
        )

        # Orden fijo y mapeo de colores
        orden = [
            "Sin conexión (>48h)",
            "Baja recepción (0–40%)",
            "Sin anomalías (≥ 80%)",
            "Batería crítica (< 20%)",
            "Problemas de posicionamiento GPS (TTF > 45s)",
            "Reinicios frecuentes (> 20)",
            "Otros fallos leves"
        ]
        colores = {
            "Sin conexión (>48h)":       "#999999",
            "Baja recepción (0–40%)":    "#d73027",
            "Sin anomalías (≥ 80%)":     "#1a9850",
            "Batería crítica (< 20%)":   "#fee08b",
            "Problemas de posicionamiento GPS (TTF > 45s)": "#d73027",
            "Reinicios frecuentes (> 20)": "#fdae61",
            "Otros fallos leves":        "#fef0d9"
        }

        agrupado["Categoría"] = pd.Categorical(
            agrupado["Categoría"], categories=orden, ordered=True
        )
        agrupado = agrupado.sort_values("Categoría")

        # Asegurar carpeta de salida de figura
        os.makedirs(os.path.dirname(ruta_fig2), exist_ok=True)

        # Dibujar barplot
        plt.figure(figsize=(14, 6))
        ax = sns.barplot(
            data=agrupado,
            y="Categoría",
            x="Nº de dispositivos",
            palette=[colores.get(c, "#cccccc") for c in agrupado["Categoría"]]
        )

        # Etiquetas al extremo de cada barra
        max_val = agrupado["Nº de dispositivos"].max()
        for bar, (_, row) in zip(ax.patches, agrupado.iterrows()):
            width = bar.get_width()
            y = bar.get_y() + bar.get_height() / 2
            ax.text(
                width + max_val * 0.01,
                y,
                row["Etiqueta"],
                va="center",
                ha="left",
                fontsize=10,
                color="black"
            )

        # Título y ejes
        try:
            fecha_hora_titulo
        except NameError:
            fecha_hora_titulo = "fecha desconocida"
        titulo = (
            f"Clasificación de fallos (conexión 24–72h, baja disponibilidad < 40%)\n"
            f"(n={len(df)}) últimas 24h del {fecha_hora_titulo}"
        )
        ax.set_title(titulo, fontsize=14)
        ax.set_xlabel("Número de dispositivos")
        ax.set_ylabel("Categoría de fallo")
        ax.set_xlim(0, max_val * 1.15)
        plt.tight_layout()
        plt.savefig(ruta_fig2)
        plt.clf()







        # === Gráfico 3: Clasificación por inactividad ===
                # === Gráfico 3: Clasificación por inactividad (bloque completo corregido) ===
        subdir_inact = os.path.join(carpeta_figs, "Clasificación por inactividad")
        os.makedirs(subdir_inact, exist_ok=True)
        ruta_fig3 = os.path.join(subdir_inact, f"{nombre_base}_inactividad.png")

        if "clasificacion_conexion" in df.columns:
            # 1) Contar por categoría
            inact = (
                df["clasificacion_conexion"]
                  .value_counts()
                  .rename_axis("Estado")
                  .reset_index(name="Nº de dispositivos")
            )

            # 2) Añadir porcentaje y etiqueta
            total = len(df)
            inact["%"] = (inact["Nº de dispositivos"] / total * 100).round(1)
            inact["Etiqueta"] = inact.apply(
                lambda row: f"{int(row['Nº de dispositivos'])}\n({row['%']}%)",
                axis=1
            )

            # 3) Definir orden y colores
            orden_estados = [
                "Activo hoy", "Inactivo 48h", "Inactivo 72h", "Inactivo 1 semana",
                "Inactivo 15 días", "Inactivo 1 mes", "Inactivo 3 meses", "Inactivo > 3 meses"
            ]
            colores_inactividad = {
                "Activo hoy": "#1a9850",
                "Inactivo 48h": "#9e3b9e",
                "Inactivo 72h": "#d73027",
                "Inactivo 1 semana": "#fc8d59",
                "Inactivo 15 días": "#fee08b",
                "Inactivo 1 mes": "#d9d9d9",
                "Inactivo 3 meses": "#bdbdbd",
                "Inactivo > 3 meses": "#969696"
            }

            # 4) Filtrar y ordenar
            inact = inact[inact["Estado"].isin(orden_estados)].copy()
            inact["Estado"] = pd.Categorical(inact["Estado"], categories=orden_estados, ordered=True)
            inact = inact.sort_values("Estado").reset_index(drop=True)

            # 5) Construir lista de colores en orden
            palette = [colores_inactividad[e] for e in inact["Estado"]]

            # 6) Dibujar barplot
            plt.figure(figsize=(12, 6))
            ax = sns.barplot(
                data=inact,
                x="Estado",
                y="Nº de dispositivos",
                palette=palette,
                dodge=False
            )

            # 7) Etiquetas encima de cada barra
            max_val = inact["Nº de dispositivos"].max()
            for idx, row in inact.iterrows():
                ax.text(
                    idx,
                    row["Nº de dispositivos"] + max_val * 0.01,
                    row["Etiqueta"],
                    ha="center",
                    va="bottom",
                    fontsize=10
                )

            # 8) Título y ejes
            titulo = (
                f"Clasificación por inactividad de dispositivos\n"
                f"(último mensaje vs hoy – {fecha_hora_titulo})\n"
                f"Total: {total} dispositivos"
            )
            ax.set_title(titulo, fontsize=13)
            ax.set_xlabel("Estado de inactividad")
            ax.set_ylabel("Número de dispositivos")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()

            # 9) Guardar y limpiar
            plt.savefig(ruta_fig3, bbox_inches="tight")
            plt.clf()

            print(f"✅ Gráfico de inactividad generado para: {archivo_csv}")


        # === Gráfico 4: Heatmap top clientes con dispositivos inactivos <72h y su distribución de ratios ===
        # === Gráfico 4: Heatmap - Alertas tempranas por ratio bajo e inactividad reciente ===
        # === Gráfico 4: Heatmap top clientes con dispositivos inactivos 48h y ratios anómalos ===
        from matplotlib.colors import ListedColormap, BoundaryNorm

        from matplotlib.colors import ListedColormap, BoundaryNorm

        # Validar columnas requeridas
        if not {"clasificacion_conexion", "pct_recibidos_vs_esperados", "customer_name"}.issubset(df.columns):
            print(f"⚠️ Saltando heatmap: faltan columnas necesarias en {archivo_csv}")
            continue

        # Crear ratio_mensajes
        df["ratio_mensajes"] = pd.to_numeric(df["pct_recibidos_vs_esperados"], errors="coerce")

        # Si todo OK, seguir
        def tramo_ratio_detallado(r):
            if r < 0.2:
                return "<20%"
            elif r < 0.4:
                return "20–40%"
            elif r < 0.6:
                return "40–60%"
            elif r < 0.8:
                return "60–80%"
            elif r <= 1.0:
                return "80–100%"
            else:
                return ">100%"

            df["tramo_ratio_detallado"] = df["ratio_mensajes"].apply(tramo_ratio_detallado)


            # Top 10 clientes con más dispositivos inactivos 48h o 72h y ratio < 20%
            clientes_top_fallo = (
                df[
                    df["clasificacion_conexion"].isin(["Conexión 24-48h", "Conexión 48-72h"]) &
                    (df["ratio_mensajes"] < 0.2)
                ]
                .groupby("customer_name")
                .size()
                .sort_values(ascending=False)
                .head(10)
                .index
            )

            df_top_clientes = df[df["customer_name"].isin(clientes_top_fallo)].copy()

            tabla = (
                df_top_clientes
                .groupby(["customer_name", "tramo_ratio_detallado"])
                .size()
                .unstack(fill_value=0)
            )

            tabla["Conexión 24-48h"] = df_top_clientes[df_top_clientes["clasificacion_conexion"] == "Conexión 24-48h"].groupby("customer_name").size()
            tabla["Conexión 48-72h"] = df_top_clientes[df_top_clientes["clasificacion_conexion"] == "Conexión 48-72h"].groupby("customer_name").size()


            tabla = tabla.fillna(0).astype(int)

            columnas_orden = ["Conexión 48-72h", "Conexión 24-48h", "<20%", "20–40%", "40–60%", "60–80%", "80–100%", ">100%"]
            for col in columnas_orden:
                if col not in tabla.columns:
                    tabla[col] = 0
            tabla = tabla[columnas_orden]

            

                        # Añadir nº y % de dispositivos inactivos 72h al nombre del cliente
            total_por_cliente = df_top_clientes.groupby("customer_name").size()
            # Reemplaza la línea anterior por este cálculo con todas las categorías >= 72h
            inactivos_72 = df_top_clientes[
                df_top_clientes["clasificacion_conexion"].isin([
                    "Inactivo 1 semana",
                    "Inactivo 15 días",
                    "Inactivo 1 mes",
                    "Inactivo 3 meses",
                    "Inactivo > 3 meses"
                ])
            ].groupby("customer_name").size()

            # Generar nuevo índice con información
            nombres_mejorados = []
            for cliente in tabla.index:
                total = total_por_cliente.get(cliente, 0)
                n_inactivos = inactivos_72.get(cliente, 0)
                porcentaje = round((n_inactivos / total) * 100) if total > 0 else 0
                nombre_nuevo = f"{cliente} (Inact. >72h: {n_inactivos} ud, {porcentaje}%)"
                nombres_mejorados.append(nombre_nuevo)

            # Sustituir índice
            tabla.index = nombres_mejorados

            tabla = tabla.sort_values("Conexión 24-48h", ascending=False)

            cmap = ListedColormap(["#ffffcc", "#c7e9b4", "#7fcdbb", "#41b6c4", "#1d91c0", "#225ea8", "#9e3b9e"])
            boundaries = [0, 1, 21, 41, 61, 81, 101, tabla.values.max() + 1]
            norm = BoundaryNorm(boundaries, cmap.N, clip=True)

            subdir_top = os.path.join(carpeta_figs, "Top Clientes Inactivos y Ratio")
            os.makedirs(subdir_top, exist_ok=True)
            ruta_fig4 = os.path.join(subdir_top, f"{nombre_base}_heatmap_inactivos48h_y_ratios.png")



            plt.figure(figsize=(14, 7))
            sns.heatmap(
                tabla,
                annot=True,
                fmt=".0f",
                cmap=cmap,
                norm=norm,
                cbar_kws={"label": "Nº de dispositivos"}
            )

            # 🔹 Línea divisoria entre inactivos y ratio
            plt.axvline(2, color='gray', linestyle='--', linewidth=1.2)

            plt.title(f"Análisis últimas 24h: Top 10 clientes con dispositivos inactivos (>48h) + ratios anómalos\n{fecha_hora_titulo}", fontsize=13)
            plt.xlabel("Categoría")
            plt.ylabel("Cliente")
            plt.xticks(rotation=45, ha='right')  # Rota etiquetas para que no se pisen
            plt.tight_layout()
            plt.savefig(ruta_fig4, bbox_inches="tight")
            plt.clf()
            print(f"✅ Heatmap generado: {ruta_fig4}")





    if nuevos_generados == 0:
        print(f"✅ Todos los análisis de {nombre_script} están al día. No se generaron nuevas figuras.")


