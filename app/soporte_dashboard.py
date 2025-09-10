import sys
import os
sys.path.append(os.path.abspath("."))

import folium
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from src.features.consulta_1 import aplicar_clasificaciones_temporales

st.set_page_config(layout="wide", page_title="📱 Dashboard Soporte - Dispositivos")

# === Cargar CSV más reciente ===
CARPETA = "data/processed"
PREFIJO = "consulta_01"

def encontrar_csv_reciente(prefijo, carpeta):
    archivos = [f for f in os.listdir(carpeta) if f.startswith(prefijo) and f.endswith(".csv")]
    if not archivos:
        return None
    archivos.sort(reverse=True)
    return os.path.join(carpeta, archivos[0])

ruta_csv = encontrar_csv_reciente(PREFIJO, CARPETA)

if ruta_csv:
    nombre_archivo = os.path.basename(ruta_csv)
    try:
        partes = nombre_archivo.replace(".csv", "").split("_")
        fecha = partes[2]
        hora = partes[3].replace("-", ":")
        fecha_hora_formateada = f"{fecha} {hora}"
    except Exception:
        fecha_hora_formateada = "Fecha desconocida"

    st.title(f"📱Dashboard Soporte consulta últimas 24h: {fecha_hora_formateada}")
    df_original = pd.read_csv(ruta_csv)
    df_original = aplicar_clasificaciones_temporales(df_original)
    st.success(f"✅ Datos cargados de: `{nombre_archivo}`")
else:
    st.error("❌ No se encontró ningún archivo CSV procesado.")
    st.stop()

# === Filtros ===
st.markdown("### 🎛️ Filtros de visualización avanzados")
colf1, colf2, colf3 = st.columns(3)

cliente = colf1.selectbox("Cliente", ["Todos"] + sorted(df_original["customer_name"].dropna().unique().tolist()), index=0)
modelo = colf2.selectbox("Modelo de dispositivo", ["Todos"] + sorted(df_original["Model"].dropna().unique().tolist()), index=0)

orden_personalizado = [
    "Conectado hoy", "Conexión 24-48h", "Conexión 48-72h",
    "Conexión 3-7 días", "Conexión 7-15 días",
    "Conexión 15 días - 1 mes", "Conexión 1-3 meses", "Conexión >3  meses"
]
estados_disponibles = df_original["clasificacion_conexion"].dropna().unique().tolist()
estados_ordenados = [estado for estado in orden_personalizado if estado in estados_disponibles]
estado = colf3.selectbox("Estado de conexión", ["Todos"] + estados_ordenados, index=0)

# === Aplicar filtros ===
df = df_original.copy()
filtro_titulo = "Todos los clientes"
if cliente != "Todos":
    df = df[df["customer_name"] == cliente]
    filtro_titulo = cliente
if modelo != "Todos":
    df = df[df["Model"] == modelo]
if estado != "Todos":
    df = df[df["clasificacion_conexion"] == estado]

# Ordenar por fecha
if "ultimo_mensaje_recibido" in df.columns:
    df["ultimo_mensaje_recibido"] = pd.to_datetime(df["ultimo_mensaje_recibido"], errors="coerce")
    df = df.sort_values(by="ultimo_mensaje_recibido", ascending=False)

# === KPIs ===
st.markdown("### 📌 Indicadores Clave")
col1, col2, col3 = st.columns(3)

total = len(df)
conectados = df[df["clasificacion_conexion"] == "Conectado hoy"].shape[0]
sin_conexion = df[df["clasificacion_conexion"] != "Conectado hoy"].shape[0]
sin_gps = df[df["mensajes_sin_gps"] > 0].shape[0] if "mensajes_sin_gps" in df.columns else 0

col1.metric("Total dispositivos", f"{total:,}")
col2.metric("Conectados hoy", f"{conectados:,}", delta=f"{(conectados/total)*100:.1f}%" if total else "0%")
col3.metric("Sin conexión", f"{sin_conexion:,}", delta=f"{(sin_conexion/total)*100:.1f}%" if total else "0%")

col4, col5, col6 = st.columns(3)
bateria_media = df["porcentaje_bateria"].mean() if "porcentaje_bateria" in df.columns and not df.empty else None
col4.metric("Batería media (%)", f"{bateria_media:.1f}%" if bateria_media else "N/A")

if "pct_recibidos_vs_esperados" in df.columns and not df.empty:
    bajos_ratio = df[(df["pct_recibidos_vs_esperados"] > 0) & (df["pct_recibidos_vs_esperados"] < 0.25)].shape[0]
    col5.metric("Ratio < 25%", f"{bajos_ratio:,}", delta=f"{(bajos_ratio/total)*100:.1f}%" if total else "0%")
else:
    col5.metric("Ratio < 25%", "N/A")

if "porcentaje_bateria" in df.columns and not df.empty:
    bajos_bateria = df[(df["porcentaje_bateria"] > 0) & (df["porcentaje_bateria"] < 20)].shape[0]
    col6.metric("Batería < 20%", f"{bajos_bateria:,}", delta=f"{(bajos_bateria/total)*100:.1f}%" if total else "0%")
else:
    col6.metric("Batería < 20%", "N/A")

# === Tabs ===
tab1, tab2, tab3 = st.tabs([f"📊 Panel General – {filtro_titulo}", "📈 Análisis Avanzado", "⚙️ Control"])

# =========================
#  # TAB 1
# =========================
with tab1:
    st.subheader(f"📊 Panel de Control – {filtro_titulo}")

    col1, col2 = st.columns(2)
    with col1:
        if "clasificacion_conexion" in df.columns:
            fig = px.pie(df, names="clasificacion_conexion", title=f"Distribución por Estado – {filtro_titulo}")
            st.plotly_chart(fig, use_container_width=True)

        if "ranch_name" in df.columns:
            df_ranch = df.groupby("ranch_name")["device_id"].nunique().reset_index()
            df_ranch.columns = ["Ganadería", "Nº Dispositivos"]
            fig = px.bar(df_ranch, x="Ganadería", y="Nº Dispositivos", title=f"Dispositivos por Ganadería – {filtro_titulo}", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "pct_recibidos_vs_esperados" in df.columns:
            fig = px.histogram(df, x="pct_recibidos_vs_esperados", nbins=20, title="Ratio de Mensajes Recibidos (%)")
            st.plotly_chart(fig, use_container_width=True)

        if "porcentaje_bateria" in df.columns:
            fig = px.histogram(df, x="porcentaje_bateria", nbins=20, title="Distribución de Batería (%)")
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("📋 Tabla y Mapa de Dispositivos")

    busqueda = st.text_input("🔎 Buscar por cualquier campo:", "")
    df_filtrado = df.copy()
    if busqueda:
        df_filtrado = df[df.apply(lambda row: busqueda.lower() in str(row).lower(), axis=1)]

    if "ultimo_mensaje_recibido" in df_filtrado.columns:
        df_filtrado["ultimo_mensaje_recibido"] = pd.to_datetime(df_filtrado["ultimo_mensaje_recibido"], errors="coerce")
        df_filtrado = df_filtrado.sort_values(by="ultimo_mensaje_recibido", ascending=False)

    if cliente == "Todos":
        st.markdown("#### 📋 Tabla de dispositivos (vista completa)")
        st.data_editor(
            df_filtrado,
            use_container_width=True,
            height=700,
            column_config={
                "porcentaje_bateria": st.column_config.ProgressColumn("Batería (%)", format="%.1f"),
                "pct_recibidos_vs_esperados": st.column_config.ProgressColumn("Ratio mensajes", format="%.2f")
            },
            hide_index=True
        )
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 📋 Tabla de dispositivos")
            st.data_editor(
                df_filtrado,
                use_container_width=True,
                height=600,
                column_config={
                    "porcentaje_bateria": st.column_config.ProgressColumn("Batería (%)", format="%.1f"),
                    "pct_recibidos_vs_esperados": st.column_config.ProgressColumn("Ratio mensajes", format="%.2f")
                },
                hide_index=True
            )

        with col2:
            st.markdown("#### 🗺️ Mapa última posición GPS")
            if "lat" in df.columns and "lon" in df.columns:
                df_coords = df_filtrado.dropna(subset=["lat", "lon"]).copy()
                if not df_coords.empty:
                    if len(df_coords) == 1:
                        zoom_location = [df_coords.iloc[0]["lat"], df_coords.iloc[0]["lon"]]
                        zoom_level = 14
                    elif len(df_coords) < 5:
                        zoom_location = [df_coords["lat"].mean(), df_coords["lon"].mean()]
                        zoom_level = 12
                    else:
                        zoom_location = [df_coords["lat"].mean(), df_coords["lon"].mean()]
                        zoom_level = 8

                    m = folium.Map(location=zoom_location, zoom_start=zoom_level, tiles="OpenStreetMap")
                    folium.TileLayer(
                        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                        attr="Esri",
                        name="Satélite (Esri)",
                        overlay=False,
                        control=True
                    ).add_to(m)

                    cluster = MarkerCluster().add_to(m)
                    for _, row in df_coords.iterrows():
                        popup = f"""
                        <b>Nº Serie:</b> {row.get('SerialNumber', 'N/A')}<br>
                        <b>Cliente:</b> {row.get('customer_name', 'N/A')}<br>
                        <b>Ultima posición GPS:</b> {row.get('ultima_posicion_gps_valida', 'N/A')}<br>
                        <b>Estado conexión:</b> {row.get('clasificacion_conexion', 'N/A')}<br>
                        <b>Último mensaje:</b> {row.get('ultimo_mensaje_recibido', 'N/A')}
                        """
                        folium.Marker(location=[row["lat"], row["lon"]], popup=popup).add_to(cluster)

                    folium.LayerControl(position="topright", collapsed=False).add_to(m)
                    st_folium(m, width=600, height=600)
                else:
                    st.info("No hay coordenadas disponibles para mostrar el mapa.")
            else:
                st.warning("El dataset no contiene columnas `lat` y `lon` necesarias para el mapa.")

# =========================
# TAB 2
# =========================

with tab2:
    # =========================
    # SUB-TABS
    # =========================
    sub1, sub2 = st.tabs(["🧬 Esquema del dataset", "🏥 Salud de ganaderías <24h"])

    # ==========================================================
    # SUB-TAB 1) ESQUEMA DEL DATASET (índice, columna, tipo)
    # ==========================================================
    with sub1:
        st.subheader("🧬 Esquema del dataset")
        if df.empty:
            st.info("No hay datos tras los filtros aplicados.")
        else:
            # Dtypes + nulos
            schema_df = (
                pd.DataFrame({
                    "columna": df.columns,
                    "tipo_dato": [str(t) for t in df.dtypes.values],
                    "n_nulos": df.isna().sum().values
                })
                .reset_index()
                .rename(columns={"index": "indice"})
            )

            # Reordenar como tu ejemplo (indice, columna, tipo_dato)
            schema_df = schema_df[["indice", "columna", "tipo_dato", "n_nulos"]]

            st.markdown("Vista general de columnas y tipos (incluye nulos):")
            st.data_editor(
                schema_df,
                hide_index=True,
                use_container_width=True,
                height=420
            )

            # Extras útiles
            st.markdown("**Resumen rápido**")
            c1, c2, c3 = st.columns(3)
            c1.metric("Filas", f"{len(df):,}")
            c2.metric("Columnas", f"{df.shape[1]:,}")
            c3.metric("Nº columnas con nulos", f"{(schema_df['n_nulos']>0).sum():,}")

            # Descarga CSV del esquema
            st.download_button(
                "⬇️ Descargar esquema (CSV)",
                data=schema_df.to_csv(index=False).encode("utf-8"),
                file_name="esquema_dataset.csv",
                mime="text/csv"
            )

    # ==========================================================
    # SUB-TAB 2) SALUD DE GANADERÍAS <24h (OK / Errores)
    # ==========================================================
    with sub2:
        st.subheader("🏥 Salud de ganaderías en las últimas 24h")

        # Columnas requeridas mínimas
        col_ranch = "ranch_name"
        col_device = "device_id"
        col_device_ok = "Dispositivo OK (>60% válidas vs esperadas)"
        col_pct_valid_vs_exp = "Posición válida vs esperadas (%)"

        # Comprobar columnas
        faltantes = [c for c in [col_ranch, col_device, col_device_ok] if c not in df.columns]
        if faltantes:
            st.warning(
                "No puedo evaluar completamente la salud de ganaderías porque faltan estas columnas: "
                + ", ".join([f"`{c}`" for c in faltantes])
            )
            st.stop()

        # Antena (no disponible todavía)
        st.info("🔌 **Estado de antena**: *no evaluado en esta versión* (no hay columna de antena online/offline).")

        # --- Agregación por ganadería ---
        agr = (
            df.groupby(col_ranch)
              .agg(
                  n_dispositivos=(col_device, "nunique"),
                  n_ok=(col_device_ok, "sum")
              )
              .reset_index()
        )
        agr["pct_ok"] = (agr["n_ok"] / agr["n_dispositivos"]).fillna(0.0)

        # Regla de negocio: ganadería OK si pct_ok >= 70%
        umbral_ranch_ok = 0.70
        agr["ganaderia_ok"] = agr["pct_ok"] >= umbral_ranch_ok

        # KPIs
        total_g = len(agr)
        g_ok = int(agr["ganaderia_ok"].sum())
        g_err = total_g - g_ok
        pct_g_ok = (g_ok / total_g * 100) if total_g else 0.0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Ganaderías monitorizadas", f"{total_g:,}")
        c2.metric("Ganaderías OK", f"{g_ok:,}", delta=f"{pct_g_ok:.1f}%")
        c3.metric("Ganaderías con errores", f"{g_err:,}", delta=f"{100 - pct_g_ok:.1f}%")
        # Dispositivos OK global
        n_dev_total = df[col_device].nunique()
        n_dev_ok = int(df[df[col_device_ok]][col_device].nunique())
        c4.metric("Dispositivos OK (global)", f"{n_dev_ok:,}/{n_dev_total:,}")

        st.divider()

        # --- Gráficas principales ---
        gcol1, gcol2 = st.columns(2)

        with gcol1:
            # Barras % OK por ganadería (orden ascendente)
            if not agr.empty:
                agr_plot = agr.sort_values("pct_ok", ascending=True).copy()
                fig = px.bar(
                    agr_plot,
                    x="pct_ok",
                    y=col_ranch,
                    orientation="h",
                    text=(agr_plot["pct_ok"] * 100).round(1).astype(str) + "%",
                    title="📊 % de dispositivos OK por ganadería (<24h)",
                )
                fig.update_layout(
                    xaxis_title="% dispositivos OK",
                    yaxis_title="Ganadería",
                    margin=dict(l=10, r=10, t=40, b=10),
                    height=520
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hay datos para graficar % OK por ganadería.")

        with gcol2:
            # Tarta Ganaderías OK vs Error
            if total_g > 0:
                pie_df = pd.DataFrame({
                    "estado": ["OK", "Con error"],
                    "n": [g_ok, g_err]
                })
                fig = px.pie(pie_df, names="estado", values="n", title="🧩 Ganaderías: OK vs Con error")
                st.plotly_chart(fig, use_container_width=True)

            # Histograma (opcional) distribución % válida vs esperadas a nivel dispositivo
            if col_pct_valid_vs_exp in df.columns:
                fig = px.histogram(
                    df,
                    x=col_pct_valid_vs_exp,
                    nbins=25,
                    title="Distribución dispositivos: Posición válida vs esperadas (%)"
                )
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # --- Clasificación de errores (limitado por columnas disponibles) ---
        st.markdown("### 🧯 Clasificación de ganaderías con error (reglas aplicadas)")
        st.caption(
            "- **Error 1**: Antena no conectada → *no evaluado (sin columna de antena)*\n"
            "- **Error 2**: Dispositivos con <60% válidas vs esperadas → listamos dispositivos afectados\n"
            "- **Error 3**: Ganadería con <70% de dispositivos OK"
        )

        # Error 3: ganaderías con <70% OK
        err3_df = agr[~agr["ganaderia_ok"]].copy()
        if not err3_df.empty:
            err3_df_display = err3_df[[col_ranch, "n_dispositivos", "n_ok", "pct_ok"]].copy()
            err3_df_display["pct_ok"] = (err3_df_display["pct_ok"] * 100).round(1)
            err3_df_display = err3_df_display.rename(columns={
                col_ranch: "Ganadería",
                "n_dispositivos": "Nº dispositivos",
                "n_ok": "Nº OK",
                "pct_ok": "% OK"
            })
            st.markdown("#### 🔴 Error 3: Ganaderías con baja proporción de dispositivos OK (<70%)")
            st.data_editor(
                err3_df_display,
                hide_index=True,
                use_container_width=True,
                height=320
            )
        else:
            st.success("✅ No hay ganaderías con Error 3 (<70% dispositivos OK).")

        # Error 2: dispositivos con <60% válidas vs esperadas (listado por ganadería)
        st.markdown("#### 🟠 Error 2: Dispositivos con <60% de posiciones válidas esperadas")
        dev_fail = df[~df[col_device_ok]].copy()
        if not dev_fail.empty:
            cols_show = [c for c in [
                col_ranch, "customer_name", col_device, "SerialNumber",
                col_pct_valid_vs_exp, "pct_recibidos_vs_esperados",
                "mensajes_esperados", "mensajes_recibidos", "mensajes_sin_gps",
                "Model", "Region", "Country"
            ] if c in dev_fail.columns]

            # Ordenar para lectura (peor primero)
            if col_pct_valid_vs_exp in cols_show:
                dev_fail = dev_fail.sort_values(col_pct_valid_vs_exp, ascending=True)

            st.data_editor(
                dev_fail[cols_show],
                hide_index=True,
                use_container_width=True,
                height=420,
                column_config={
                    col_pct_valid_vs_exp: st.column_config.ProgressColumn(
                        "Posición válida vs esperadas (%)", format="%.1f"
                    ),
                    "pct_recibidos_vs_esperados": st.column_config.ProgressColumn(
                        "Ratio recibidos vs esperados (%)", format="%.2f"
                    )
                }
            )

            # Resumen por ganadería (cuántos fallan)
            resumen_fail = (dev_fail.groupby(col_ranch)[col_device]
                                     .nunique().reset_index()
                                     .rename(columns={col_ranch: "Ganadería", col_device: "Dispositivos <60%"}))
            st.markdown("##### Resumen por ganadería (dispositivos con <60%)")
            st.data_editor(
                resumen_fail.sort_values("Dispositivos <60%", ascending=False),
                hide_index=True,
                use_container_width=True,
                height=260
            )
        else:
            st.success("✅ No hay dispositivos con <60% de posiciones válidas vs esperadas.")

        st.divider()

        # --- Tabla general de estado por ganadería (OK/Error) ---
        st.markdown("### 🗂️ Estado por ganadería (resumen)")
        resumen = agr.copy()
        resumen["% OK"] = (resumen["pct_ok"] * 100).round(1)
        resumen["Estado"] = resumen["ganaderia_ok"].map({True: "OK", False: "Con error"})
        resumen = resumen.rename(columns={
            col_ranch: "Ganadería",
            "n_dispositivos": "Nº dispositivos",
            "n_ok": "Nº OK"
        })[["Ganadería", "Nº dispositivos", "Nº OK", "% OK", "Estado"]].sort_values("% OK", ascending=False)

        st.data_editor(
            resumen,
            hide_index=True,
            use_container_width=True,
            height=340
        )

        # Botón de descarga
        st.download_button(
            "⬇️ Descargar estado por ganadería (CSV)",
            data=resumen.to_csv(index=False).encode("utf-8"),
            file_name="estado_ganaderias_24h.csv",
            mime="text/csv"
        )


st.markdown("---")
st.markdown(
    "<div style='text-align: center; font-size: 0.9em; color: gray;'>"
    "👨‍💻 Responsable del dashboard: Guillermo Durántez – "
    "<a href='mailto:guillermo@ixorigue.com'>guillermo@ixorigue.com</a>"
    "</div>",
    unsafe_allow_html=True
)
