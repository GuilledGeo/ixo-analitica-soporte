import sys
import os
sys.path.append(os.path.abspath("."))

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

# === Extraer fecha del CSV para título ===
nombre_archivo = os.path.basename(ruta_csv)
try:
    partes = nombre_archivo.replace(".csv", "").split("_")
    fecha = partes[2]
    hora = partes[3].replace("-", ":")
    fecha_hora_formateada = f"{fecha} {hora}"
except Exception:
    fecha_hora_formateada = "Fecha desconocida"

st.title(f"📱Dashboard Soporte consulta últimas 24h: {fecha_hora_formateada}")

if ruta_csv:
    df_original = pd.read_csv(ruta_csv)
    st.success(f"✅ Datos cargados de: `{os.path.basename(ruta_csv)}`")
else:
    st.error("❌ No se encontró ningún archivo CSV procesado.")
    st.stop()

# === Filtros debajo de KPIs ===
st.markdown("### 🎛️ Filtros de visualización avanzados")

colf1, colf2, colf3 = st.columns(3)
cliente = colf1.selectbox("Cliente", ["Todos"] + sorted(df_original["customer_name"].dropna().unique().tolist()), index=0)
modelo = colf2.selectbox("Modelo de dispositivo", ["Todos"] + sorted(df_original["Model"].dropna().unique().tolist()), index=0)
estado = colf3.selectbox("Estado de conexión", ["Todos"] + sorted(df_original["clasificacion_conexion"].dropna().unique().tolist()), index=0)

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

# === KPIs dinámicos ===
st.markdown("### 📌 Indicadores Clave")
col1, col2, col3 = st.columns(3)

total = len(df)
conectados = df[df["clasificacion_conexion"] == "Conectado hoy"].shape[0]
sin_conexion = df[df["clasificacion_conexion"] != "Conectado hoy"].shape[0]
sin_gps = df[df["mensajes_sin_gps"] > 0].shape[0] if "mensajes_sin_gps" in df.columns else 0

col1.metric("Total dispositivos", f"{total:,}")
col2.metric("Conectados hoy", f"{conectados:,}", delta=f"{(conectados/total)*100:.1f}%" if total else "0%")
col3.metric("Sin conexión 24h", f"{sin_conexion:,}", delta=f"{(sin_conexion/total)*100:.1f}%" if total else "0%")

col4, col5, col6 = st.columns(3)

bateria_media = df["porcentaje_bateria"].mean() if "porcentaje_bateria" in df.columns and not df.empty else None
col4.metric("Batería media (%)", f"{bateria_media:.1f}%" if bateria_media else "N/A")

if "pct_recibidos_vs_esperados" in df.columns and not df.empty:
    bajos_ratio = df[df["pct_recibidos_vs_esperados"] < 0.5].shape[0]
    col5.metric("Ratio < 50%", f"{bajos_ratio:,}", delta=f"{(bajos_ratio/total)*100:.1f}%" if total else "0%")
else:
    col5.metric("Ratio < 50%", "N/A")

if "porcentaje_bateria" in df.columns and not df.empty:
    bajos_bateria = df[df["porcentaje_bateria"] < 20].shape[0]
    col6.metric("Batería < 20%", f"{bajos_bateria:,}", delta=f"{(bajos_bateria/total)*100:.1f}%" if total else "0%")
else:
    col6.metric("Batería < 20%", "N/A")

# === Tabs ===
tab1, tab2, tab3 = st.tabs([f"📊 Panel General – {filtro_titulo}", "📈 Análisis Avanzado", "⚙️ Control"])

# === TAB 1 ===
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
            fig = px.bar(df_ranch, x="Ganadería", y="Nº Dispositivos",
                         title=f"Dispositivos por Ganadería – {filtro_titulo}", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "pct_recibidos_vs_esperados" in df.columns:
            fig = px.histogram(df, x="pct_recibidos_vs_esperados", nbins=20,
                               title="Ratio de Mensajes Recibidos (%)")
            st.plotly_chart(fig, use_container_width=True)

        if "porcentaje_bateria" in df.columns:
            fig = px.histogram(df, x="porcentaje_bateria", nbins=20,
                               title="Distribución de Batería (%)")
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("📋 Tabla y Mapa de Dispositivos")

    busqueda = st.text_input("🔎 Buscar por cualquier campo:", "")
    df_filtrado = df.copy()
    if busqueda:
        df_filtrado = df[df.apply(lambda row: busqueda.lower() in str(row).lower(), axis=1)]

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
            import folium
            from streamlit_folium import st_folium
            from folium.plugins import MarkerCluster

            if "lat" in df.columns and "lon" in df.columns:
                df_coords = df_filtrado.dropna(subset=["lat", "lon"]).copy()

                if not df_coords.empty:
                    # Zoom inteligente según número de dispositivos
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

                    # Fondo tipo satélite estilo Google Earth (World Imagery)
                    folium.TileLayer(
                        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                        attr="Esri",
                        name="Satélite (Esri)",
                        overlay=False,
                        control=True
                    ).add_to(m)

                    # Nota: no añadimos el MarkerCluster como capa controlable (no se podrá ocultar)


                    cluster = MarkerCluster().add_to(m)

                    for _, row in df_coords.iterrows():
                        popup = f"""
                        <b>Nº Serie:</b> {row['SerialNumber']}<br>
                        <b>Cliente:</b> {row.get('customer_name', 'N/A')}<br>
                        <b>Ultima posición GPS:</b> {row.get('ultima_posicion_gps_valida', 'N/A')}<br>
                        <b>Estado conexión:</b> {row.get('clasificacion_conexion', 'N/A')}<br>
                        <b>Último mensaje:</b> {row.get('ultimo_mensaje_recibido', 'N/A')}
                        """
                        folium.Marker(
                            location=[row["lat"], row["lon"]],
                            popup=popup
                        ).add_to(cluster)

                    folium.LayerControl(position="topright", collapsed=False).add_to(m)
                    st_folium(m, width=600, height=600)
                else:
                    st.info("No hay coordenadas disponibles para mostrar el mapa.")
            else:
                st.warning("El dataset no contiene columnas `lat` y `lon` necesarias para el mapa.")
