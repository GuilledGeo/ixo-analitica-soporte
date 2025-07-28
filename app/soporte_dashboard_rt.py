import sys
import os
sys.path.append(os.path.abspath("."))

import streamlit as st
import pandas as pd
import plotly.express as px
import time
from datetime import datetime, timedelta

import requests

from src.features.consulta_1 import aplicar_clasificaciones_temporales

# === Configuración general ===
st.set_page_config(layout="wide", page_title="📱 Dashboard Soporte Ixorigué - Dispositivos")

# === Refresco automático cada 5 minutos (solo si no es la primera carga)
REFRESH_INTERVAL = 300
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()
if 'primera_carga' not in st.session_state:
    st.session_state.primera_carga = True
else:
    tiempo_desde_ultima = time.time() - st.session_state.last_refresh
    if tiempo_desde_ultima > REFRESH_INTERVAL and not st.session_state.primera_carga:
        st.session_state.last_refresh = time.time()
        st.experimental_rerun()

# === CONTENEDORES TEMPORALES ===
placeholder_bienvenida = st.empty()
placeholder_subtitulo = st.empty()
placeholder_barra = st.empty()
placeholder_footer = st.empty()

# === Mostrar solo si es primera vez
if st.session_state.primera_carga:
    placeholder_bienvenida.title("📱 Bienvenido al Panel de Control de Soporte – Ixorigué")
    placeholder_subtitulo.markdown("Los datos están siendo consultados desde la API en vivo. Por favor, espera...")
    placeholder_footer.markdown("<small>Desarrollado por Guillermo Durántez – Ixorigué</small>", unsafe_allow_html=True)

# === BLOQUE DE CARGA CON DEPURACIÓN ===
with st.spinner("⏳ Solicitando datos a la API..."):
    tiempo_inicio = time.time()
    barra_carga = placeholder_barra.progress(0, text="Cargando datos...")

    @st.cache_data(ttl=300)
    def cargar_datos():
        url = "https://ixo-dash-soporte.onrender.com/consulta_01"
        st.write(f"🔗 Consultando datos desde: {url}")
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.DataFrame(response.json())
            st.write("📊 Datos recibidos correctamente. Filas obtenidas:", len(df))
            return aplicar_clasificaciones_temporales(df)
        else:
            raise ValueError(f"❌ Error en la API ({response.status_code}): {response.text}")

    df_original = None
    intentos = 0
    max_intentos = 3

    while df_original is None and intentos < max_intentos:
        try:
            df_original = cargar_datos()
        except Exception as e:
            st.warning(f"⚠️ Intento {intentos + 1} fallido: {e}")
            print(f"❌ Error intento {intentos + 1}: {e}")
            intentos += 1
            time.sleep(2)

    if df_original is None:
        st.error("❌ No se pudo cargar la información tras varios intentos.")
        st.stop()

    tiempo_fin = time.time()
    duracion_segundos = round(tiempo_fin - tiempo_inicio, 2)

    for i in range(101):
        barra_carga.progress(i, text=f"Cargando datos... {i}%")
        time.sleep(duracion_segundos / 100)

# === Limpiar contenido temporal solo tras primera carga
placeholder_bienvenida.empty()
placeholder_subtitulo.empty()
placeholder_barra.empty()
placeholder_footer.empty()
st.session_state.primera_carga = False  # Ya no es primera vez

# === Confirmación de actualización ===
st.success(f"✅ Datos actualizados: {datetime.now().strftime('%d/%m/%Y %H:%M')} (⏱️ {duracion_segundos:.2f} segundos)")

# === Advertencia si tarda demasiado
if duracion_segundos >= 60:
    st.warning(
        "⚠️ Esta consulta ha tardado más de 1 minuto en completarse. "
        "Esto puede deberse a latencia o problemas de conexión con la API.\n\n"
        "Si este problema persiste, contacta con Guillermo (guillermo@ixorigue.com)."
    )


# === Título principal (después de carga) ===

st.title(f"📡 Dashboard Soporte Ixorigue – Consulta 24h ({(datetime.now() + timedelta(hours=2)).strftime('%d/%m/%Y %H:%M')})")

# === Filtros ===
st.markdown("### 🎛️ Filtros de visualización avanzados")
colf1, colf2, colf3 = st.columns(3)
# Asegurarse de que todos los valores sean cadenas válidas
clientes_unicos = df_original["customer_name"].dropna().astype(str).unique().tolist()
clientes_ordenados = sorted(clientes_unicos)
cliente = colf1.selectbox("Cliente", ["Todos"] + clientes_ordenados, index=0)
modelo = colf2.selectbox("Modelo de dispositivo", ["Todos"] + sorted(df_original["Model"].dropna().unique().tolist()), index=0)

orden_personalizado = [
    "Conectado hoy", "Conexión 24-48h", "Conexión 48-72h",
    "Conexión 3-7 días", "Conexión 7-15 días", "Conexión 15 días - 1 mes",
    "Conexión 1-3 meses", "Conexión >3  meses"
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

    # Ordenar por fecha en la tabla también
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
st.markdown("---")
st.markdown(
    "<div style='text-align: center; font-size: 0.9em; color: gray;'>"
    "👨‍💻 Responsable del dashboard: Guillermo Durántez – "
    "<a href='mailto:guillermo@ixorigue.com'>guillermo@ixorigue.com</a>"
    "</div>",
    unsafe_allow_html=True
)
