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

# ==============================
#   Normalización de países
# ==============================
# Alias detectados en tu dataset (-> ISO-2 válidos)
ISO_ALIAS_MAP = {
    "UR": "UY",  # Uruguay
    "CH": "CL",  # CH en tus datos = Chile (si fuese Suiza, usa "CH": "CH")
}

# ISO-3 -> ISO-2 (solo los que te pueden aparecer de tu lista)
ISO3_TO_ISO2 = {
    "ARG": "AR",
    "BOL": "BO",
    "BRA": "BR",
    "CHL": "CL",
    "COL": "CO",
    "DOM": "DO",
    "ECU": "EC",
    "ESP": "ES",
    "HRV": "HR",
    "MNE": "ME",
    "PRI": "PR",
    "ROU": "RO",
    "URY": "UY",
    "VEN": "VE",
}

# Conjuntos de región (ISO-2)
LATAM_ISO2  = {"AR","BO","BR","CL","CO","DO","EC","PR","UY","VE"}
EUROPE_ISO2 = {"ES","HR","ME","RO"}

def normalize_country(code: str) -> str | None:
    """Devuelve ISO-2 o None a partir del valor original (acepta ISO-2/ISO-3 y alias)."""
    if pd.isna(code):
        return None
    c = str(code).strip().upper()
    if not c:
        return None
    # Alias del dataset (p.ej., UR→UY; CH→CL si es Chile)
    if c in ISO_ALIAS_MAP:
        c = ISO_ALIAS_MAP[c]
    # ISO-3 -> ISO-2
    if len(c) == 3 and c in ISO3_TO_ISO2:
        c = ISO3_TO_ISO2[c]
    # Si ya es ISO-2, lo dejamos tal cual; si no, lo marcamos como desconocido (None)
    if len(c) == 2:
        return c
    return None

def infer_region_from_iso2(c_iso2: str | None) -> str:
    """Clasifica en LATAM / Europa / Desconocido (sin 'Otros')."""
    if c_iso2 is None:
        return "Desconocido"
    if c_iso2 in LATAM_ISO2:
        return "LATAM"
    if c_iso2 in EUROPE_ISO2:
        return "Europa"
    return "Desconocido"  # Todo lo demás es desconocido para tu caso de uso

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

    # ===== Normalizar fechas a UTC tz-aware UNA SOLA VEZ =====
    DATE_COLS_UTC = [
        "ultimo_mensaje_recibido",
        "gateway_last_seen",
        "visto_ultima_vez",
        "fecha_cambio_bateria",
        "ultima_posicion_gps_valida",
    ]
    for col in DATE_COLS_UTC:
        if col in df_original.columns:
            df_original[col] = pd.to_datetime(df_original[col], errors="coerce", utc=True)

    # Normalización de Country y región
    if "Country" in df_original.columns:
        df_original["Country_norm"] = df_original["Country"].apply(normalize_country)
        df_original["Region_norm"] = df_original["Country_norm"].apply(infer_region_from_iso2)
        df_original["Region_norm"] = pd.Categorical(
            df_original["Region_norm"],
            categories=["LATAM", "Europa", "Desconocido"],
            ordered=False
        )
    else:
        df_original["Country_norm"] = None
        df_original["Region_norm"] = pd.Categorical(
            ["Desconocido"] * len(df_original),
            categories=["LATAM", "Europa", "Desconocido"],
            ordered=False
        )

    # Clasificaciones temporales
    df_original = aplicar_clasificaciones_temporales(df_original)

    # ===== Re-normalizar fechas tras posibles cambios del paso anterior =====
    for col in DATE_COLS_UTC:
        if col in df_original.columns:
            df_original[col] = pd.to_datetime(df_original[col], errors="coerce", utc=True)

    st.success(f"✅ Datos cargados de: `{nombre_archivo}`")
else:
    st.error("❌ No se encontró ningún archivo CSV procesado.")
    st.stop()

# === Filtros ===
st.markdown("### 🎛️ Filtros de visualización avanzados")
colf1, colf2, colf3, colf4 = st.columns(4)

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

# Solo las 4 opciones solicitadas
region = colf4.selectbox("Región (Country)", ["Todos", "LATAM", "Europa", "Desconocido"], index=0)

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

# Filtro por región (estricto a las 3 categorías)
if region != "Todos":
    df = df[df["Region_norm"].astype(str).str.strip() == region]

# Orden por fecha de último mensaje (forzando UTC por robustez)
if "ultimo_mensaje_recibido" in df.columns:
    df["ultimo_mensaje_recibido"] = pd.to_datetime(df["ultimo_mensaje_recibido"], errors="coerce", utc=True)
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
col4.metric("Batería media (%)", f"{bateria_media:.1f}%" if bateria_media is not None else "N/A")

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

    # Ordenar por fecha (forzando UTC por robustez)
    if "ultimo_mensaje_recibido" in df_filtrado.columns:
        df_filtrado["ultimo_mensaje_recibido"] = pd.to_datetime(df_filtrado["ultimo_mensaje_recibido"], errors="coerce", utc=True)
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
#  # TAB 2  (GANADERÍAS OK base vs AJUSTADA)
# =========================
with tab2:
    st.subheader(f"📈 Análisis Avanzado – {filtro_titulo}")

    if df.empty:
        st.info("No hay datos con los filtros actuales.")
        st.stop()

    # =========================
    # Helpers y columnas base
    # =========================
    df_work = df.copy()

    # (1) Flag dispositivo OK base (≥50% válidas vs esperadas)
    col_pct_valid_vs_exp = "Posición válida vs esperadas (%)"
    if "Dispositivo OK (≥50% válidas vs esperadas)" in df_work.columns:
        df_work["dispositivo_ok_base"] = df_work["Dispositivo OK (≥50% válidas vs esperadas)"].astype(bool)
    elif col_pct_valid_vs_exp in df_work.columns:
        df_work[col_pct_valid_vs_exp] = pd.to_numeric(df_work[col_pct_valid_vs_exp], errors="coerce")
        df_work["dispositivo_ok_base"] = (df_work[col_pct_valid_vs_exp] >= 50).fillna(False)
    else:
        df_work["dispositivo_ok_base"] = False  # fallback conservador

    # (2) Última comunicación y flag "comunicó en 3 días"
    now_utc = pd.Timestamp.now(tz="UTC")  # tz-aware
    if "ultimo_mensaje_recibido" in df_work.columns:
        ts_last = pd.to_datetime(df_work["ultimo_mensaje_recibido"], errors="coerce", utc=True)
        df_work["comunico_3d"] = ((now_utc - ts_last) <= pd.Timedelta(days=3)).fillna(False)
    else:
        df_work["comunico_3d"] = False  # si no hay columna, no podemos acreditar comunicación

    # Boolean de gateways online a nivel fila (solo informativo; NO afecta a OK/NO OK)
    def to_bool(x):
        if isinstance(x, bool):
            return x
        s = str(x).strip().lower()
        return s in {"true", "1", "yes", "y", "si", "sí"}

    if "all_gateways_online" in df_work.columns:
        df_work["all_gateways_online_bool"] = df_work["all_gateways_online"].map(to_bool)
    else:
        df_work["all_gateways_online_bool"] = False

    # =========================
    # Agregado por ganadería (base + ajustado)
    # =========================
    for c in ["ranch_name", "customer_name", "Country", "Region"]:
        if c not in df_work.columns:
            df_work[c] = None

    grp = df_work.groupby("ranch_name", dropna=False)

    def first_non_null(s):
        s2 = s.dropna()
        return s2.iloc[0] if not s2.empty else None

    ranch_status = grp.apply(lambda g: pd.Series({
        "n_dispositivos": int(g["device_id"].nunique()),
        "n_ok_base": int(g["dispositivo_ok_base"].sum()),
        "non_ok_count": int((~g["dispositivo_ok_base"]).sum()),
        "non_ok_comm3d_count": int((~g["dispositivo_ok_base"] & g["comunico_3d"]).sum()),
        "customer_name": first_non_null(g["customer_name"]),
        "Country": first_non_null(g["Country"]),
        "Region": first_non_null(g["Region"]),
        # informativas
        "all_gateways_online": bool(g["all_gateways_online_bool"].fillna(False).all()),
        "ranch_gateway_overall_status": first_non_null(g["ranch_gateway_overall_status"]),
    })).reset_index()

    # Porcentajes base
    ranch_status["pct_ok_base"] = (
        100.0 * ranch_status["n_ok_base"] / ranch_status["n_dispositivos"]
    ).replace([pd.NA, float("inf")], 0).fillna(0)
    ranch_status["ranch_ok_base"] = ranch_status["pct_ok_base"] >= 50.0

    # Regla de ajuste: NO OK base y TODOS los NO OK comunicaron en 3 días
    ranch_status["ajuste_aplicado"] = (
        (~ranch_status["ranch_ok_base"]) &
        (ranch_status["non_ok_count"] > 0) &
        (ranch_status["non_ok_comm3d_count"] == ranch_status["non_ok_count"])
    )

    ranch_status["n_ok_ajustada"] = ranch_status.apply(
        lambda r: r["n_dispositivos"] if r["ajuste_aplicado"] else r["n_ok_base"], axis=1
    )
    ranch_status["pct_ok_ajustada"] = (
        100.0 * ranch_status["n_ok_ajustada"] / ranch_status["n_dispositivos"]
    ).replace([pd.NA, float("inf")], 0).fillna(0)
    ranch_status["ranch_ok_ajustada"] = ranch_status["pct_ok_ajustada"] >= 50.0

    # =========================
    # KPIs y selector de vista
    # =========================
    vista = st.radio(
        "Vista de métrica",
        options=["Base", "Ajustada"],
        index=1,  # por defecto mostramos la ajustada
        horizontal=True
    )

    if vista == "Base":
        ranch_status["ranch_ok_view"] = ranch_status["ranch_ok_base"]
        ranch_status["pct_ok_view"] = ranch_status["pct_ok_base"]
        titulo_view = "ESPERADAS (Base)"
    else:
        ranch_status["ranch_ok_view"] = ranch_status["ranch_ok_ajustada"]
        ranch_status["pct_ok_view"] = ranch_status["pct_ok_ajustada"]
        titulo_view = "AJUSTADA (Regla 3 días aplicada)"

    total_ranch = ranch_status.shape[0]
    n_ok_ranch = int(ranch_status["ranch_ok_view"].sum())
    n_no_ok_ranch = total_ranch - n_ok_ranch

    st.markdown(f"##### KPIs – Vista: **{titulo_view}**")
    colk1, colk2, colk3, colk4 = st.columns(4)
    colk1.metric("Ganaderías (con filtros)", f"{total_ranch:,}")
    colk2.metric("Ganaderías OK", f"{n_ok_ranch:,}", delta=f"{(n_ok_ranch / total_ranch * 100):.1f}%" if total_ranch else "0%")
    colk3.metric("Ganaderías NO OK", f"{n_no_ok_ranch:,}", delta=f"{(n_no_ok_ranch / total_ranch * 100):.1f}%" if total_ranch else "0%")
    promoted = int((~ranch_status["ranch_ok_base"] & ranch_status["ranch_ok_ajustada"]).sum())
    colk4.metric("NO OK → OK por ajuste", f"{promoted:,}")

    st.divider()

    # =========================
    # Visualizaciones
    # =========================
    st.markdown(f"#### % de dispositivos OK por ganadería – {titulo_view}")
    if not ranch_status.empty:
        df_bar = ranch_status.sort_values("pct_ok_view", ascending=True)
        fig = px.bar(
            df_bar,
            x="pct_ok_view",
            y="ranch_name",
            color="ranch_ok_view",
            color_discrete_map={True: "#2ca02c", False: "#d62728"},
            text=df_bar["pct_ok_view"].map(lambda v: f"{v:.1f}%"),
            labels={"pct_ok_view": "% dispositivos OK", "ranch_name": "Ganadería", "ranch_ok_view": "Ganadería OK"},
            height=min(700, 30 * max(6, df_bar.shape[0])),
        )
        fig.update_layout(xaxis_title="% dispositivos OK", yaxis_title=None, bargap=0.25)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay datos de ganaderías para graficar.")

    st.markdown(f"#### Distribución de ganaderías NO OK – {titulo_view}")
    error_counts = (~ranch_status["ranch_ok_view"]).value_counts()
    if error_counts.get(True, 0) == 0:
        st.success("Todas las ganaderías están OK con los filtros actuales.")
    else:
        if vista == "Base":
            breakdown = ranch_status[~ranch_status["ranch_ok_base"]][["ranch_name", "pct_ok_base"]]
            fig = px.histogram(breakdown, x="pct_ok_base", nbins=10, title="Histograma % OK (Base) de las NO OK")
        else:
            breakdown = ranch_status[~ranch_status["ranch_ok_ajustada"]][
                ["ranch_name", "pct_ok_ajustada", "ajuste_aplicado", "non_ok_count", "non_ok_comm3d_count"]
            ]
            fig = px.histogram(breakdown, x="pct_ok_ajustada", nbins=10, title="Histograma % OK (Ajustada) de las NO OK")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # =========================
    # Tablas de trabajo
    # =========================
    st.markdown("#### 📋 Estado de ganaderías (Base vs Ajustada)")
    cols_order = [
        "ranch_name", "customer_name", "Country", "Region",
        "n_dispositivos",
        "n_ok_base", "pct_ok_base", "ranch_ok_base",
        "non_ok_count", "non_ok_comm3d_count", "ajuste_aplicado",
        "n_ok_ajustada", "pct_ok_ajustada", "ranch_ok_ajustada",
        "all_gateways_online", "ranch_gateway_overall_status",  # informativas
    ]
    for c in cols_order:
        if c not in ranch_status.columns:
            ranch_status[c] = None

    st.dataframe(
        ranch_status[cols_order].sort_values(
            ["ranch_ok_ajustada", "pct_ok_ajustada", "pct_ok_base"],
            ascending=[True, False, False]
        ),
        use_container_width=True,
        hide_index=True
    )

    st.download_button(
        "⬇️ Descargar estado de ganaderías (Base vs Ajustada, CSV)",
        ranch_status[cols_order].to_csv(index=False).encode("utf-8"),
        file_name="ranch_status_base_vs_ajustada.csv",
        mime="text/csv"
    )

    st.markdown("—")

    # =========================
    # Foco en ganaderías NO OK en la vista elegida
    # =========================
    st.markdown(f"#### 🔎 Foco en ganaderías NO OK – {titulo_view}")

    if vista == "Base":
        not_ok = ranch_status[~ranch_status["ranch_ok_base"]]
    else:
        not_ok = ranch_status[~ranch_status["ranch_ok_ajustada"]]

    if not not_ok.empty:
        ranch_sel = st.selectbox(
            "Selecciona ganadería para diagnóstico",
            options=not_ok.sort_values("pct_ok_view", ascending=True)["ranch_name"].tolist(),
            index=0
        )

        df_ranch_devices = df_work[df_work["ranch_name"] == ranch_sel].copy()

        c1, c2, c3, c4 = st.columns(4)
        total_dev = df_ranch_devices["device_id"].nunique()
        ok_dev_base = int(df_ranch_devices["dispositivo_ok_base"].sum())
        pct_dev_ok_base = (100.0 * ok_dev_base / total_dev) if total_dev else 0.0
        gw_ok = df_ranch_devices["all_gateways_online_bool"].fillna(False).all()

        # Ajuste a nivel de la ganadería seleccionada
        non_ok = (~df_ranch_devices["dispositivo_ok_base"]).sum()
        non_ok_comm3d = (~df_ranch_devices["dispositivo_ok_base"] & df_ranch_devices["comunico_3d"]).sum()
        ajuste_aplica = (pct_dev_ok_base < 50.0) and (non_ok > 0) and (non_ok == non_ok_comm3d)
        ok_dev_ajustada = total_dev if ajuste_aplica else ok_dev_base
        pct_dev_ok_ajustada = (100.0 * ok_dev_ajustada / total_dev) if total_dev else 0.0

        c1.metric("Dispositivos", f"{total_dev:,}")
        c2.metric("% OK (base)", f"{pct_dev_ok_base:.1f}%")
        c3.metric("% OK (ajustada)", f"{pct_dev_ok_ajustada:.1f}%", delta="+ajuste" if ajuste_aplica else None)
        c4.metric("Antenas online", "Sí" if gw_ok else "No")  # solo informativo

        # Tabla de dispositivos de la ganadería seleccionada
        df_ranch_devices["no_ok_base"] = ~df_ranch_devices["dispositivo_ok_base"]
        df_ranch_devices["comunico_3d"] = df_ranch_devices["comunico_3d"].astype(bool)
        cols_ranch_dev = [
            "device_id", "SerialNumber", "Model",
            "clasificacion_conexion",
            "Mensajes esperados (detallado)", "Mensajes recibidos (n)",
            "Mensaje con posición GPS (n)", "Posición GPS válida (n)",
            "Posición válida vs esperadas (%)",
            "ultimo_mensaje_recibido",
            "no_ok_base", "comunico_3d",
            "porcentaje_bateria",
            "gateway_name", "gateway_serial", "gateway_last_seen",
        ]
        cols_ranch_dev = [c for c in cols_ranch_dev if c in df_ranch_devices.columns]

        st.dataframe(
            df_ranch_devices[cols_ranch_dev].sort_values(
                by=[c for c in ["no_ok_base", "comunico_3d", "Posición válida vs esperadas (%)", "ultimo_mensaje_recibido"] if c in cols_ranch_dev],
                ascending=[False, False, True, False]
            ),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.success("No hay ganaderías NO OK con los filtros actuales en esta vista.")

# =========================
#  # TAB 3 (Opcional futuro)
# =========================
with tab3:
    st.subheader("⚙️ Control")
    st.write("Ajustes y herramientas de administración (próximamente).")
