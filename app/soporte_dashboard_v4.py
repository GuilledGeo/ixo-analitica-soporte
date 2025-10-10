# app/soporte_dashboard_v4.py
# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.abspath("."))

import folium
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

from src.features.consulta_1 import aplicar_clasificaciones_temporales

# ==============================
# Config básica
# ==============================
st.set_page_config(layout="wide", page_title="📱 Dashboard Soporte - Dispositivos")

# ========= helper rerun compatible =========
def _rerun():
    # Streamlit >=1.27 usa st.rerun()
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        # fallback por si hay versiones antiguas
        try:
            st.experimental_rerun()  # type: ignore[attr-defined]
        except Exception:
            pass

# ==============================
# Normalización de países (solo Country, no afecta a clasificacion_conexion)
# ==============================
ISO_ALIAS_MAP = {"UR": "UY", "CH": "CL"}
ISO3_TO_ISO2 = {
    "ARG":"AR","BOL":"BO","BRA":"BR","CHL":"CL","COL":"CO","DOM":"DO","ECU":"EC",
    "ESP":"ES","HRV":"HR","MNE":"ME","PRI":"PR","ROU":"RO","URY":"UY","VEN":"VE",
}
LATAM_ISO2  = {"AR","BO","BR","CL","CO","DO","EC","PR","UY","VE"}
EUROPE_ISO2 = {"ES","HR","ME","RO"}

def normalize_country(code: str) -> str | None:
    if pd.isna(code): return None
    c = str(code).strip().upper()
    if not c: return None
    if c in ISO_ALIAS_MAP: c = ISO_ALIAS_MAP[c]
    if len(c) == 3 and c in ISO3_TO_ISO2: c = ISO3_TO_ISO2[c]
    return c if len(c) == 2 else None

def infer_region_from_iso2(c_iso2: str | None) -> str:
    if c_iso2 is None: return "Desconocido"
    if c_iso2 in LATAM_ISO2: return "LATAM"
    if c_iso2 in EUROPE_ISO2: return "Europa"
    return "Desconocido"

# ==============================
# Carga CSV reciente
# ==============================
@st.cache_data(show_spinner=True)
def encontrar_csv_reciente(prefijo: str, carpeta: str) -> str | None:
    try:
        archivos = [f for f in os.listdir(carpeta) if f.startswith(prefijo) and f.endswith(".csv")]
        if not archivos: return None
        archivos.sort(reverse=True)
        return os.path.join(carpeta, archivos[0])
    except Exception:
        return None

@st.cache_data(show_spinner=True)
def cargar_desde_csv() -> tuple[pd.DataFrame, str, str]:
    CARPETA = "data/processed"
    PREFIJO = "consulta_01"
    ruta_csv = encontrar_csv_reciente(PREFIJO, CARPETA)
    if not ruta_csv:
        raise RuntimeError("No se encontró ningún archivo CSV procesado en data/processed.")
    df = pd.read_csv(ruta_csv)
    nombre_archivo = os.path.basename(ruta_csv)
    try:
        partes = nombre_archivo.replace(".csv", "").split("_")
        fecha = partes[2]
        hora = partes[3].replace("-", ":")
        fecha_hora_formateada = f"{fecha} {hora}"
    except Exception:
        fecha_hora_formateada = "Fecha desconocida"
    return df, nombre_archivo, fecha_hora_formateada

# ==============================
# Helpers (no tocan 'Conectado hoy')
# ==============================
def to_bool(x):
    if isinstance(x, bool): return x
    s = str(x).strip().lower()
    return s in {"true","1","yes","y","si","sí"}

def count_ratio_below_25(series: pd.Series) -> int:
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty: return 0
    p = s.dropna().quantile(0.99)
    if p <= 1.5:
        return ((s > 0) & (s < 0.25)).sum()
    else:
        return ((s > 0) & (s < 25)).sum()

# ==============================
# Cargar datos
# ==============================
try:
    df_original, nombre_archivo, fecha_hora_formateada = cargar_desde_csv()
except Exception as e:
    st.error(f"❌ Error al cargar CSV: {e}")
    st.stop()

st.title(f"📱Dashboard Soporte consulta últimas 24h: {fecha_hora_formateada}")
st.success(f"✅ Datos cargados de: `{nombre_archivo}`")

DATE_COLS_UTC = [
    "ultimo_mensaje_recibido", "gateway_last_seen", "visto_ultima_vez",
    "fecha_cambio_bateria", "ultima_posicion_gps_valida"
]
for col in DATE_COLS_UTC:
    if col in df_original.columns:
        df_original[col] = pd.to_datetime(df_original[col], errors="coerce", utc=True)

# País / región
if "Country" in df_original.columns:
    df_original["Country_norm"] = df_original["Country"].apply(normalize_country)
    df_original["Region_norm"] = df_original["Country_norm"].apply(infer_region_from_iso2)
    df_original["Region_norm"] = pd.Categorical(df_original["Region_norm"], categories=["LATAM","Europa","Desconocido"])
else:
    df_original["Country_norm"] = None
    df_original["Region_norm"] = pd.Categorical(["Desconocido"]*len(df_original), categories=["LATAM","Europa","Desconocido"])

# Enriquecimiento temporal (NO tocamos la etiqueta resultante)
df_original = aplicar_clasificaciones_temporales(df_original)

# ==============================
# Filtros
# ==============================
st.markdown("### 🎛️ Filtros de visualización avanzados")
colf1, colf2, colf3, colf4 = st.columns(4)

# Cliente (select único)
cliente = colf1.selectbox(
    "Cliente",
    ["Todos"] + sorted(df_original["customer_name"].dropna().unique().tolist()),
    index=0
)

# Modelo -> MULTISELECT
modelos_presentes = sorted(df_original["Model"].dropna().astype(str).unique().tolist()) \
    if "Model" in df_original.columns else []
modelos_multi = colf2.multiselect(
    "Modelos de dispositivo (multi)",
    options=modelos_presentes,
    default=[]
)

# Estado de conexión -> MULTISELECT, tal cual viene en datos
if "clasificacion_conexion" in df_original.columns:
    estados_presentes = sorted(pd.Series(df_original["clasificacion_conexion"]
                                         .dropna().astype(str).unique()).tolist())
else:
    estados_presentes = []
estado_multi = colf3.multiselect(
    "Estado de conexión (multi)",
    options=estados_presentes,
    default=[]
)

# Región -> MULTISELECT
regiones_presentes = ["LATAM", "Europa", "Desconocido"]
regiones_multi = colf4.multiselect(
    "Región (multi)",
    options=regiones_presentes,
    default=[]
)

# ---- Aplicar filtros ----
df = df_original.copy()
filtro_titulo = "Todos los clientes"
if cliente != "Todos":
    df = df[df["customer_name"] == cliente]
    filtro_titulo = cliente

# Solo filtra si hay selección; si no, muestra todo
if modelos_multi and "Model" in df.columns:
    df = df[df["Model"].astype(str).isin(modelos_multi)]

if estado_multi and "clasificacion_conexion" in df.columns:
    df = df[df["clasificacion_conexion"].astype(str).isin(estado_multi)]

if regiones_multi and "Region_norm" in df.columns:
    df = df[df["Region_norm"].astype(str).isin(regiones_multi)]

if "ultimo_mensaje_recibido" in df.columns:
    df = df.sort_values(by="ultimo_mensaje_recibido", ascending=False)

# ==============================
# KPIs (sin recalcular 'Conectado hoy')
# ==============================
st.markdown("### 📌 Indicadores Clave")
col1, col2, col3 = st.columns(3)
total = len(df)
conectados = df[df["clasificacion_conexion"].astype(str) == "Conectado hoy"].shape[0] if "clasificacion_conexion" in df.columns else 0
sin_conexion = total - conectados
col1.metric("Total dispositivos", f"{total:,}")
col2.metric("Conectados hoy", f"{conectados:,}", delta=f"{(conectados/total*100):.1f}%" if total else "0%")
col3.metric("Sin conexión", f"{sin_conexion:,}", delta=f"{(sin_conexion/total*100):.1f}%" if total else "0%")

col4, col5, col6 = st.columns(3)
bateria_media = df["porcentaje_bateria"].mean() if "porcentaje_bateria" in df.columns and not df.empty else None
col4.metric("Batería media (%)", f"{bateria_media:.1f}%" if bateria_media is not None else "N/A")
if "pct_recibidos_vs_esperados" in df.columns and not df.empty:
    bajos_ratio = count_ratio_below_25(df["pct_recibidos_vs_esperados"])
    col5.metric("Ratio < 25%", f"{int(bajos_ratio):,}", delta=f"{(bajos_ratio/total*100):.1f}%" if total else "0%")
else:
    col5.metric("Ratio < 25%", "N/A")
if "porcentaje_bateria" in df.columns and not df.empty:
    bajos_bateria = df[(df["porcentaje_bateria"] > 0) & (df["porcentaje_bateria"] < 20)].shape[0]
    col6.metric("Batería < 20%", f"{bajos_bateria:,}", delta=f"{(bajos_bateria/total*100):.1f}%" if total else "0%")
else:
    col6.metric("Batería < 20%", "N/A")

# ==============================
# Tabs
# ==============================
tab1, tab2, tab3 = st.tabs([f"📊 Panel General – {filtro_titulo}", "📈 Análisis Avanzado", "⚙️ Control"])

# =========================
# TAB 1
# =========================
with tab1:
    st.subheader(f"📊 Panel de Control – {filtro_titulo}")

    colA, colB = st.columns(2)
    with colA:
        if "clasificacion_conexion" in df.columns:
            fig = px.pie(df, names="clasificacion_conexion", title=f"Distribución por Estado – {filtro_titulo}")
            st.plotly_chart(fig, use_container_width=True)
        if "ranch_name" in df.columns:
            df_ranch = df.groupby("ranch_name")["device_id"].nunique().reset_index()
            df_ranch.columns = ["Ganadería", "Nº Dispositivos"]
            fig = px.bar(df_ranch, x="Ganadería", y="Nº Dispositivos",
                         title=f"Dispositivos por Ganadería – {filtro_titulo}", text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
    with colB:
        if "pct_recibidos_vs_esperados" in df.columns:
            fig = px.histogram(df, x="pct_recibidos_vs_esperados", nbins=20, title="Ratio de Mensajes Recibidos (%)")
            st.plotly_chart(fig, use_container_width=True)
        if "porcentaje_bateria" in df.columns:
            fig = px.histogram(df, x="porcentaje_bateria", nbins=20, title="Distribución de Batería (%)")
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("📋 Tabla y Mapa de Dispositivos")

    busqueda = st.text_input("🔎 Buscar por cualquier campo:", "")
    df_filtrado = df if not busqueda else df[df.apply(lambda r: busqueda.lower() in str(r).lower(), axis=1)]
    if "ultimo_mensaje_recibido" in df_filtrado.columns:
        df_filtrado = df_filtrado.sort_values(by="ultimo_mensaje_recibido", ascending=False)

    if cliente == "Todos":
        st.markdown("#### 📋 Tabla de dispositivos (vista completa)")
        st.data_editor(
            df_filtrado, use_container_width=True, height=700,
            column_config={
                "porcentaje_bateria": st.column_config.ProgressColumn("Batería (%)", format="%.1f"),
                "pct_recibidos_vs_esperados": st.column_config.ProgressColumn("Ratio mensajes", format="%.2f")
            },
            hide_index=True
        )
    else:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 📋 Tabla de dispositivos")
            st.data_editor(
                df_filtrado, use_container_width=True, height=600,
                column_config={
                    "porcentaje_bateria": st.column_config.ProgressColumn("Batería (%)", format="%.1f"),
                    "pct_recibidos_vs_esperados": st.column_config.ProgressColumn("Ratio mensajes", format="%.2f")
                },
                hide_index=True
            )
        with c2:
            st.markdown("#### 🗺️ Mapa última posición GPS")
            if {"lat","lon"}.issubset(df.columns):
                df_coords = df_filtrado.dropna(subset=["lat","lon"]).copy()
                if not df_coords.empty:
                    if len(df_coords) == 1:
                        zoom_location = [df_coords.iloc[0]["lat"], df_coords.iloc[0]["lon"]]; zoom_level = 14
                    elif len(df_coords) < 5:
                        zoom_location = [df_coords["lat"].mean(), df_coords["lon"].mean()]; zoom_level = 12
                    else:
                        zoom_location = [df_coords["lat"].mean(), df_coords["lon"].mean()]; zoom_level = 8
                    m = folium.Map(location=zoom_location, zoom_start=zoom_level, tiles="OpenStreetMap")
                    folium.TileLayer(
                        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                        attr="Esri", name="Satélite (Esri)", overlay=False, control=True
                    ).add_to(m)
                    cluster = MarkerCluster().add_to(m)
                    for _, row in df_coords.iterrows():
                        popup = f"""
                        <b>Nº Serie:</b> {row.get('SerialNumber','N/A')}<br>
                        <b>Cliente:</b> {row.get('customer_name','N/A')}<br>
                        <b>Última pos. GPS:</b> {row.get('ultima_posicion_gps_valida','N/A')}<br>
                        <b>Estado conexión:</b> {row.get('clasificacion_conexion','N/A')}<br>
                        <b>Último mensaje:</b> {row.get('ultimo_mensaje_recibido','N/A')}
                        """
                        folium.Marker(location=[row["lat"], row["lon"]], popup=popup).add_to(cluster)
                    folium.LayerControl(position="topright", collapsed=False).add_to(m)
                    st_folium(m, width=600, height=600)
                else:
                    st.info("No hay coordenadas disponibles para mostrar el mapa.")
            else:
                st.warning("El dataset no contiene columnas `lat` y `lon` necesarias para el mapa.")

# =========================
# TAB 2 – Ajuste por ventana + umbrales editables
# =========================
with tab2:
    st.subheader(f"📈 Análisis Avanzado – {filtro_titulo}")

    if df.empty:
        st.info("No hay datos con los filtros actuales.")
        st.stop()

    # ---- Estado inicial en sesión ----
    if "ventana_dias" not in st.session_state:
        st.session_state["ventana_dias"] = 3
    if "umbral_ok_device" not in st.session_state:
        st.session_state["umbral_ok_device"] = 50  # %
    if "umbral_ok_ranch" not in st.session_state:
        st.session_state["umbral_ok_ranch"] = 50   # %

    # ---- UI compacta ----
    cc1, cc2, cc3, cc4, cc5 = st.columns([1, 1, 1, 1, 2])
    with cc1:
        val_num = st.number_input(
            "Ventana (días)",
            min_value=1, max_value=60, value=int(st.session_state["ventana_dias"]), step=1,
            help="Si una ganadería es NO OK, pero **todos** sus dispositivos NO OK han comunicado dentro de esta ventana, "
                 "la consideramos **OK (ajustada)**."
        )
    with cc2:
        new_umbral_device = st.number_input(
            "% OK dispositivo", min_value=0, max_value=100,
            value=int(st.session_state["umbral_ok_device"]), step=1,
            help="Umbral mínimo de % de posiciones válidas vs esperadas para considerar un dispositivo OK."
        )
    with cc3:
        new_umbral_ranch = st.number_input(
            "% OK ganadería", min_value=0, max_value=100,
            value=int(st.session_state["umbral_ok_ranch"]), step=1,
            help="Umbral mínimo de % de dispositivos OK para considerar una ganadería OK."
        )
    with cc4:
        if st.button("Aplicar"):
            st.session_state["ventana_dias"] = int(val_num)
            st.session_state["umbral_ok_device"] = int(new_umbral_device)
            st.session_state["umbral_ok_ranch"] = int(new_umbral_ranch)
            _rerun()
    with cc5:
        st.markdown("**Selección rápida (ventana):**")
        r1c1, r1c2, r1c3, r1c4, r1c5 = st.columns(5)
        if r1c1.button("1d"):  st.session_state["ventana_dias"] = 1;  _rerun()
        if r1c2.button("3d"):  st.session_state["ventana_dias"] = 3;  _rerun()
        if r1c3.button("7d"):  st.session_state["ventana_dias"] = 7;  _rerun()
        if r1c4.button("14d"): st.session_state["ventana_dias"] = 14; _rerun()
        if r1c5.button("30d"): st.session_state["ventana_dias"] = 30; _rerun()

    ventana_dias = int(st.session_state["ventana_dias"])
    UMBRAL_DEVICE = int(st.session_state["umbral_ok_device"])
    UMBRAL_RANCH  = int(st.session_state["umbral_ok_ranch"])

    st.caption(f"Ventana actual: **{ventana_dias} días** · % OK dispositivo: **{UMBRAL_DEVICE}%** · % OK ganadería: **{UMBRAL_RANCH}%**")

    # ---- Cálculos (cliente) ----
    df_work = df.copy()
    now_utc = pd.Timestamp.now(tz="UTC")

    # Dispositivo OK por % válidas vs esperadas
    col_pct_valid_vs_exp = "Posición válida vs esperadas (%)"
    if col_pct_valid_vs_exp in df_work.columns:
        df_work[col_pct_valid_vs_exp] = pd.to_numeric(df_work[col_pct_valid_vs_exp], errors="coerce")
        df_work["dispositivo_ok_base"] = (df_work[col_pct_valid_vs_exp] >= UMBRAL_DEVICE).fillna(False)
    elif "Dispositivo OK (≥50% válidas vs esperadas)" in df_work.columns:
        # Fallback si no tenemos el %; aviso de umbral fijo
        st.info("No se encontró columna 'Posición válida vs esperadas (%)'. Se usa la columna booleana precalculada (umbral fijo de 50%).")
        df_work["dispositivo_ok_base"] = df_work["Dispositivo OK (≥50% válidas vs esperadas)"].astype(bool)
    else:
        df_work["dispositivo_ok_base"] = False

    # Comunicación dentro de la ventana
    if "ultimo_mensaje_recibido" in df_work.columns:
        ts_last = pd.to_datetime(df_work["ultimo_mensaje_recibido"], errors="coerce", utc=True)
        limite_nd = now_utc - pd.Timedelta(days=ventana_dias)
        df_work["comunico_window"] = (ts_last >= limite_nd).fillna(False)
    else:
        df_work["comunico_window"] = False

    df_work["all_gateways_online_bool"] = df_work.get("all_gateways_online", False)
    df_work["all_gateways_online_bool"] = df_work["all_gateways_online_bool"].map(to_bool)

    for c in ["ranch_name", "customer_name", "Country", "Region"]:
        if c not in df_work.columns: df_work[c] = None

    grp = df_work.groupby("ranch_name", dropna=False)

    def first_non_null(s):
        s2 = s.dropna()
        return s2.iloc[0] if not s2.empty else None

    ranch_status = grp.apply(lambda g: pd.Series({
        "n_dispositivos": int(g["device_id"].nunique()),
        "n_ok_base": int(g["dispositivo_ok_base"].sum()),
        "non_ok_count": int((~g["dispositivo_ok_base"]).sum()),
        "non_ok_comm_window_count": int((~g["dispositivo_ok_base"] & g["comunico_window"]).sum()),
        "customer_name": first_non_null(g["customer_name"]),
        "Country": first_non_null(g["Country"]),
        "Region": first_non_null(g["Region"]),
        "all_gateways_online": bool(g["all_gateways_online_bool"].fillna(False).all()),
        "ranch_gateway_overall_status": first_non_null(g["ranch_gateway_overall_status"]),
    })).reset_index()

    ranch_status["pct_ok_base"] = (
        100.0 * ranch_status["n_ok_base"] / ranch_status["n_dispositivos"]
    ).replace([pd.NA, float("inf")], 0).fillna(0)

    # Umbral de ganadería OK (editable)
    ranch_status["ranch_ok_base"] = ranch_status["pct_ok_base"] >= UMBRAL_RANCH

    ranch_status["ajuste_aplicado"] = (
        (~ranch_status["ranch_ok_base"]) &
        (ranch_status["non_ok_count"] > 0) &
        (ranch_status["non_ok_comm_window_count"] == ranch_status["non_ok_count"])
    )
    ranch_status["n_ok_ajustada"] = ranch_status.apply(
        lambda r: r["n_dispositivos"] if r["ajuste_aplicado"] else r["n_ok_base"], axis=1
    )
    ranch_status["pct_ok_ajustada"] = (
        100.0 * ranch_status["n_ok_ajustada"] / ranch_status["n_dispositivos"]
    ).replace([pd.NA, float("inf")], 0).fillna(0)
    ranch_status["ranch_ok_ajustada"] = ranch_status["pct_ok_ajustada"] >= UMBRAL_RANCH

    vista = st.radio(
        "Vista de métrica",
        options=["Base", f"Ajustada ({ventana_dias} días)"],
        index=1, horizontal=True
    )
    if vista == "Base":
        ranch_status["ranch_ok_view"] = ranch_status["ranch_ok_base"]
        ranch_status["pct_ok_view"] = ranch_status["pct_ok_base"]
        titulo_view = f"ESPERADAS (Base, umbral {UMBRAL_RANCH}%)"
    else:
        ranch_status["ranch_ok_view"] = ranch_status["ranch_ok_ajustada"]
        ranch_status["pct_ok_view"] = ranch_status["pct_ok_ajustada"]
        titulo_view = f"AJUSTADA (ventana {ventana_dias} días, umbral {UMBRAL_RANCH}%)"

    total_ranch = ranch_status.shape[0]
    n_ok_ranch = int(ranch_status["ranch_ok_view"].sum())
    n_no_ok_ranch = total_ranch - n_ok_ranch
    promoted = int((~ranch_status["ranch_ok_base"] & ranch_status["ranch_ok_ajustada"]).sum())

    colk1, colk2, colk3, colk4, colk5 = st.columns(5)
    colk1.metric("Ganaderías (con filtros)", f"{total_ranch:,}")
    colk2.metric("Ganaderías OK", f"{n_ok_ranch:,}", delta=f"{(n_ok_ranch/total_ranch*100):.1f}%" if total_ranch else "0%")
    colk3.metric("Ganaderías NO OK", f"{n_no_ok_ranch:,}", delta=f"{(n_no_ok_ranch/total_ranch*100):.1f}%" if total_ranch else "0%")
    colk4.metric("NO OK → OK por ajuste", f"{promoted:,}")
    colk5.metric("Ventana (días)", f"{ventana_dias}")

    st.divider()

    st.markdown(f"#### % de dispositivos OK por ganadería – {titulo_view}")
    if not ranch_status.empty:
        df_bar = ranch_status.sort_values("pct_ok_view", ascending=True)
        fig = px.bar(
            df_bar, x="pct_ok_view", y="ranch_name",
            color="ranch_ok_view",
            color_discrete_map={True:"#2ca02c", False:"#d62728"},
            text=df_bar["pct_ok_view"].map(lambda v: f"{v:.1f}%"),
            labels={"pct_ok_view":"% dispositivos OK","ranch_name":"Ganadería","ranch_ok_view":"Ganadería OK"},
            height=min(700, 30*max(6, df_bar.shape[0]))
        )
        fig.update_layout(xaxis_title="% dispositivos OK", yaxis_title=None, bargap=0.25)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay datos de ganaderías para graficar.")

    st.markdown(f"#### Distribución de ganaderías NO OK – {titulo_view}")
    if (~ranch_status["ranch_ok_view"]).sum() == 0:
        st.success("Todas las ganaderías están OK con los filtros actuales.")
    else:
        if vista == "Base":
            breakdown = ranch_status[~ranch_status["ranch_ok_base"]][["ranch_name","pct_ok_base"]]
            fig = px.histogram(breakdown, x="pct_ok_base", nbins=10, title=f"Histograma % OK (Base, umbral {UMBRAL_RANCH}%) de las NO OK")
        else:
            breakdown = ranch_status[~ranch_status["ranch_ok_ajustada"]][
                ["ranch_name","pct_ok_ajustada","ajuste_aplicado","non_ok_count","non_ok_comm_window_count"]
            ]
            fig = px.histogram(
                breakdown, x="pct_ok_ajustada", nbins=10,
                title=f"Histograma % OK (Ajustada {ventana_dias}d, umbral {UMBRAL_RANCH}%) de las NO OK"
            )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.markdown("#### 📋 Estado de ganaderías (Base vs Ajustada)")
    cols_order = [
        "ranch_name","customer_name","Country","Region",
        "n_dispositivos",
        "n_ok_base","pct_ok_base","ranch_ok_base",
        "non_ok_count","non_ok_comm_window_count","ajuste_aplicado",
        "n_ok_ajustada","pct_ok_ajustada","ranch_ok_ajustada",
        "all_gateways_online","ranch_gateway_overall_status",
    ]
    for c in cols_order:
        if c not in ranch_status.columns: ranch_status[c] = None

    display_cols = ranch_status[cols_order].rename(columns={
        "non_ok_comm_window_count": f"NO OK que comunicaron en {ventana_dias} días"
    })
    st.dataframe(
        display_cols.sort_values(["ranch_ok_ajustada","pct_ok_ajustada","pct_ok_base"],
                                 ascending=[True, False, False]),
        use_container_width=True, hide_index=True
    )
    st.download_button(
        f"⬇️ Descargar estado de ganaderías (Base vs Ajustada {ventana_dias}d, CSV)",
        display_cols.to_csv(index=False).encode("utf-8"),
        file_name=f"ranch_status_base_vs_ajustada_{ventana_dias}d.csv",
        mime="text/csv"
    )

    st.markdown("—")

    st.markdown(f"#### 🔎 Foco en ganaderías NO OK – {titulo_view}")
    not_ok_view = ranch_status[~ranch_status["ranch_ok_view"]]
    if not not_ok_view.empty:
        ranch_sel = st.selectbox(
            "Selecciona ganadería para diagnóstico",
            options=not_ok_view.sort_values("pct_ok_view", ascending=True)["ranch_name"].tolist(),
            index=0
        )
        df_ranch_devices = df_work[df_work["ranch_name"] == ranch_sel].copy()

        c1, c2, c3, c4 = st.columns(4)
        total_dev = df_ranch_devices["device_id"].nunique()
        ok_dev_base = int(df_ranch_devices["dispositivo_ok_base"].sum())
        pct_dev_ok_base = (100.0 * ok_dev_base / total_dev) if total_dev else 0.0
        gw_ok = df_ranch_devices["all_gateways_online_bool"].fillna(False).all()

        non_ok = (~df_ranch_devices["dispositivo_ok_base"]).sum()
        non_ok_commN = (~df_ranch_devices["dispositivo_ok_base"] & df_ranch_devices["comunico_window"]).sum()
        ajuste_aplica = (pct_dev_ok_base < UMBRAL_RANCH) and (non_ok > 0) and (non_ok == non_ok_commN)
        ok_dev_ajustada = total_dev if ajuste_aplica else ok_dev_base
        pct_dev_ok_ajustada = (100.0 * ok_dev_ajustada / total_dev) if total_dev else 0.0

        c1.metric("Dispositivos", f"{total_dev:,}")
        c2.metric("% OK (base)", f"{pct_dev_ok_base:.1f}%")
        c3.metric(f"% OK (ajustada {ventana_dias}d)", f"{pct_dev_ok_ajustada:.1f}%", delta="+ajuste" if ajuste_aplica else None)
        c4.metric("Antenas online", "Sí" if gw_ok else "No")

        df_ranch_devices["no_ok_base"] = ~df_ranch_devices["dispositivo_ok_base"]
        df_ranch_devices["comunico_window"] = df_ranch_devices["comunico_window"].astype(bool)
        cols_ranch_dev = [
            "device_id","SerialNumber","Model",
            "clasificacion_conexion",
            "Mensajes esperados (detallado)","Mensajes recibidos (n)",
            "Mensaje con posición GPS (n)","Posición GPS válida (n)",
            "Posición válida vs esperadas (%)",
            "ultimo_mensaje_recibido",
            "no_ok_base","comunico_window",
            "porcentaje_bateria",
            "gateway_name","gateway_serial","gateway_last_seen",
        ]
        cols_ranch_dev = [c for c in cols_ranch_dev if c in df_ranch_devices.columns]
        df_ranch_display = df_ranch_devices[cols_ranch_dev].rename(
            columns={"comunico_window": f"Comunicó en {ventana_dias} días"}
        )
        st.dataframe(
            df_ranch_display.sort_values(
                by=[c for c in ["no_ok_base", f"Comunicó en {ventana_dias} días", "Posición válida vs esperadas (%)", "ultimo_mensaje_recibido"] if c in df_ranch_display.columns],
                ascending=[False, False, True, False]
            ),
            use_container_width=True, hide_index=True
        )
    else:
        st.success("No hay ganaderías NO OK con los filtros actuales en esta vista.")

# =========================
# TAB 3 – Control
# =========================
with tab3:
    st.subheader("⚙️ Control")
    st.write("Ajustes y herramientas de administración.")
    st.markdown(f"- Ventana de ajuste actual: **{int(st.session_state.get('ventana_dias', 3))} días**")
    st.markdown(f"- Umbral OK dispositivo: **{int(st.session_state.get('umbral_ok_device', 50))}%**")
    st.markdown(f"- Umbral OK ganadería: **{int(st.session_state.get('umbral_ok_ranch', 50))}%**")
    st.markdown(f"- CSV de origen: **{nombre_archivo}**")
