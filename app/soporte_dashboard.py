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
#  # TAB 2  (CORREGIDO: ranch_ok SOLO por % dispositivos OK)
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

    # Columna dispositivo_ok (ESPERADAS): usa la tuya si existe; si no, la calculamos
    if "Dispositivo OK (≥60% válidas vs esperadas)" in df_work.columns:
        df_work["dispositivo_ok"] = df_work["Dispositivo OK (≥60% válidas vs esperadas)"].astype(bool)
    else:
        col_pct_valid_vs_exp = "Posición válida vs esperadas (%)"
        if col_pct_valid_vs_exp in df_work.columns:
            df_work["dispositivo_ok"] = (pd.to_numeric(df_work[col_pct_valid_vs_exp], errors="coerce") >= 60).fillna(False)
        else:
            df_work["dispositivo_ok"] = False  # fallback conservador

    # === NUEVO: Columna dispositivo_ok_rec (RECIBIDAS)
    if "Dispositivo OK (≥60% válidas vs recibidas)" in df_work.columns:
        df_work["dispositivo_ok_rec"] = df_work["Dispositivo OK (≥60% válidas vs recibidas)"].astype(bool)
    elif "Posición válida vs recibidas (%)" in df_work.columns:
        pct_rec = pd.to_numeric(df_work["Posición válida vs recibidas (%)"], errors="coerce")
        df_work["dispositivo_ok_rec"] = (pct_rec >= 60).fillna(False)
    elif {"Posición GPS válida (n)", "Mensajes recibidos (n)"} <= set(df_work.columns):
        # Recalcular % recibidas si no viene hecho
        valid_n = pd.to_numeric(df_work["Posición GPS válida (n)"], errors="coerce")
        rec_n = pd.to_numeric(df_work["Mensajes recibidos (n)"], errors="coerce").replace({0: pd.NA})
        pct_rec = (valid_n / rec_n) * 100
        df_work["dispositivo_ok_rec"] = (pct_rec >= 60).fillna(False)
    else:
        df_work["dispositivo_ok_rec"] = False  # fallback conservador

    # Boolean de gateways online a nivel fila (puede venir como string "TRUE"/"FALSE")
    if "all_gateways_online" in df_work.columns:
        df_work["all_gateways_online_bool"] = df_work["all_gateways_online"].map(
            lambda x: True if str(x).strip().lower() in {"true", "1"} else False
        )
    else:
        df_work["all_gateways_online_bool"] = False

    # =========================
    # Agregado por ganadería
    # =========================
    ranch_cols_keep = ["ranch_name", "customer_name", "Country", "Region"]
    for c in ranch_cols_keep:
        if c not in df_work.columns:
            df_work[c] = None

    grp = df_work.groupby("ranch_name", dropna=False)

    def agg_bool_all(s):
        # true solo si TODOS los valores son True (ignorando NaN -> False)
        s2 = s.fillna(False).astype(bool)
        return bool(s2.all())

    # --- Métricas por ganadería (ESPERADAS)
    ranch_status = grp.agg(
        n_dispositivos=("device_id", "nunique"),
        n_ok=("dispositivo_ok", "sum"),
        pct_ok=("dispositivo_ok", lambda s: (100.0 * s.sum() / max(1, s.shape[0]))),
        all_gateways_online=("all_gateways_online_bool", agg_bool_all),  # informativo
        ranch_gateway_overall_status=("ranch_gateway_overall_status", lambda s: s.dropna().iloc[0] if s.dropna().size else None),
        customer_name=("customer_name", lambda s: s.dropna().iloc[0] if s.dropna().size else None),
        Country=("Country", lambda s: s.dropna().iloc[0] if s.dropna().size else None),
        Region=("Region", lambda s: s.dropna().iloc[0] if s.dropna().size else None),
    ).reset_index()

    # --- NUEVO: Métricas por ganadería (RECIBIDAS)
    ranch_status_rec = grp.agg(
        n_dispositivos=("device_id", "nunique"),
        n_ok_rec=("dispositivo_ok_rec", "sum"),
        pct_ok_rec=("dispositivo_ok_rec", lambda s: (100.0 * s.sum() / max(1, s.shape[0]))),
    ).reset_index()

    # =========================
    # Reglas de clasificación (CORREGIDO)
    # =========================
    # Regla de OK de ganadería (SOLO por % de dispositivos OK; sin antenas) — ESPERADAS
    ranch_status["ranch_ok"] = ranch_status["pct_ok"] >= 70.0

    # Clasificación de fallo (SOLO por % de dispositivos OK) — ESPERADAS
    def clasificar_fallo(row):
        if row["pct_ok"] < 70.0:
            return "Error 3: <70% dispositivos OK"
        return None

    ranch_status["error_categoria"] = ranch_status.apply(clasificar_fallo, axis=1)

    # (Opcional) Etiqueta informativa de antenas, sin afectar OK/NO OK
    ranch_status["aviso_antena"] = ranch_status["all_gateways_online"].map(
        lambda v: "Antena no conectada" if v is False else None
    )

    # === NUEVO: Reglas de clasificación — RECIBIDAS
    ranch_status_rec["ranch_ok_rec"] = ranch_status_rec["pct_ok_rec"] >= 70.0
    # Etiquetas de error para RECIBIDAS (mismo criterio)
    ranch_status_rec["error_categoria_rec"] = ranch_status_rec["pct_ok_rec"].apply(
        lambda v: "Error 3 (recibidas): <70% dispositivos OK" if v < 70.0 else None
    )

    # =========================
    # KPIs generales (ESPERADAS)
    # =========================
    total_ranch = ranch_status.shape[0]
    n_ok_ranch = int(ranch_status["ranch_ok"].sum())
    n_no_ok_ranch = total_ranch - n_ok_ranch

    st.markdown("##### KPIs (basado en **ESPERADAS**)")
    colk1, colk2, colk3, colk4 = st.columns(4)
    colk1.metric("Ganaderías (con filtros)", f"{total_ranch:,}")
    colk2.metric("Ganaderías OK", f"{n_ok_ranch:,}", delta=f"{(n_ok_ranch / total_ranch * 100):.1f}%" if total_ranch else "0%")
    colk3.metric("Ganaderías NO OK", f"{n_no_ok_ranch:,}", delta=f"{(n_no_ok_ranch / total_ranch * 100):.1f}%" if total_ranch else "0%")

    # Desglose de errores (solo por % OK, ESPERADAS)
    error_counts = ranch_status[~ranch_status["ranch_ok"]].groupby("error_categoria").size().reset_index(name="n")
    if error_counts.empty:
        colk4.metric("Fallo más común", "—")
    else:
        top_err = error_counts.sort_values("n", ascending=False).iloc[0]
        colk4.metric("Fallo más común", f"{top_err['error_categoria']}", delta=f"{int(top_err['n'])} ranchos")

    # =========================
    # NUEVO: KPIs generales (RECIBIDAS)
    # =========================
    total_ranch_rec = ranch_status_rec.shape[0]
    n_ok_ranch_rec = int(ranch_status_rec["ranch_ok_rec"].sum())
    n_no_ok_ranch_rec = total_ranch_rec - n_ok_ranch_rec

    st.markdown("##### KPIs (basado en **RECIBIDAS**)")
    colr1, colr2, colr3, colr4 = st.columns(4)
    colr1.metric("Ganaderías (recibidas)", f"{total_ranch_rec:,}")
    colr2.metric("Ganaderías OK (recibidas)", f"{n_ok_ranch_rec:,}", delta=f"{(n_ok_ranch_rec / total_ranch_rec * 100):.1f}%" if total_ranch_rec else "0%")
    colr3.metric("Ganaderías NO OK (recibidas)", f"{n_no_ok_ranch_rec:,}", delta=f"{(n_no_ok_ranch_rec / total_ranch_rec * 100):.1f}%" if total_ranch_rec else "0%")

    error_counts_rec = ranch_status_rec[~ranch_status_rec["ranch_ok_rec"]].groupby("error_categoria_rec").size().reset_index(name="n")
    if error_counts_rec.empty:
        colr4.metric("Fallo más común (recibidas)", "—")
    else:
        top_err_rec = error_counts_rec.sort_values("n", ascending=False).iloc[0]
        colr4.metric("Fallo más común (recibidas)", f"{top_err_rec['error_categoria_rec']}", delta=f"{int(top_err_rec['n'])} ranchos")

    st.divider()

    # =========================
    # Visualizaciones
    # =========================
    colv1, colv2 = st.columns([3, 2])

    with colv1:
        st.markdown("#### % de dispositivos OK por ganadería")
        if not ranch_status.empty:
            df_bar = ranch_status.sort_values("pct_ok", ascending=True)
            fig = px.bar(
                df_bar,
                x="pct_ok",
                y="ranch_name",
                color="ranch_ok",
                color_discrete_map={True: "#2ca02c", False: "#d62728"},
                text=df_bar["pct_ok"].map(lambda v: f"{v:.1f}%"),
                labels={"pct_ok": "% dispositivos OK", "ranch_name": "Ganadería", "ranch_ok": "Ganadería OK"},
                height=min(700, 30 * max(6, df_bar.shape[0])),
            )
            fig.update_layout(xaxis_title="% dispositivos OK", yaxis_title=None, bargap=0.25)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos de ganaderías para graficar.")

    with colv2:
        st.markdown("#### Distribución de ganaderías NO OK por tipo de fallo")
        if not error_counts.empty:
            fig = px.pie(error_counts, names="error_categoria", values="n", hole=0.35)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("Todas las ganaderías están OK con los filtros actuales.")

    st.divider()

    # =========================
    # Tablas de trabajo
    # =========================
    st.markdown("#### 📋 Estado de ganaderías")
    cols_order = [
        "ranch_name", "customer_name", "Country", "Region",
        "n_dispositivos", "n_ok", "pct_ok",
        "all_gateways_online", "ranch_gateway_overall_status",  # informativas
        "aviso_antena",  # informativa
        "ranch_ok", "error_categoria"
    ]
    for c in cols_order:
        if c not in ranch_status.columns:
            ranch_status[c] = None

    st.dataframe(
        ranch_status[cols_order].sort_values(["ranch_ok", "pct_ok"], ascending=[True, False]),
        use_container_width=True,
        hide_index=True
    )

    # Botón descarga
    st.download_button(
        "⬇️ Descargar estado de ganaderías (CSV)",
        ranch_status[cols_order].to_csv(index=False).encode("utf-8"),
        file_name="ranch_status.csv",
        mime="text/csv"
    )

    st.markdown("—")

    # =========================
    # Dispositivos con Error 2 (no llegan al 60%)
    # =========================
    st.markdown("#### 🚨 Dispositivos con <60% de válidas vs esperadas (Error 2)")

    # Aseguramos columna de % válidas vs esperadas
    if "Posición válida vs esperadas (%)" in df_work.columns:
        pct_valid_col = "Posición válida vs esperadas (%)"
        df_work[pct_valid_col] = pd.to_numeric(df_work[pct_valid_col], errors="coerce")
        df_error2 = df_work[df_work[pct_valid_col] < 60].copy()
    else:
        # Si no existe la columna, deducimos desde flags dispositivo_ok
        df_error2 = df_work[~df_work["dispositivo_ok"]].copy()

    # Campos recomendados
    cols_dev = [
        "device_id", "SerialNumber", "Model",
        "ranch_name", "customer_name", "Country", "Region",
        "clasificacion_conexion",
        "ultimo_mensaje_recibido",
        "Mensajes esperados (detallado)", "Mensajes recibidos (n)",
        "Posición GPS válida (n)", "Posición válida vs esperadas (%)",
        "gateway_name", "gateway_serial", "all_gateways_online",
        "porcentaje_bateria"
    ]
    cols_dev_exist = [c for c in cols_dev if c in df_error2.columns]

    if not df_error2.empty:
        df_error2_disp = df_error2[cols_dev_exist].sort_values(
            by=[c for c in ["Posición válida vs esperadas (%)", "ultimo_mensaje_recibido"] if c in cols_dev_exist],
            ascending=[True, False] if "Posición válida vs esperadas (%)" in cols_dev_exist else False
        )
        st.dataframe(df_error2_disp, use_container_width=True, hide_index=True)

        st.download_button(
            "⬇️ Descargar dispositivos Error 2 (CSV)",
            df_error2_disp.to_csv(index=False).encode("utf-8"),
            file_name="dispositivos_error2.csv",
            mime="text/csv"
        )
    else:
        st.success("No hay dispositivos con Error 2 bajo los filtros actuales.")

    st.divider()

    # =========================
    # Foco en ganaderías NO OK (detalle por causa)
    # =========================
    st.markdown("#### 🔎 Foco en ganaderías NO OK")
    not_ok = ranch_status[~ranch_status["ranch_ok"]]
    if not not_ok.empty:
        # Selector de ganadería para diagnóstico rápido
        ranch_sel = st.selectbox(
            "Selecciona ganadería para diagnóstico",
            options=not_ok.sort_values("pct_ok", ascending=True)["ranch_name"].tolist(),
            index=0
        )

        # Subconjunto de dispositivos de la ganadería elegida (respetando filtros globales)
        df_ranch_devices = df_work[df_work["ranch_name"] == ranch_sel].copy()

        c1, c2, c3 = st.columns(3)
        total_dev = df_ranch_devices["device_id"].nunique()
        ok_dev = int(df_ranch_devices["dispositivo_ok"].sum())
        pct_dev_ok = (100.0 * ok_dev / total_dev) if total_dev else 0.0
        gw_ok = df_ranch_devices["all_gateways_online_bool"].fillna(False).all()

        c1.metric("Dispositivos", f"{total_dev:,}")
        c2.metric("Dispositivos OK", f"{ok_dev:,}", delta=f"{pct_dev_ok:.1f}%")
        c3.metric("Antenas online", "Sí" if gw_ok else "No")  # solo informativo

        # Tabla de dispositivos de la ganadería
        cols_ranch_dev = [
            "device_id", "SerialNumber", "Model",
            "clasificacion_conexion",
            "Mensajes esperados (detallado)", "Mensajes recibidos (n)",
            "Mensaje con posición GPS (n)", "Posición GPS válida (n)",
            "Posición válida vs esperadas (%)",
            "porcentaje_bateria",
            "gateway_name", "gateway_serial", "gateway_last_seen",
        ]
        cols_ranch_dev = [c for c in cols_ranch_dev if c in df_ranch_devices.columns]

        st.dataframe(
            df_ranch_devices[cols_ranch_dev].sort_values(
                by=[c for c in ["Posición válida vs esperadas (%)", "Mensajes recibidos (n)"] if c in cols_ranch_dev],
                ascending=[True, False] if "Posición válida vs esperadas (%)" in cols_ranch_dev else False
            ),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.success("No hay ganaderías NO OK con los filtros actuales.")


st.markdown("---")
st.markdown(
    "<div style='text-align: center; font-size: 0.9em; color: gray;'>"
    "👨‍💻 Responsable del dashboard: Guillermo Durántez – "
    "<a href='mailto:guillermo@ixorigue.com'>guillermo@ixorigue.com</a>"
    "</div>",
    unsafe_allow_html=True
)
