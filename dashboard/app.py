from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st
import streamlit.components.v1 as components


ROOT = Path(__file__).resolve().parents[1]
DOCKER_COMPOSE = ["docker-compose"]
SERVICE_MAP = {
    "postgres": "logistica_postgres_1",
    "kafka": "logistica_kafka_1",
    "nifi": "logistica_nifi_1",
    "spark": "logistica_spark_1",
    "cassandra": "logistica_cassandra_1",
    "namenode": "logistica_namenode_1",
    "datanode": "logistica_datanode_1",
    "airflow": "logistica_airflow-webserver_1",
}

KDD_MERMAID = r"""
flowchart LR
    A[API publica Open-Meteo] --> B[NiFi]
    B --> C[Kafka datos_crudos]
    B --> D[Kafka datos_filtrados]
    C --> E[HDFS raw]
    D --> F[Spark staging clima]
    F --> G[Hive stg_weather_open_meteo]
    G --> H[Dimensiones Hive]
    H --> I[dim_ports_routes_weather]
    I --> J[fact_weather_operational]
    E --> K[stg_ships / alerts]
    K --> L[GraphFrames]
    L --> M[fact_route_risk / fact_graph_centrality]
    M --> N[fact_alerts]
    K --> O[Cassandra vehicle_latest_state]
    J --> P[Airflow]
    N --> P
    O --> P
"""

SEQUENCE_MERMAID = r"""
sequenceDiagram
    participant API as Open-Meteo API
    participant NiFi as NiFi
    participant Kafka as Kafka
    participant Spark as Spark
    participant Hive as Hive/HDFS
    participant Cass as Cassandra
    participant Airflow as Airflow

    API->>NiFi: GET weather snapshot
    NiFi->>Kafka: Publica datos_crudos
    NiFi->>Kafka: Publica datos_filtrados
    Kafka->>Spark: Lee topic datos_filtrados_ok
    Spark->>Hive: Crea staging y facts weather
    Spark->>Hive: Crea grafos y alertas
    Spark->>Cass: Carga vehicle_latest_state
    Airflow->>Spark: Orquesta micro-batches
"""

CLASS_MERMAID = r"""
classDiagram
    class Port {
      +port_id
      +port_name
      +macro_region
      +lat
      +lon
    }
    class Route {
      +route_id
      +origin_port
      +dest_port
      +route_mode
      +sea_hours_estimate
    }
    class WeatherEvent {
      +event_ts
      +temperature_c
      +humidity_pct
      +wind_speed_kmh
      +severity
    }
    class OperationalAlert {
      +via_port
      +risk_level
      +stock_status
      +recommendation
      +severity
    }
    class VehicleLatestState {
      +ship_id
      +route_id
      +dest_port
      +stock_on_hand
    }
    Route --> Port
    WeatherEvent --> Port
    OperationalAlert --> Route
    VehicleLatestState --> Route
"""

USE_CASE_MERMAID = r"""
flowchart TB
    U1([Profesor / Tribunal]) --> UC1[Ver arquitectura KDD]
    U1 --> UC2[Ver estado de servicios]
    U1 --> UC3[Ver mapa de barcos y rutas]
    U1 --> UC4[Ver alertas operativas]
    U1 --> UC5[Ver grafos y nodos criticos]
    U1 --> UC6[Arrancar / parar stack]
"""

CUSTOM_CSS = """
<style>
    .stApp {
        background: radial-gradient(circle at top left, #17345c 0%, #0b1220 45%, #070b13 100%);
        color: #e5eefb;
    }
    .stApp, .stApp * {
        color: #e5eefb;
    }
    [data-testid="stAppViewContainer"],
    [data-testid="stHeader"],
    [data-testid="stToolbar"],
    [data-testid="stSidebar"],
    [data-testid="stSidebar"] *,
    [data-testid="stMainBlockContainer"],
    [data-testid="stMainBlockContainer"] *,
    section[data-testid="stSidebar"] *,
    div[data-testid="stVerticalBlock"] *,
    div[data-testid="stHorizontalBlock"] * {
        color: #e5eefb !important;
    }
    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2rem;
        max-width: 1500px;
    }
    h1, h2, h3, h4, h5, h6, p, li, span, label {
        color: #e5eefb !important;
        font-family: "Inter", sans-serif;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(180deg, rgba(16,27,52,0.95), rgba(10,18,34,0.94));
        border: 1px solid rgba(125, 211, 252, 0.12);
        padding: 1rem;
        border-radius: 1rem;
        box-shadow: 0 12px 32px rgba(0,0,0,0.22);
    }
    div[data-testid="stDataFrame"], div[data-testid="stTable"] {
        background: rgba(8, 15, 29, 0.72);
        border: 1px solid rgba(125, 211, 252, 0.1);
        border-radius: 1rem;
        padding: 0.5rem;
    }
    div[data-baseweb="tab-list"] button {
        color: #d9e7ff !important;
        background: rgba(255,255,255,0.04) !important;
        border-radius: 12px 12px 0 0 !important;
    }
    div[data-baseweb="tab-list"] button[aria-selected="true"] {
        color: #ffffff !important;
        background: rgba(98, 210, 198, 0.12) !important;
    }
    .stButton button {
        color: #edf5ff !important;
        border: 1px solid rgba(125, 211, 252, 0.18) !important;
        background: linear-gradient(180deg, rgba(17,34,62,0.92), rgba(9,18,35,0.92)) !important;
    }
    .stButton button:hover {
        border-color: rgba(98, 210, 198, 0.45) !important;
        color: #ffffff !important;
    }
    [data-testid="stMetricLabel"],
    [data-testid="stMetricValue"],
    [data-testid="stMetricDelta"],
    [data-testid="stMarkdownContainer"],
    [data-testid="stCaptionContainer"],
    [data-testid="stText"],
    [data-testid="stSubheader"],
    [data-testid="stHeaderActionElements"],
    [data-testid="stExpander"],
    [data-testid="stExpander"] *,
    [data-testid="stTabs"] *,
    [data-testid="stSidebarNav"] *,
    [data-testid="stNotificationContent"] *,
    [data-testid="stException"] *,
    [data-testid="stAlertContainer"] *,
    [data-testid="stCodeBlock"] *,
    [data-testid="stCheckbox"] *,
    [data-testid="stRadio"] *,
    [data-testid="stSelectbox"] *,
    [data-testid="stMultiSelect"] *,
    [data-testid="stTextInput"] *,
    [data-testid="stNumberInput"] *,
    [data-testid="stFileUploader"] *,
    .stAlert,
    .stCode,
    .stText,
    .element-container,
    .element-container *,
    .st-emotion-cache-1kyxreq,
    .st-emotion-cache-1kyxreq *,
    .st-emotion-cache-z5fcl4,
    .st-emotion-cache-z5fcl4 *,
    .st-emotion-cache-10trblm,
    .st-emotion-cache-1wivap2,
    .st-emotion-cache-16txtl3,
    .st-emotion-cache-ue6h4q {
        color: #e5eefb !important;
    }
    .stDataFrame table, .stTable table {
        color: #e5eefb !important;
        background: transparent !important;
    }
    .stDataFrame thead tr th, .stTable thead tr th {
        color: #9fd8ff !important;
        background: rgba(255,255,255,0.03) !important;
    }
    .stDataFrame tbody tr td, .stTable tbody tr td {
        color: #e5eefb !important;
    }
    .stDataFrame [role="gridcell"],
    .stDataFrame [role="columnheader"],
    .stDataFrame [role="rowheader"],
    .stDataEditor [role="gridcell"],
    .stDataEditor [role="columnheader"],
    .stDataEditor [role="rowheader"] {
        color: #e5eefb !important;
    }
    svg text,
    svg tspan,
    .mermaid text,
    .mermaid tspan,
    .mermaid span,
    .mermaid foreignObject,
    .mermaid foreignObject * {
        fill: #e5eefb !important;
        color: #e5eefb !important;
    }
    .mermaid .nodeLabel,
    .mermaid .edgeLabel,
    .mermaid .label,
    .mermaid .cluster-label text {
        color: #e5eefb !important;
        fill: #e5eefb !important;
    }
    a, a:visited, a:hover {
        color: #86d7ff !important;
    }
    code, pre {
        color: #f1f5f9 !important;
    }
    .glass-card {
        background: linear-gradient(180deg, rgba(13,23,43,0.96), rgba(8,14,28,0.92));
        border: 1px solid rgba(125, 211, 252, 0.14);
        border-radius: 22px;
        padding: 20px 22px;
        box-shadow: 0 18px 48px rgba(0,0,0,0.25);
    }
    .hero-eyebrow {
        color: #62d2c6 !important;
        font-size: 0.8rem;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        margin-bottom: 0.2rem;
    }
    .hero-title {
        font-size: 4rem;
        line-height: 0.95;
        font-weight: 800;
        margin: 0;
    }
    .hero-subtitle {
        color: #a6b7d5 !important;
        font-size: 1rem;
        max-width: 760px;
        margin-top: 0.8rem;
    }
    .tag-chip {
        display: inline-block;
        padding: 8px 14px;
        margin: 6px 8px 0 0;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.1);
        background: rgba(255,255,255,0.03);
        color: #d7e6ff !important;
        font-size: 0.86rem;
    }
    .tiny-label {
        color: #8aa4c9 !important;
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.15em;
    }
    .big-number {
        font-size: 3rem;
        font-weight: 800;
        margin-top: 0.3rem;
    }
    .status-pill {
        display: inline-block;
        border-radius: 999px;
        padding: 6px 12px;
        font-size: 0.8rem;
        font-weight: 700;
    }
    .pill-ok { background: rgba(34,197,94,0.16); color: #86efac !important; }
    .pill-warn { background: rgba(245,158,11,0.16); color: #fcd34d !important; }
    .pill-danger { background: rgba(239,68,68,0.16); color: #fca5a5 !important; }
    .pill-off { background: rgba(148,163,184,0.14); color: #cbd5e1 !important; }
</style>
"""


def run_command(command: list[str], timeout: int = 120, check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=check,
    )


def render_mermaid(title: str, code: str) -> None:
    diagram_id = f"mermaid-{hashlib.md5(title.encode('utf-8')).hexdigest()[:10]}"
    code_json = json.dumps(code)
    components.html(
        f"""
        <div style=\"background:linear-gradient(180deg, rgba(12,20,38,0.97), rgba(8,14,28,0.96));padding:18px;border-radius:20px;border:1px solid rgba(125,211,252,0.14);box-shadow:0 18px 48px rgba(0,0,0,0.25);\">
          <div style=\"color:#8ecfe0;font-size:12px;letter-spacing:0.16em;text-transform:uppercase;margin-bottom:6px;font-family:Inter,sans-serif;\">Diagrama</div>
          <h3 style=\"color:white;font-family:Inter,sans-serif;margin:0 0 12px 0;\">{title}</h3>
          <div id=\"{diagram_id}\" style=\"overflow:auto;background:rgba(255,255,255,0.02);border-radius:16px;padding:10px;\"></div>
        </div>
        <script type=\"module\">
          import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
          const definition = {code_json};
          mermaid.initialize({{
            startOnLoad: false,
            theme: 'dark',
            securityLevel: 'loose',
            themeVariables: {{
              background: '#0b1220',
              primaryColor: '#133657',
              primaryTextColor: '#e5eefb',
              primaryBorderColor: '#62d2c6',
              lineColor: '#7dd3fc',
              secondaryColor: '#111c31',
              tertiaryColor: '#0f172a',
              fontFamily: 'Inter, sans-serif'
            }}
          }});
          const el = document.getElementById('{diagram_id}');
          mermaid.render('{diagram_id}-svg', definition).then((result) => {{
            el.innerHTML = result.svg;
          }}).catch((error) => {{
            el.innerHTML = `<div style="color:#fca5a5;font-family:Inter,sans-serif;padding:12px;">No se pudo renderizar el diagrama.<br/>${{String(error)}}</div>`;
          }});
        </script>
        """,
        height=460,
    )


def render_html_card(html: str, height: int = 180) -> None:
    components.html(f"<div class='glass-card'>{html}</div>", height=height)


def render_metric_card(title: str, value: str, subtitle: str) -> None:
    render_html_card(
        f"""
        <div class='tiny-label'>{title}</div>
        <div class='big-number'>{value}</div>
        <div style='color:#8aa4c9;margin-top:0.3rem'>{subtitle}</div>
        """,
        height=170,
    )


@st.cache_data(ttl=15)
def get_service_status() -> list[dict[str, str]]:
    result = run_command(["docker", "ps", "-a", "--format", "{{.Names}}\t{{.Status}}"])
    status_by_name: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "\t" in line:
            name, status = line.split("\t", 1)
            status_by_name[name] = status

    rows = []
    for service, container_name in SERVICE_MAP.items():
        raw_status = status_by_name.get(container_name, "Not created")
        if "healthy" in raw_status.lower() or raw_status.startswith("Up"):
            badge = "OK"
        elif "Restarting" in raw_status or "Exited" in raw_status:
            badge = "NOK"
        elif raw_status == "Not created":
            badge = "OFF"
        else:
            badge = "BOOT"
        rows.append({"service": service, "container": container_name, "status": raw_status, "badge": badge})
    return rows


def compose_service_action(service: str, action: str) -> str:
    if action == "start":
        result = run_command(DOCKER_COMPOSE + ["up", "-d", service], timeout=600)
    else:
        result = run_command(DOCKER_COMPOSE + ["stop", service], timeout=300)
    st.cache_data.clear()
    return result.stdout or result.stderr or f"{action} {service} ejecutado"


def run_script(script: str) -> str:
    result = run_command(["bash", script], timeout=1200)
    st.cache_data.clear()
    return result.stdout or result.stderr


@st.cache_data(ttl=60)
def get_dashboard_bundle() -> dict:
    command = DOCKER_COMPOSE + [
        "exec",
        "-T",
        "spark",
        "spark-submit",
        "/home/jovyan/jobs/spark/99_dashboard_bundle.py",
    ]
    result = run_command(command, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or "No se pudo consultar el bundle del dashboard")

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    json_line = next((line for line in reversed(lines) if line.startswith("{") and line.endswith("}")), None)
    if not json_line:
        raise RuntimeError("No se encontro salida JSON valida del bundle del dashboard")
    return json.loads(json_line)


def build_map_data(bundle: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ports = pd.DataFrame(bundle.get("dim_ports", []))
    routes = pd.DataFrame(bundle.get("dim_routes", []))
    ships = pd.DataFrame(bundle.get("ships_latest", []))
    alerts = pd.DataFrame(bundle.get("fact_alerts", []))

    if not routes.empty and not ports.empty:
        port_lookup = ports[["port_name", "lat", "lon"]].rename(
            columns={"lat": "origin_lat", "lon": "origin_lon"}
        )
        routes = routes.merge(port_lookup, left_on="origin_port", right_on="port_name", how="left").drop(columns=["port_name"])
        dest_lookup = ports[["port_name", "lat", "lon"]].rename(
            columns={"port_name": "dest_port", "lat": "dest_lat", "lon": "dest_lon"}
        )
        routes = routes.merge(dest_lookup, on="dest_port", how="left")

    if not alerts.empty and not routes.empty:
        routes = routes.merge(alerts[["via_port", "severity", "risk_level"]], left_on="dest_port", right_on="via_port", how="left")
        routes["severity"] = routes["severity"].fillna(1)
    else:
        routes["severity"] = 1

    if not routes.empty:
        routes["color"] = routes["severity"].apply(
            lambda sev: [220, 38, 38] if sev >= 4 else [245, 158, 11] if sev >= 3 else [59, 130, 246]
        )

    if not ships.empty:
        ships["tooltip"] = ships.apply(
            lambda row: f"{row['ship_id']} | {row['dest_port']} | {row['route_id']}", axis=1
        )

    return ports, routes, ships


def summarize_risk(bundle: dict) -> tuple[int, int, float]:
    alerts = pd.DataFrame(bundle.get("fact_alerts", []))
    weather = pd.DataFrame(bundle.get("fact_weather_operational", []))
    critical = int((alerts["severity"] >= 4).sum()) if not alerts.empty else 0
    medium = int((alerts["severity"] >= 2).sum()) if not alerts.empty else 0
    avg_delay = float(weather["weather_delay_hours_estimate"].mean()) if not weather.empty else 0.0
    return critical, medium, avg_delay


def build_service_badge(badge: str) -> str:
    if badge == "OK":
        return "pill-ok"
    if badge == "NOK":
        return "pill-danger"
    if badge == "BOOT":
        return "pill-warn"
    return "pill-off"


st.set_page_config(page_title="Dashboard KDD Logistica", layout="wide")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

with st.sidebar:
    st.subheader("Controles")
    if st.button("Refrescar dashboard"):
        st.cache_data.clear()
        st.rerun()
    if st.button("Levantar stack completo"):
        st.code(run_command(["docker-compose", "up", "-d", "postgres", "kafka", "nifi", "spark", "cassandra", "namenode", "datanode", "airflow-webserver"], timeout=600).stdout)
        st.cache_data.clear()
    if st.button("Parar stack completo"):
        st.code(run_command(["docker-compose", "down"], timeout=600).stdout)
        st.cache_data.clear()
    if st.button("Rebuild tablas Hive demo"):
        st.code(run_script("scripts/66_rebuild_hive_demo_tables.sh")[-3000:])
    if st.button("Cargar Cassandra latest state"):
        st.code(run_script("scripts/65_load_vehicle_latest_state_cassandra.sh")[-3000:])

bundle = get_dashboard_bundle()
service_rows = get_service_status()

ok_count = sum(1 for row in service_rows if row["badge"] == "OK")
nok_count = sum(1 for row in service_rows if row["badge"] == "NOK")
off_count = sum(1 for row in service_rows if row["badge"] == "OFF")
critical_alerts, medium_alerts, avg_delay = summarize_risk(bundle)

hero_left, hero_right = st.columns([1.7, 0.9])
with hero_left:
    st.markdown("<div class='hero-eyebrow'>BIG DATA TRANSPORT MONITOR</div>", unsafe_allow_html=True)
    st.markdown("<div class='hero-title'>Transport Pulse<br/>Dashboard</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='hero-subtitle'>Centro de control del pipeline KDD: posicion de flota, riesgo meteorologico, alertas operativas, estado de servicios y analitica de red para la logistica maritima.</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div>
          <span class='tag-chip'>NiFi</span>
          <span class='tag-chip'>Kafka</span>
          <span class='tag-chip'>Spark + Hive</span>
          <span class='tag-chip'>Cassandra</span>
          <span class='tag-chip'>GraphFrames</span>
          <span class='tag-chip'>Airflow</span>
          <span class='tag-chip'>Open-Meteo</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
with hero_right:
    render_html_card(
        f"""
        <div class='tiny-label'>sincronizado</div>
        <h3 style='margin:6px 0 10px 0'>Estado del pipeline</h3>
        <p style='color:#9db2d5'>Snapshots servidos desde Spark/Hive, HDFS y Cassandra con control en vivo de servicios Docker.</p>
        <div style='display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:12px'>
          <div><div class='tiny-label'>Pipeline</div><strong>En seguimiento</strong></div>
          <div><div class='tiny-label'>Riesgo medio</div><strong>{'MEDIUM' if medium_alerts else 'LOW'}</strong></div>
          <div><div class='tiny-label'>Servicios OK</div><strong>{ok_count}/{len(service_rows)}</strong></div>
          <div><div class='tiny-label'>Storage</div><strong>HDFS + Hive</strong></div>
        </div>
        """,
        height=260,
    )

metric_cols = st.columns(6)
with metric_cols[0]:
    render_metric_card("Flota monitorizada", str(len(bundle.get("ships_latest", []))), "Ultimo estado por vehiculo")
with metric_cols[1]:
    render_metric_card("Rutas bajo observacion", str(len(bundle.get("fact_alerts", []))), "Alertas operativas disponibles")
with metric_cols[2]:
    render_metric_card("Riesgo meteo alto/medio", str(medium_alerts), "Fact weather operational")
with metric_cols[3]:
    render_metric_card("Retraso medio actual", f"{avg_delay:.1f}h", "Sobre rutas con contexto")
with metric_cols[4]:
    render_metric_card("Servicios NOK", str(nok_count), "Contenedores con incidencia")
with metric_cols[5]:
    render_metric_card("Vehiculos impactados", str(critical_alerts), "Alertas severidad >= 4")

tab_overview, tab_diagrams, tab_map, tab_ops = st.tabs([
    "Resumen KDD",
    "Diagramas",
    "Mapa y Alertas",
    "Servicios",
])

with tab_overview:
    st.subheader("Requisitos y evidencias de la rubrica")
    st.markdown(
        """
        - Ingesta real con **NiFi + Kafka** usando `datos_crudos` y `datos_filtrados`.
        - Persistencia en **HDFS/Hive** con staging, dimensiones y facts curadas.
        - Analitica de **GraphFrames** con hops y criticidad por grado.
        - Baja latencia en **Cassandra** con `vehicle_latest_state`.
        - Orquestacion en **Airflow** con DAG microbatch y DAG mensual.
        - Defensa oficial sobre **Docker/local** con estrategia de **micro-batch documentado**.
        """
    )

    if bundle.get("errors"):
        st.warning("Datasets no disponibles en este refresco:")
        for error in bundle["errors"]:
            st.write(f"- {error}")

    overview_left, overview_right = st.columns([1.55, 1])
    with overview_left:
        render_html_card(
            """
            <div class='tiny-label'>ruta de defensa</div>
            <h3 style='margin:0.3rem 0 0.7rem 0'>Micro-batch documentado</h3>
            <p style='color:#9db2d5'>La demo se centra en Docker/local con NiFi, Kafka, HDFS/Hive, Spark, GraphFrames, Cassandra y Airflow, manteniendo streaming real como evidencia tecnica complementaria.</p>
            <ul>
              <li>staging y curated persistidos en Hive/HDFS</li>
              <li>alertas operativas y riesgo de red demostrables</li>
              <li>estado de vehiculos en Cassandra para baja latencia</li>
            </ul>
            """,
            height=230,
        )
    with overview_right:
        latest_weather = pd.DataFrame(bundle.get("fact_weather_operational", []))
        if not latest_weather.empty:
            top = latest_weather.iloc[0]
            render_html_card(
                f"""
                <div class='tiny-label'>resumen ejecutivo</div>
                <h3 style='margin:0.3rem 0 0.6rem 0'>{top['port_ref']} / {top['route_id']}</h3>
                <p><strong>Estado:</strong> {top['port_operational_status']}</p>
                <p><strong>Riesgo:</strong> {top['weather_risk_level']}</p>
                <p><strong>Retraso estimado:</strong> {top['weather_delay_hours_estimate']} horas</p>
                <p><strong>Accion:</strong> {top['recommended_action']}</p>
                """,
                height=230,
            )

    c1, c2 = st.columns([1.1, 0.9])
    with c1:
        st.subheader("Alertas operativas")
        st.dataframe(pd.DataFrame(bundle.get("fact_alerts", [])), use_container_width=True, hide_index=True)
    with c2:
        st.subheader("Estado operativo por vehiculo")
        st.dataframe(pd.DataFrame(bundle.get("ships_latest", [])), use_container_width=True, hide_index=True)

with tab_diagrams:
    render_mermaid("Flujo KDD", KDD_MERMAID)
    render_mermaid("Diagrama de Secuencia", SEQUENCE_MERMAID)
    render_mermaid("Diagrama de Clases", CLASS_MERMAID)
    render_mermaid("Casos de Uso", USE_CASE_MERMAID)

with tab_map:
    st.subheader("Seguimiento de flota y alertas de ruta")
    ports_df, routes_df, ships_df = build_map_data(bundle)
    layers = []
    if not routes_df.empty:
        layers.append(
            pdk.Layer(
                "LineLayer",
                data=routes_df,
                get_source_position="[origin_lon, origin_lat]",
                get_target_position="[dest_lon, dest_lat]",
                get_color="color",
                get_width=6,
                pickable=True,
            )
        )
    if not ports_df.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=ports_df,
                get_position="[lon, lat]",
                get_radius=18000,
                get_fill_color=[15, 118, 110],
                pickable=True,
            )
        )
    if not ships_df.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=ships_df,
                get_position="[lon, lat]",
                get_radius=24000,
                get_fill_color=[220, 38, 38],
                pickable=True,
            )
        )

    if layers:
        st.pydeck_chart(
            pdk.Deck(
                map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
                initial_view_state=pdk.ViewState(latitude=36.5, longitude=3.0, zoom=2.2, pitch=20),
                layers=layers,
                tooltip={"text": "{port_name}{ship_id}{tooltip}"},
            ),
            use_container_width=True,
        )

    col_left, col_right = st.columns([1.15, 0.85])
    with col_left:
        st.subheader("Ultimo estado de barcos")
        st.dataframe(ships_df, use_container_width=True, hide_index=True)
    with col_right:
        st.subheader("Clima y operacion")
        st.dataframe(pd.DataFrame(bundle.get("fact_weather_operational", [])), use_container_width=True, hide_index=True)

    network_left, network_right = st.columns([1.2, 0.8])
    with network_left:
        st.subheader("Red logistica activa")
        st.dataframe(pd.DataFrame(bundle.get("graph_centrality", [])), use_container_width=True, hide_index=True)
    with network_right:
        st.subheader("Alertas por puerto")
        st.dataframe(pd.DataFrame(bundle.get("fact_alerts", [])), use_container_width=True, hide_index=True)

with tab_ops:
    st.subheader("Estado y control de servicios")
    for row in service_rows:
        cols = st.columns([2, 3, 2, 1, 1])
        cols[0].write(f"**{row['service']}**")
        cols[1].write(row["status"])
        cols[2].markdown(
            f"<span class='status-pill {build_service_badge(row['badge'])}'>{row['badge']}</span>",
            unsafe_allow_html=True,
        )
        if cols[3].button("On", key=f"start_{row['service']}"):
            st.code(compose_service_action(row["service"] if row["service"] != "airflow" else "airflow-webserver", "start"))
            st.rerun()
        if cols[4].button("Off", key=f"stop_{row['service']}"):
            st.code(compose_service_action(row["service"] if row["service"] != "airflow" else "airflow-webserver", "stop"))
            st.rerun()

    ops_left, ops_right = st.columns([1, 1])
    with ops_left:
        st.subheader("GraphFrames")
        st.dataframe(pd.DataFrame(bundle.get("graph_centrality", [])), use_container_width=True, hide_index=True)
    with ops_right:
        st.subheader("Fact weather operational")
        st.dataframe(pd.DataFrame(bundle.get("fact_weather_operational", [])), use_container_width=True, hide_index=True)
