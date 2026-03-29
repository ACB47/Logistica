from __future__ import annotations

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
    components.html(
        f"""
        <div style=\"background:#0f172a;padding:16px;border-radius:18px;border:1px solid #1e293b;\">
          <h3 style=\"color:white;font-family:Inter,sans-serif;\">{title}</h3>
          <pre class=\"mermaid\">{code}</pre>
        </div>
        <script type=\"module\">
          import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
          mermaid.initialize({{ startOnLoad: true, theme: 'dark' }});
        </script>
        """,
        height=420,
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
        "python",
        "/home/jovyan/jobs/spark/99_dashboard_bundle.py",
    ]
    result = run_command(command, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or "No se pudo consultar el bundle del dashboard")
    return json.loads(result.stdout.strip() or "{}")


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


st.set_page_config(page_title="Dashboard KDD Logistica", layout="wide")

st.title("Dashboard KDD Logistica Maritima")
st.caption("Panel de defensa: arquitectura, diagramas, estado de servicios, mapa GPS y alertas operativas")

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

metric_cols = st.columns(4)
metric_cols[0].metric("Servicios OK", ok_count)
metric_cols[1].metric("Servicios NOK", nok_count)
metric_cols[2].metric("Servicios OFF", off_count)
metric_cols[3].metric("Alertas operativas", len(bundle.get("fact_alerts", [])))

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

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Alertas operativas")
        st.dataframe(pd.DataFrame(bundle.get("fact_alerts", [])), use_container_width=True, hide_index=True)
    with c2:
        st.subheader("Fact weather operational")
        st.dataframe(pd.DataFrame(bundle.get("fact_weather_operational", [])), use_container_width=True, hide_index=True)

with tab_diagrams:
    render_mermaid("Flujo KDD", KDD_MERMAID)
    render_mermaid("Diagrama de Secuencia", SEQUENCE_MERMAID)
    render_mermaid("Diagrama de Clases", CLASS_MERMAID)
    render_mermaid("Casos de Uso", USE_CASE_MERMAID)

with tab_map:
    st.subheader("Mapa de barcos y rutas con alertas")
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

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Ultimo estado de barcos")
        st.dataframe(ships_df, use_container_width=True, hide_index=True)
    with col_right:
        st.subheader("Alertas por puerto")
        st.dataframe(pd.DataFrame(bundle.get("fact_alerts", [])), use_container_width=True, hide_index=True)

with tab_ops:
    st.subheader("Estado y control de servicios")
    for row in service_rows:
        cols = st.columns([2, 3, 2, 1, 1])
        cols[0].write(f"**{row['service']}**")
        cols[1].write(row["status"])
        badge_color = {"OK": "🟢", "NOK": "🔴", "BOOT": "🟠", "OFF": "⚫"}.get(row["badge"], "⚪")
        cols[2].write(f"{badge_color} {row['badge']}")
        if cols[3].button("On", key=f"start_{row['service']}"):
            st.code(compose_service_action(row["service"] if row["service"] != "airflow" else "airflow-webserver", "start"))
            st.rerun()
        if cols[4].button("Off", key=f"stop_{row['service']}"):
            st.code(compose_service_action(row["service"] if row["service"] != "airflow" else "airflow-webserver", "stop"))
            st.rerun()

    st.subheader("GraphFrames")
    st.dataframe(pd.DataFrame(bundle.get("graph_centrality", [])), use_container_width=True, hide_index=True)
