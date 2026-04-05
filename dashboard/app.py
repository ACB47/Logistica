from __future__ import annotations

import json
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from math import isnan

import pandas as pd
import plotly.express as px
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

SHIP_CONSTRAINTS = {
    "Huelga": 72,
    "Tormenta": 96,
    "Problemas geopoliticos": 120,
    "Piratas": 144,
    "Problemas tecnicos del barco": 60,
    "Huelga de operaciones portuarias": 84,
}

CUSTOM_CSS = """
<style>
  .stApp {
    background: linear-gradient(180deg, #eef4fb 0%, #dfeaf7 45%, #d5e4f5 100%);
    color: #10233f;
  }
  .block-container {
    padding-top: 1rem;
    padding-bottom: 2rem;
    max-width: 98%;
  }
  h1, h2, h3, h4, h5, h6, p, li, span, label, div {
    color: #10233f;
    font-family: Inter, sans-serif;
  }
  .hero-eyebrow {
    color: #0f766e !important;
    font-size: 0.78rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    margin-bottom: 0.2rem;
    font-weight: 700;
  }
  .hero-title {
    font-size: 4rem;
    line-height: 0.94;
    font-weight: 800;
    margin: 0;
    color: #10233f !important;
  }
  .hero-subtitle {
    color: #38597c !important;
    font-size: 1rem;
    margin-top: 0.8rem;
    max-width: 760px;
  }
  .tag-chip {
    display: inline-block;
    padding: 8px 14px;
    margin: 6px 8px 0 0;
    border-radius: 999px;
    border: 1px solid rgba(15, 76, 129, 0.15);
    background: rgba(255, 255, 255, 0.65);
    color: #17345c !important;
    font-size: 0.86rem;
  }
  .card {
    background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(240,246,252,0.95));
    border: 1px solid rgba(15, 76, 129, 0.12);
    border-radius: 22px;
    padding: 18px 20px;
    box-shadow: 0 14px 38px rgba(15, 23, 42, 0.08);
  }
  .card-title {
    color: #567697 !important;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.15em;
  }
  .card-value {
    color: #10233f !important;
    font-size: 3rem;
    font-weight: 800;
    margin-top: 0.25rem;
  }
  .card-sub {
    color: #4b617c !important;
    margin-top: 0.2rem;
    font-size: 0.94rem;
  }
  div[data-baseweb="tab-list"] button {
    color: #17345c !important;
    background: rgba(255,255,255,0.5) !important;
    border-radius: 14px 14px 0 0 !important;
  }
  div[data-baseweb="tab-list"] button[aria-selected="true"] {
    background: rgba(255,255,255,0.95) !important;
    color: #0f172a !important;
  }
  .stButton button {
    color: #10233f !important;
    background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(233,241,250,0.98)) !important;
    border: 1px solid rgba(15,76,129,0.16) !important;
  }
  .stDataFrame, .stTable {
    border-radius: 16px;
    overflow: hidden;
  }
  .status-pill {
    display: inline-block;
    padding: 6px 12px;
    border-radius: 999px;
    font-size: 0.8rem;
    font-weight: 700;
  }
  .ok { background: rgba(34,197,94,0.15); color: #166534 !important; }
  .warn { background: rgba(245,158,11,0.18); color: #92400e !important; }
  .nok { background: rgba(239,68,68,0.16); color: #991b1b !important; }
  .off { background: rgba(148,163,184,0.16); color: #334155 !important; }
</style>
"""


def run_command(command: list[str], timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=timeout)


def render_card(title: str, value: str, subtitle: str, height: int = 155) -> None:
    components.html(
        f"""
        <div class="card">
          <div class="card-title">{title}</div>
          <div class="card-value">{value}</div>
          <div class="card-sub">{subtitle}</div>
        </div>
        """,
        height=height,
    )


def render_panel(title: str, subtitle: str, body: str, height: int = 230) -> None:
    components.html(
        f"""
        <div class="card">
          <div class="card-title">{subtitle}</div>
          <h3 style="margin:0.2rem 0 0.8rem 0;color:#10233f;">{title}</h3>
          <div style="color:#38597c;">{body}</div>
        </div>
        """,
        height=height,
    )


def render_diagram(title: str, subtitle: str, svg: str, height: int = 420) -> None:
    components.html(
        f"""
        <div class="card" style="padding:18px;">
          <div class="card-title">{subtitle}</div>
          <h3 style="margin:0.2rem 0 0.8rem 0;color:#10233f;">{title}</h3>
          <div style="background:rgba(15,76,129,0.04);border-radius:16px;padding:10px;overflow:auto;">{svg}</div>
        </div>
        """,
        height=height,
    )


def flow_diagram_svg() -> str:
    return """
    <svg viewBox="0 0 1200 360" width="100%" height="360" xmlns="http://www.w3.org/2000/svg">
      <defs><marker id="a" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="#0f4c81"/></marker></defs>
      <style>
        .n{fill:#ffffff;stroke:#7dd3fc;stroke-width:2;rx:18;ry:18}
        .t{fill:#10233f;font:600 14px Inter,sans-serif;text-anchor:middle;dominant-baseline:middle}
        .l{stroke:#0f4c81;stroke-width:3;fill:none;marker-end:url(#a)}
      </style>
      <rect class="n" x="20" y="54" width="150" height="56"/><text class="t" x="95" y="82">Open-Meteo</text>
      <rect class="n" x="210" y="54" width="115" height="56"/><text class="t" x="267" y="82">NiFi</text>
      <rect class="n" x="360" y="18" width="180" height="56"/><text class="t" x="450" y="46">Kafka datos_crudos</text>
      <rect class="n" x="360" y="110" width="180" height="56"/><text class="t" x="450" y="138">Kafka datos_filtrados</text>
      <rect class="n" x="590" y="18" width="170" height="56"/><text class="t" x="675" y="46">HDFS raw</text>
      <rect class="n" x="590" y="110" width="190" height="56"/><text class="t" x="685" y="138">Spark staging clima</text>
      <rect class="n" x="840" y="18" width="190" height="56"/><text class="t" x="935" y="46">Hive / HDFS curated</text>
      <rect class="n" x="840" y="110" width="190" height="56"/><text class="t" x="935" y="138">GraphFrames + facts</text>
      <rect class="n" x="840" y="202" width="190" height="56"/><text class="t" x="935" y="230">Cassandra latest state</text>
      <rect class="n" x="1080" y="110" width="100" height="56"/><text class="t" x="1130" y="138">Airflow</text>
      <path class="l" d="M170 82 L210 82"/><path class="l" d="M325 82 L360 46"/><path class="l" d="M325 82 L360 138"/>
      <path class="l" d="M540 46 L590 46"/><path class="l" d="M540 138 L590 138"/>
      <path class="l" d="M780 138 L840 46"/><path class="l" d="M780 138 L840 138"/><path class="l" d="M780 138 L840 230"/>
      <path class="l" d="M1030 46 L1080 138"/><path class="l" d="M1030 138 L1080 138"/><path class="l" d="M1030 230 L1080 138"/>
    </svg>
    """


def sequence_diagram_svg() -> str:
    return """
    <svg viewBox="0 0 1120 360" width="100%" height="360" xmlns="http://www.w3.org/2000/svg">
      <defs><marker id="b" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="#0f4c81"/></marker></defs>
      <style>
        .box{fill:#ffffff;stroke:#7dd3fc;stroke-width:2;rx:12}
        .txt{fill:#10233f;font:600 13px Inter,sans-serif;text-anchor:middle}
        .line{stroke:#94a3b8;stroke-width:1.5;stroke-dasharray:5 5}
        .msg{stroke:#0f4c81;stroke-width:2.5;marker-end:url(#b)}
        .lbl{fill:#10233f;font:12px Inter,sans-serif}
      </style>
      <rect class="box" x="20" y="20" width="100" height="34"/><text class="txt" x="70" y="42">API</text>
      <rect class="box" x="170" y="20" width="100" height="34"/><text class="txt" x="220" y="42">NiFi</text>
      <rect class="box" x="320" y="20" width="100" height="34"/><text class="txt" x="370" y="42">Kafka</text>
      <rect class="box" x="470" y="20" width="100" height="34"/><text class="txt" x="520" y="42">Spark</text>
      <rect class="box" x="620" y="20" width="100" height="34"/><text class="txt" x="670" y="42">Hive</text>
      <rect class="box" x="770" y="20" width="120" height="34"/><text class="txt" x="830" y="42">Cassandra</text>
      <rect class="box" x="950" y="20" width="120" height="34"/><text class="txt" x="1010" y="42">Airflow</text>
      <line class="line" x1="70" y1="54" x2="70" y2="330"/><line class="line" x1="220" y1="54" x2="220" y2="330"/><line class="line" x1="370" y1="54" x2="370" y2="330"/><line class="line" x1="520" y1="54" x2="520" y2="330"/><line class="line" x1="670" y1="54" x2="670" y2="330"/><line class="line" x1="830" y1="54" x2="830" y2="330"/><line class="line" x1="1010" y1="54" x2="1010" y2="330"/>
      <line class="msg" x1="70" y1="100" x2="220" y2="100"/><text class="lbl" x="105" y="90">GET weather snapshot</text>
      <line class="msg" x1="220" y1="145" x2="370" y2="145"/><text class="lbl" x="245" y="135">raw / filtered</text>
      <line class="msg" x1="370" y1="190" x2="520" y2="190"/><text class="lbl" x="405" y="180">consume topic</text>
      <line class="msg" x1="520" y1="235" x2="670" y2="235"/><text class="lbl" x="555" y="225">staging y facts</text>
      <line class="msg" x1="520" y1="280" x2="830" y2="280"/><text class="lbl" x="610" y="270">vehicle_latest_state</text>
      <line class="msg" x1="1010" y1="315" x2="520" y2="315"/><text class="lbl" x="760" y="305">orquesta micro-batch</text>
    </svg>
    """


def class_diagram_svg() -> str:
    return """
    <svg viewBox="0 0 1120 390" width="100%" height="390" xmlns="http://www.w3.org/2000/svg">
      <defs><marker id="c" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="#0f4c81"/></marker></defs>
      <style>
        .box{fill:#ffffff;stroke:#7dd3fc;stroke-width:2;rx:18}
        .title{fill:#10233f;font:700 14px Inter,sans-serif}
        .item{fill:#334155;font:12px Inter,sans-serif}
        .link{stroke:#0f4c81;stroke-width:2.5;fill:none;marker-end:url(#c)}
      </style>
      <rect class="box" x="20" y="24" width="220" height="130"/><text class="title" x="40" y="50">Port</text><text class="item" x="40" y="78">+ port_id</text><text class="item" x="40" y="98">+ port_name</text><text class="item" x="40" y="118">+ macro_region</text><text class="item" x="40" y="138">+ lat / lon</text>
      <rect class="box" x="290" y="24" width="220" height="150"/><text class="title" x="310" y="50">Route</text><text class="item" x="310" y="78">+ route_id</text><text class="item" x="310" y="98">+ origin_port</text><text class="item" x="310" y="118">+ dest_port</text><text class="item" x="310" y="138">+ route_mode</text><text class="item" x="310" y="158">+ sea_hours_estimate</text>
      <rect class="box" x="560" y="24" width="240" height="150"/><text class="title" x="580" y="50">WeatherEvent</text><text class="item" x="580" y="78">+ event_ts</text><text class="item" x="580" y="98">+ temperature_c</text><text class="item" x="580" y="118">+ humidity_pct</text><text class="item" x="580" y="138">+ wind_speed_kmh</text><text class="item" x="580" y="158">+ severity</text>
      <rect class="box" x="850" y="24" width="240" height="150"/><text class="title" x="870" y="50">OperationalAlert</text><text class="item" x="870" y="78">+ via_port</text><text class="item" x="870" y="98">+ risk_level</text><text class="item" x="870" y="118">+ stock_status</text><text class="item" x="870" y="138">+ recommendation</text><text class="item" x="870" y="158">+ severity</text>
      <rect class="box" x="420" y="220" width="280" height="120"/><text class="title" x="440" y="246">VehicleLatestState</text><text class="item" x="440" y="274">+ ship_id</text><text class="item" x="440" y="294">+ route_id</text><text class="item" x="440" y="314">+ dest_port</text><text class="item" x="440" y="334">+ stock_on_hand</text>
      <path class="link" d="M240 88 C260 88, 270 88, 290 88"/>
      <path class="link" d="M510 100 C530 100, 540 100, 560 100"/>
      <path class="link" d="M800 100 C820 100, 830 100, 850 100"/>
      <path class="link" d="M510 145 C545 145, 520 220, 560 220"/>
    </svg>
    """


def use_case_diagram_svg() -> str:
    return """
    <svg viewBox="0 0 1080 370" width="100%" height="370" xmlns="http://www.w3.org/2000/svg">
      <style>
        .actor{fill:#ffffff;stroke:#17345c;stroke-width:2}
        .uc{fill:#ffffff;stroke:#7dd3fc;stroke-width:2}
        .ucTxt{fill:#10233f;font:13px Inter,sans-serif;text-anchor:middle;dominant-baseline:middle}
        .lab{fill:#17345c;font:13px Inter,sans-serif}
        .line{stroke:#0f4c81;stroke-width:2}
      </style>
      <circle class="actor" cx="100" cy="70" r="22"/><line class="actor" x1="100" y1="92" x2="100" y2="145"/><line class="actor" x1="72" y1="112" x2="128" y2="112"/><line class="actor" x1="100" y1="145" x2="76" y2="182"/><line class="actor" x1="100" y1="145" x2="124" y2="182"/><text class="lab" x="35" y="220">Profesor / Tribunal</text>
      <ellipse class="uc" cx="410" cy="60" rx="170" ry="30"/><text class="ucTxt" x="410" y="60">Ver arquitectura y flujo KDD</text>
      <ellipse class="uc" cx="410" cy="120" rx="170" ry="30"/><text class="ucTxt" x="410" y="120">Ver estado de servicios</text>
      <ellipse class="uc" cx="410" cy="180" rx="170" ry="30"/><text class="ucTxt" x="410" y="180">Ver mapa de barcos y alertas</text>
      <ellipse class="uc" cx="760" cy="90" rx="170" ry="30"/><text class="ucTxt" x="760" y="90">Consultar facts y Cassandra</text>
      <ellipse class="uc" cx="760" cy="170" rx="170" ry="30"/><text class="ucTxt" x="760" y="170">Arrancar / parar servicios</text>
      <ellipse class="uc" cx="760" cy="250" rx="170" ry="30"/><text class="ucTxt" x="760" y="250">Explicar grafos y nodos criticos</text>
      <line class="line" x1="128" y1="112" x2="240" y2="60"/>
      <line class="line" x1="128" y1="112" x2="240" y2="120"/>
      <line class="line" x1="128" y1="112" x2="240" y2="180"/>
      <line class="line" x1="580" y1="90" x2="590" y2="90"/>
      <line class="line" x1="580" y1="120" x2="590" y2="170"/>
      <line class="line" x1="580" y1="180" x2="590" y2="250"/>
    </svg>
    """


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
    output_file = ROOT / "jobs" / "dashboard_bundle_output.json"
    command = DOCKER_COMPOSE + [
        "exec",
        "-T",
        "spark",
        "spark-submit",
        "/home/jovyan/jobs/spark/99_dashboard_bundle.py",
    ]
    result = run_command(command, timeout=600)
    if result.returncode != 0 and not output_file.exists():
        raise RuntimeError(result.stderr or result.stdout or "No se pudo consultar el bundle del dashboard")

    if not output_file.exists():
        raise RuntimeError("No se genero el archivo dashboard_bundle_output.json")

    return json.loads(output_file.read_text(encoding="utf-8"))


def reset_dashboard_cache() -> None:
    st.cache_data.clear()
    st.rerun()


def build_map_data(bundle: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ports = pd.DataFrame(bundle.get("dim_ports", []))
    routes = pd.DataFrame(bundle.get("dim_routes", []))
    ships = pd.DataFrame(bundle.get("ships_latest", []))
    alerts = pd.DataFrame(bundle.get("fact_alerts", []))

    if not routes.empty and not ports.empty:
        origin_lookup = ports[["port_name", "lat", "lon"]].rename(columns={"lat": "origin_lat", "lon": "origin_lon"})
        routes = routes.merge(origin_lookup, left_on="origin_port", right_on="port_name", how="left").drop(columns=["port_name"])
        dest_lookup = ports[["port_name", "lat", "lon"]].rename(columns={"port_name": "dest_port", "lat": "dest_lat", "lon": "dest_lon"})
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

    return ports, routes, ships, alerts


def summarize_risk(bundle: dict) -> tuple[int, int, float]:
    alerts = pd.DataFrame(bundle.get("fact_alerts", []))
    weather = pd.DataFrame(bundle.get("fact_weather_operational", []))
    critical = int((alerts["severity"] >= 4).sum()) if not alerts.empty else 0
    medium = int((alerts["severity"] >= 2).sum()) if not alerts.empty else 0
    avg_delay = float(weather["weather_delay_hours_estimate"].mean()) if not weather.empty else 0.0
    return critical, medium, avg_delay


def enrich_ship_eta_dates(ships_df: pd.DataFrame) -> pd.DataFrame:
    if ships_df.empty or "eta_hours_estimate" not in ships_df.columns:
        return ships_df
    ships_df = ships_df.copy()
    base_now = datetime.now()
    ships_df["eta_fecha"] = ships_df["eta_hours_estimate"].apply(
        lambda hours: (base_now + timedelta(hours=float(hours))).strftime("%Y-%m-%d %H:%M") if pd.notna(hours) else "N/A"
    )
    if "voyage_days_elapsed" in ships_df.columns:
        ships_df["fecha_salida_origen"] = ships_df["voyage_days_elapsed"].apply(
            lambda days: (base_now - timedelta(hours=float(days) * 24)).strftime("%Y-%m-%d %H:%M") if pd.notna(days) else "N/A"
        )
    return ships_df


def build_control_tower_kpis(bundle: dict) -> dict[str, float | int | str]:
    stock = pd.DataFrame(bundle.get("stock_valladolid", []))
    ships = pd.DataFrame(bundle.get("ships_latest", []))
    orders = pd.DataFrame(bundle.get("customer_orders_douai", []))
    air = pd.DataFrame(bundle.get("fact_air_recovery_options", []))

    if not stock.empty:
        order_totals = (
            orders.groupby("article_ref", as_index=False)["ordered_pieces"].sum()
            if not orders.empty
            else pd.DataFrame(columns=["article_ref", "ordered_pieces"])
        )
        if not order_totals.empty:
            stock = stock.merge(order_totals, on="article_ref", how="left")
        else:
            stock["ordered_pieces"] = 0
        stock["ordered_pieces"] = stock["ordered_pieces"].fillna(0)
        stock["available_after_orders"] = stock["total_stock_pieces"] - stock["ordered_pieces"]
        stock["doh"] = (stock["available_after_orders"] / stock["daily_consumption_avg"]).round(1)
        doh = round(float(stock["doh"].mean()), 1)
        security_level = round(float((stock["safety_stock_min"] / stock["total_stock_pieces"]).mean() * 100), 1)
        critical_refs = int((stock["available_after_orders"] <= stock["safety_stock_min"]).sum())
    else:
        doh = 0.0
        security_level = 0.0
        critical_refs = 0

    eta_avg = round(float(ships["eta_hours_estimate"].mean()), 1) if not ships.empty else 0.0
    otif = 100.0
    if not orders.empty and "order_status" in orders.columns:
        otif = round(float((orders["order_status"] == "sea_committed").mean() * 100), 1)

    if not air.empty and "time_saved_hours" in air.columns:
        avg_time_saved = round(float(air["time_saved_hours"].mean()), 1)
        best_mode = str(air.iloc[0].get("recommended_mode", "N/A"))
    else:
        avg_time_saved = 0.0
        best_mode = "N/A"

    return {
        "doh": doh,
        "otif": otif,
        "eta_avg": eta_avg,
        "security_level": security_level,
        "critical_refs": critical_refs,
        "avg_time_saved": avg_time_saved,
        "best_mode": best_mode,
    }


def build_ship_simulation(bundle: dict, ship_id: str | None, active_constraints: list[str]) -> dict[str, object] | None:
    if not ship_id or ship_id == "Ninguno":
        return None

    ships = pd.DataFrame(bundle.get("ships_latest", []))
    recovery = pd.DataFrame(bundle.get("fact_air_recovery_options", []))
    if ships.empty or recovery.empty:
        return None

    ship_match = ships[ships["ship_id"] == ship_id]
    recovery_match = recovery[recovery["ship_id"] == ship_id]
    if ship_match.empty or recovery_match.empty:
        return None

    ship_row = ship_match.iloc[0]
    recovery_row = recovery_match.iloc[0]
    extra_hours = sum(SHIP_CONSTRAINTS[name] for name in active_constraints)
    base_eta = float(recovery_row.get("ship_remaining_hours", ship_row.get("eta_hours_estimate", 0.0)) or 0.0)
    recalculated_eta = round(base_eta + extra_hours, 1)
    hours_until_stock_break = float(recovery_row.get("hours_until_stock_break", 0.0) or 0.0)
    margin_hours = round(hours_until_stock_break - recalculated_eta, 1)
    cover_status = "NO CUBRE" if margin_hours < 96 else "CUBRE"
    air_eta = float(recovery_row.get("air_total_eta_hours", 0.0) or 0.0)
    air_cost = float(recovery_row.get("air_total_cost_eur", 0.0) or 0.0)
    recommendation = "AEREO_CAMION" if cover_status == "NO CUBRE" else "MARITIMO"

    return {
        "ship_id": ship_id,
        "origin_port": ship_row.get("origin_port"),
        "dest_port": ship_row.get("dest_port"),
        "base_eta": base_eta,
        "extra_hours": extra_hours,
        "recalculated_eta": recalculated_eta,
        "hours_until_stock_break": hours_until_stock_break,
        "margin_hours": margin_hours,
        "cover_status": cover_status,
        "recommended_mode": recommendation,
        "air_eta": air_eta,
        "air_cost": air_cost,
        "constraints": active_constraints,
    }


def build_service_badge(badge: str) -> str:
    return {
        "OK": "ok",
        "NOK": "nok",
        "BOOT": "warn",
        "OFF": "off",
    }.get(badge, "off")


def build_stock_table(bundle: dict, customer_city: str | None = None, industrial_week: str | None = None) -> pd.DataFrame:
    stock = pd.DataFrame(bundle.get("stock_valladolid", []))
    orders = filter_orders(bundle, customer_city, industrial_week)
    if stock.empty:
        return stock

    if not orders.empty:
        order_totals = orders.groupby("article_ref", as_index=False)["ordered_pieces"].sum()
        order_totals = order_totals.rename(columns={"ordered_pieces": "douai_ordered_pieces"})
        stock = stock.merge(order_totals, on="article_ref", how="left")
    else:
        stock["douai_ordered_pieces"] = 0

    stock["douai_ordered_pieces"] = stock["douai_ordered_pieces"].fillna(0).astype(int)
    stock["available_after_orders"] = stock["total_stock_pieces"] - stock["douai_ordered_pieces"]
    stock["stock_status"] = stock.apply(
        lambda row: "ROJO"
        if row["available_after_orders"] <= row["safety_stock_min"]
        else "NARANJA"
        if row["available_after_orders"] <= row["safety_stock_min"] + row["daily_consumption_avg"] * 2
        else "VERDE",
        axis=1,
    )
    stock["status_dot"] = stock["stock_status"].map({"VERDE": "🟢", "NARANJA": "🟠", "ROJO": "🔴"})
    stock = stock.rename(
        columns={
            "article_ref": "Referencia articulo",
            "article_name": "Designacion articulo",
            "total_stock_packs": "Cantidad total embalajes",
            "total_stock_pieces": "Cantidad total piezas",
            "pieces_per_pack": "Piezas por embalaje",
            "daily_consumption_avg": "Consumo medio diario",
            "safety_stock_min": "Stock minimo seguridad",
            "douai_ordered_pieces": "Pedido cliente Douai (piezas)",
            "available_after_orders": "Disponible tras pedido",
            "status_dot": "Estado",
            "stock_status": "Status Stock",
        }
    )
    return stock[
        [
            "Estado",
            "Status Stock",
            "Referencia articulo",
            "Designacion articulo",
            "Piezas por embalaje",
            "Consumo medio diario",
            "Cantidad total embalajes",
            "Cantidad total piezas",
            "Stock minimo seguridad",
        ]
    ]


def filter_orders(bundle: dict, customer_city: str | None = None, industrial_week: str | None = None) -> pd.DataFrame:
    orders = pd.DataFrame(bundle.get("customer_orders_douai", []))
    if orders.empty:
        return orders
    if customer_city and customer_city != "Todos":
        orders = orders[orders["customer_city"] == customer_city]
    if industrial_week:
        orders = orders[orders["industrial_week"] == industrial_week]
    return orders


def build_stock_context(bundle: dict, customer_city: str | None = None, industrial_week: str | None = None) -> pd.DataFrame:
    stock = pd.DataFrame(bundle.get("stock_valladolid", []))
    orders = filter_orders(bundle, customer_city, industrial_week)
    if stock.empty:
        return stock

    if not orders.empty:
        order_totals = orders.groupby("article_ref", as_index=False)["ordered_pieces"].sum()
        order_totals = order_totals.rename(columns={"ordered_pieces": "douai_ordered_pieces"})
        stock = stock.merge(order_totals, on="article_ref", how="left")
    else:
        stock["douai_ordered_pieces"] = 0

    stock["douai_ordered_pieces"] = stock["douai_ordered_pieces"].fillna(0).astype(int)
    stock["available_after_orders"] = stock["total_stock_pieces"] - stock["douai_ordered_pieces"]
    stock["stock_status"] = stock.apply(
        lambda row: "ROJO"
        if row["available_after_orders"] <= row["safety_stock_min"]
        else "NARANJA"
        if row["available_after_orders"] <= row["safety_stock_min"] + row["daily_consumption_avg"] * 2
        else "VERDE",
        axis=1,
    )
    stock["hours_to_stockout"] = stock.apply(
        lambda row: round(max(0.0, row["available_after_orders"]) / row["daily_consumption_avg"] * 24, 1)
        if row["daily_consumption_avg"] > 0
        else 0.0,
        axis=1,
    )
    return stock


def build_stock_bar_chart(bundle: dict, customer_city: str | None = None, industrial_week: str | None = None):
    stock_df = build_stock_table(bundle, customer_city, industrial_week)
    if stock_df.empty:
        return None

    color_map = {"VERDE": "#16a34a", "NARANJA": "#f59e0b", "ROJO": "#dc2626"}
    fig = px.bar(
        stock_df,
        x="Referencia articulo",
        y="Cantidad total piezas",
        color="Status Stock",
        color_discrete_map=color_map,
        text="Cantidad total piezas",
    )
    fig.update_layout(
        height=360,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.6)",
        font_color="#10233f",
        margin=dict(l=10, r=10, t=20, b=10),
        xaxis_title="Referencias",
        yaxis_title="Piezas en stock",
        legend_title="Semaforo stock",
        xaxis={"type": "category", "categoryorder": "array", "categoryarray": stock_df["Referencia articulo"].tolist()},
    )
    fig.update_xaxes(
        tickmode="array",
        tickvals=stock_df["Referencia articulo"].tolist(),
        ticktext=stock_df["Referencia articulo"].tolist(),
        tickangle=-45,
    )
    fig.update_traces(textposition="outside")
    return fig


def build_stock_rupture_gantt(bundle: dict, customer_city: str | None = None, industrial_week: str | None = None):
    stock = build_stock_context(bundle, customer_city, industrial_week)
    air = pd.DataFrame(bundle.get("fact_air_recovery_options", []))
    if stock.empty:
        return None

    now = datetime.now()
    rows: list[dict] = []
    for _, row in stock.iterrows():
        maritime = air[air["dest_port"] == row.get("inbound_port")]
        if not maritime.empty and pd.notna(maritime.iloc[0].get("ship_remaining_hours")):
            ship_eta = float(maritime.iloc[0]["ship_remaining_hours"])
        else:
            ship_eta = 240.0

        stockout_value = row.get("hours_to_stockout", 0.0)
        stockout_hours = float(stockout_value) if pd.notna(stockout_value) else 0.0
        stockout_date = now + timedelta(hours=stockout_hours)
        ship_arrival_date = now + timedelta(hours=ship_eta)
        rows.append(
            {
                "article_ref": row["article_ref"],
                "task_name": "Cobertura stock Valladolid",
                "start_date": now,
                "end_date": stockout_date,
                "industrial_week": "Stock window",
                "transport_mode": "stock",
            }
        )
        rows.append(
            {
                "article_ref": row["article_ref"],
                "task_name": "Llegada maritima estimada",
                "start_date": now,
                "end_date": ship_arrival_date,
                "industrial_week": "Sea ETA",
                "transport_mode": "sea_eta",
            }
        )

    gantt = pd.DataFrame(rows)
    fig = px.timeline(
        gantt,
        x_start="start_date",
        x_end="end_date",
        y="article_ref",
        color="transport_mode",
        text="task_name",
        hover_data=["industrial_week"],
        color_discrete_map={
            "stock": "#16a34a",
            "sea_eta": "#2563eb",
        },
    )
    fig.update_layout(
        height=430,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.6)",
        font_color="#10233f",
        margin=dict(l=10, r=10, t=20, b=20),
        xaxis_title="Calendario y semana industrial",
        yaxis_title="Referencia articulo",
        legend_title="Tipo de barra",
    )
    fig.update_yaxes(autorange="reversed")
    return fig


def build_air_contingency_table(bundle: dict, customer_city: str | None = None, industrial_week: str | None = None) -> pd.DataFrame:
    stock = build_stock_context(bundle, customer_city, industrial_week)
    orders_cov = build_orders_coverage(bundle, customer_city)
    air = pd.DataFrame(bundle.get("fact_air_recovery_options", []))
    if stock.empty or orders_cov.empty or air.empty:
        return pd.DataFrame()

    if industrial_week:
        orders_cov = orders_cov[orders_cov["industrial_week"] == industrial_week]

    risk_orders = orders_cov[orders_cov["cover_status"] == "NO CUBRE"].copy()
    if risk_orders.empty:
        return pd.DataFrame()

    stock_lookup = stock.set_index("article_ref") if "article_ref" in stock.columns else pd.DataFrame()
    rows = []
    for _, order in risk_orders.iterrows():
        article_ref = order["article_ref"]
        if article_ref not in stock_lookup.index:
            continue

        stock_row = stock_lookup.loc[article_ref]
        best_air = air[air["dest_port"] == stock_row["inbound_port"]]
        if best_air.empty:
            continue

        option = best_air.iloc[0]
        rows.append(
            {
                "Referencia": article_ref,
                "Pedido": order["order_id"],
                "Alternativa aerea": f"Shanghai -> {option['air_dest_city']} + camion Valladolid",
                "Llegada Valladolid": pd.to_datetime(order["arrival_valladolid"]).strftime("%Y-%m-%d %H:%M"),
                "Sobre coste aereo (EUR)": round(float(option["air_total_cost_eur"]), 2),
                "Cliente": order["customer_city"],
                "Semana": order["industrial_week"],
                "Cobertura": order["cover_status"],
            }
        )
    return pd.DataFrame(rows)


def format_iw(date_value: pd.Timestamp) -> str:
    week = int(date_value.isocalendar().week)
    return f"IW{week:02d}"


def build_orders_coverage(bundle: dict, customer_city: str | None = None) -> pd.DataFrame:
    orders = filter_orders(bundle, customer_city, None)
    stock = build_stock_context(bundle, customer_city, None)
    air = pd.DataFrame(bundle.get("fact_air_recovery_options", []))
    if orders.empty or stock.empty:
        return pd.DataFrame()

    stock_lookup = stock.set_index("article_ref") if "article_ref" in stock.columns else pd.DataFrame()
    rows = []
    now = datetime.now()
    for _, order in orders.iterrows():
        article_ref = order["article_ref"]
        if article_ref not in stock_lookup.index:
            continue
        stock_row = stock_lookup.loc[article_ref]
        best_option = air[air["dest_port"] == stock_row.get("inbound_port")]
        ship_eta = float(best_option.iloc[0]["ship_remaining_hours"]) if not best_option.empty and pd.notna(best_option.iloc[0].get("ship_remaining_hours")) else 240.0
        air_eta = float(best_option.iloc[0]["air_total_eta_hours"]) if not best_option.empty and pd.notna(best_option.iloc[0].get("air_total_eta_hours")) else ship_eta
        warehouse_arrival = now + timedelta(hours=air_eta if stock_row["stock_status"] == "ROJO" else ship_eta)
        need_date = pd.to_datetime(order["planned_delivery_date"])
        margin_days = round((need_date - warehouse_arrival).total_seconds() / 86400, 1)
        rows.append(
            {
                "order_id": order["order_id"],
                "article_ref": article_ref,
                "customer_city": order["customer_city"],
                "industrial_week": order["industrial_week"],
                "needed_date_client": need_date,
                "arrival_valladolid": warehouse_arrival,
                "margin_days": margin_days,
                "cover_status": "CUBRE" if margin_days >= 4 else "NO CUBRE",
                "recommended_mode": best_option.iloc[0]["recommended_mode"] if not best_option.empty else "MARITIMO",
                "air_total_cost_eur": float(best_option.iloc[0]["air_total_cost_eur"]) if not best_option.empty else 0.0,
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "order_id",
            "article_ref",
            "customer_city",
            "industrial_week",
            "needed_date_client",
            "arrival_valladolid",
            "margin_days",
            "cover_status",
            "recommended_mode",
            "air_total_cost_eur",
        ],
    )


def build_stock_horizon(bundle: dict, customer_city: str | None = None) -> pd.DataFrame:
    stock = build_stock_context(bundle, customer_city, None)
    orders_cov = build_orders_coverage(bundle, customer_city)
    raw_orders = filter_orders(bundle, customer_city, None)
    if stock.empty:
        return pd.DataFrame()

    base_week = 14
    weeks = [f"IW{week:02d}" for week in range(base_week, base_week + 10)]
    rows = []
    for _, stock_row in stock.iterrows():
        projected_stock = int(stock_row["total_stock_pieces"])
        safety_stock = int(stock_row["safety_stock_min"])
        daily_cons = int(stock_row["daily_consumption_avg"])
        weekly_consumption = daily_cons * 7
        pieces_per_pack = int(stock_row["pieces_per_pack"])

        for week in weeks:
            # Filtrar ordenes y llegadas para esta semana y esta referencia
            week_orders = orders_cov[(orders_cov["article_ref"] == stock_row["article_ref"]) & (orders_cov["industrial_week"] == week)]
            order_subset = raw_orders[(raw_orders["article_ref"] == stock_row["article_ref"]) & (raw_orders["industrial_week"] == week)]
            
            # Salidas: si hay pedidos usamos la suma, si no usamos el consumo semanal teorico
            outbound = int(order_subset["ordered_pieces"].sum()) if not order_subset.empty else weekly_consumption
            
            # Entradas: barcos que llegan en esta semana (IW)
            inbound = 0
            if not week_orders.empty:
                # Contamos cuantos barcos llegan segun arrival_valladolid
                arrival_matches = week_orders[week_orders["arrival_valladolid"].apply(lambda d: format_iw(pd.Timestamp(d)) == week)]
                inbound = int(len(arrival_matches) * pieces_per_pack)

            projected_stock = max(0, projected_stock + inbound - outbound)
            cover_status = "NO CUBRE" if projected_stock <= safety_stock else ("ALERTA" if projected_stock <= safety_stock + weekly_consumption else "CUBRE")
            
            rows.append(
                {
                    "Referencia": stock_row["article_ref"],
                    "Semana industrial": week,
                    "Stock inicial": int(stock_row["total_stock_pieces"]),
                    "Entrada semana": inbound,
                    "Salida pedidos": outbound,
                    "Stock proyectado": projected_stock,
                    "CUBRE/NO CUBRE": cover_status,
                }
            )
    return pd.DataFrame(rows)


def render_stock_horizon_table(bundle: dict, customer_city: str | None = None) -> None:
    from datetime import datetime as _dt
    stock = build_stock_context(bundle, customer_city, None)
    if stock.empty:
        st.info("Todavia no hay datos para calcular el horizonte semanal de stock.")
        return

    weeks = [f"IW{week:02d}" for week in range(14, 24)]
    horizon_df = build_stock_horizon(bundle, customer_city)

    stock_lookup = stock.set_index("article_ref")
    today_str = _dt.now().strftime("%d/%m/%Y")
    rows_html = []
    for article_ref in stock_lookup.index:
        article = stock_lookup.loc[article_ref]
        safety_stock = int(article["safety_stock_min"])
        daily_cons = int(article["daily_consumption_avg"])
        alert_threshold = safety_stock + (daily_cons * 7)

        week_cells = []
        worst_status = "CUBRE"
        for week in weeks:
            week_row = horizon_df[(horizon_df["Referencia"] == article_ref) & (horizon_df["Semana industrial"] == week)]
            if week_row.empty:
                pieces = max(0, int(article["total_stock_pieces"]))
            else:
                pieces = max(0, int(week_row.iloc[0]["Stock proyectado"]))

            if pieces <= safety_stock:
                status = "NO CUBRE"
                color = "#dc2626"
                icon = "✈"
                if worst_status == "CUBRE":
                    worst_status = "NO CUBRE"
            elif pieces <= alert_threshold:
                status = "ALERTA"
                color = "#f59e0b"
                icon = "✈"
                if worst_status == "CUBRE":
                    worst_status = "ALERTA"
            else:
                status = "CUBRE"
                color = "#16a34a"
                icon = "🚢"

            week_cells.append(
                f"<td style='text-align:center; min-width:88px; padding:8px 6px; background:#fff; border-radius:8px;'>"
                f"<div style='font-weight:800; font-size:1.1rem; color:#10233f'>{pieces}</div>"
                f"<div style='font-size:0.72rem; font-weight:700; color:{color}; margin-top:2px;'>{status}</div>"
                f"<div style='font-size:1.15rem; margin-top:2px;'>{icon}</div>"
                f"</td>"
            )

        transport_icon = "✈" if worst_status != "CUBRE" else "🚢"
        transport_color = "#dc2626" if worst_status == "NO CUBRE" else ("#f59e0b" if worst_status == "ALERTA" else "#16a34a")
        transport_label = "Aereo" if worst_status != "CUBRE" else "Maritimo"

        mode_cell = (
            f"<td style='text-align:center; min-width:80px; padding:8px 6px; background:#fff; border-radius:8px;'>"
            f"<div style='font-size:1.6rem;'>{transport_icon}</div>"
            f"<div style='font-size:0.72rem; font-weight:700; color:{transport_color}; margin-top:2px;'>{transport_label}</div>"
            f"</td>"
        )

        ref_cell = f"<td style='font-weight:700; padding:8px 10px; white-space:nowrap;'>{article_ref}</td>"
        rows_html.append(f"<tr>{ref_cell}{''.join(week_cells)}{mode_cell}</tr>")

    headers = (
        f"<th style='padding:10px 12px; text-align:left;'>Referencia</th>"
        + "".join([f"<th style='padding:10px 8px;'>{week}</th>" for week in weeks])
        + f"<th style='padding:10px 12px;'>Tipo de envio</th>"
    )
    html = f"""
    <div style='width:100%;'>
      <div class='card' style='padding:14px 16px; width:100%; border-radius:22px; box-sizing:border-box;'>
        <div class='card-title'>horizonte de stock</div>
        <h3 style='margin:0.15rem 0 0.6rem 0; color:#10233f; font-size:1.25rem;'>Cobertura por semanas industriales (Horizonte 10 semanas)</h3>
        <div style='overflow-x:auto; width:100%;'>
          <table style='width:100%; min-width:1300px; border-collapse:separate; border-spacing:4px 6px; font-family:Inter,sans-serif;'>
            <thead>
              <tr style='background:rgba(15,76,129,0.07); color:#0f4c81;'>{headers}</tr>
            </thead>
            <tbody>
              {''.join(rows_html)}
            </tbody>
          </table>
        </div>
        <div style='margin-top:12px; display:flex; gap:18px; color:#475569; font-size:0.85rem;'>
          <span>🚢 <b style='color:#16a34a'>CUBRE</b> = stock suficiente (maritimo)</span>
          <span>✈ <b style='color:#f59e0b'>ALERTA</b> = preparar envio aereo</span>
          <span>✈ <b style='color:#dc2626'>NO CUBRE</b> = activar contingencia aerea</span>
        </div>
      </div>
    </div>
    """
    components.html(html, height=580, scrolling=True)


def build_gantt(bundle: dict, industrial_week: str | None = None):
    gantt = pd.DataFrame(bundle.get("article_gantt", []))
    if gantt.empty:
        return None
    if industrial_week:
        gantt = gantt[gantt["industrial_week"] == industrial_week]
        if gantt.empty:
            return None
    gantt["start_date"] = pd.to_datetime(gantt["start_date"])
    gantt["end_date"] = pd.to_datetime(gantt["end_date"])
    fig = px.timeline(
        gantt,
        x_start="start_date",
        x_end="end_date",
        y="article_ref",
        color="transport_mode",
        text="task_name",
        hover_data=["industrial_week"],
        color_discrete_map={
            "sea": "#2563eb",
            "air": "#dc2626",
            "truck": "#0891b2",
            "warehouse": "#16a34a",
        },
    )
    fig.update_layout(
        height=460,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.6)",
        font_color="#10233f",
        margin=dict(l=10, r=10, t=20, b=20),
        xaxis_title="Calendario",
        yaxis_title="Referencia articulo",
    )
    fig.update_yaxes(autorange="reversed")
    return fig


st.set_page_config(page_title="Dashboard KDD Logistica", layout="wide")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### Control Tower")
    st.caption("Supervision KDD y operacion logistica")

    st.markdown("#### 1. Estado del stack")
    service_rows = get_service_status()
    for row in service_rows:
        icon = {"OK": "🟢", "NOK": "🔴", "BOOT": "🟠", "OFF": "⚫"}.get(row["badge"], "⚪")
        st.markdown(f"- {icon} **{row['service']}**: {row['badge']}")

    st.markdown("#### 2. Accesos rapidos")
    st.markdown(
        """
        - NiFi: `https://localhost:8443`
        - Airflow: `http://localhost:8085`
        - NameNode: `http://localhost:9870`
        - Spark UI: `http://localhost:4040`
        - Cassandra: `localhost:9042`
        """
    )

    st.markdown("#### 3. Acciones globales")
    if st.button("Refrescar dashboard"):
        reset_dashboard_cache()
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

    st.markdown("#### 4. Navegacion recomendada")
    page_options = [
        "1. Resumen Ejecutivo",
        "2. Control Tower Valladolid",
        "3. Arquitectura en vivo",
        "4. KDD Fase I - Ingesta",
        "5. KDD Fase II - Spark",
        "6. GraphFrames",
        "7. Persistencia",
        "8. Orquestacion",
        "9. Evidencias KDD",
    ]
    if "dashboard_page" not in st.session_state:
        st.session_state["dashboard_page"] = page_options[0]

    for page_name in page_options:
        if st.button(page_name, use_container_width=True, key=f"nav_{page_name}"):
            st.session_state["dashboard_page"] = page_name

    current_page = st.radio(
        "Seccion actual",
        page_options,
        index=page_options.index(st.session_state["dashboard_page"]),
        key="dashboard_page_radio",
    )
    st.session_state["dashboard_page"] = current_page

    st.markdown("#### 5. Parametros de demo")
    st.slider("Factor de demanda", 80, 140, 100)
    st.slider("Retraso adicional ETA (h)", 0, 120, 48)
    st.toggle("Activar contingencia aerea", value=True)
    selected_week = st.selectbox("Semana industrial", ["Todas", "IW14", "IW15", "IW16", "IW17"], index=0)
    selected_customer = st.selectbox("Cliente destino", ["Todos", "Douai", "Cleon"], index=0)

    st.markdown("#### 6. Ayuda rapida")
    st.markdown(
        """
        - Verde: cobertura suficiente
        - Naranja: cobertura tensionada
        - Rojo: riesgo de rotura
        - Primero revisa stock Valladolid y ETA
        - Luego consulta contingencia aerea
        """
    )

bundle = get_dashboard_bundle()
ok_count = sum(1 for row in service_rows if row["badge"] == "OK")
nok_count = sum(1 for row in service_rows if row["badge"] == "NOK")
off_count = sum(1 for row in service_rows if row["badge"] == "OFF")
critical_alerts, medium_alerts, avg_delay = summarize_risk(bundle)
kpis = build_control_tower_kpis(bundle)

hero_left, hero_right = st.columns([1.7, 0.9])
with hero_left:
    st.markdown("<div class='hero-eyebrow'>BIG DATA TRANSPORT MONITOR</div>", unsafe_allow_html=True)
    st.markdown("<div class='hero-title'>2026: Predictive<br/>Logistics</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='hero-subtitle'>Centro de control del pipeline KDD: posicion de flota, riesgo meteorologico, alertas operativas, contingencia aerea, estado de servicios y analitica de red.</div>",
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
          <span class='tag-chip'>Air Recovery</span>
          <span class='tag-chip'>Airflow</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
with hero_right:
    render_panel(
        "Estado del pipeline",
        "sincronizado",
        f"Snapshots servidos desde Spark/Hive, HDFS y Cassandra.<br/><br/><strong>Servicios OK:</strong> {ok_count}/{len(service_rows)}<br/><strong>Riesgo medio:</strong> {'MEDIUM' if medium_alerts else 'LOW'}<br/><strong>Storage:</strong> HDFS + Hive",
        height=250,
    )

metric_cols = st.columns(6)
with metric_cols[0]:
    render_card("Flota monitorizada", str(len(bundle.get("ships_latest", []))), "Ultimo estado por vehiculo")
with metric_cols[1]:
    render_card("DOH Valladolid", f"{kpis['doh']}", "Cobertura media de stock")
with metric_cols[2]:
    render_card("OTIF estimado", f"{kpis['otif']}%", "Pedidos Douai comprometidos")
with metric_cols[3]:
    render_card("ETA medio barcos", f"{kpis['eta_avg']}h", "Llegada estimada a España")
with metric_cols[4]:
    render_card("Stock seguridad", f"{kpis['security_level']}%", "Nivel medio de colchón")
with metric_cols[5]:
    render_card("Referencias críticas", str(kpis['critical_refs']), "Riesgo de rotura de stock")

current_page = st.session_state.get("dashboard_page", "1. Resumen Ejecutivo")

if current_page == "1. Resumen Ejecutivo":
    left, right = st.columns([1.1, 0.9])
    with left:
        st.subheader("Gestión por excepción")
        render_panel(
            "Valladolid bajo vigilancia",
            "control tower",
            f"<strong>DOH medio:</strong> {kpis['doh']} dias<br/><strong>ETA medio barcos:</strong> {kpis['eta_avg']} horas<br/><strong>Referencias críticas:</strong> {kpis['critical_refs']}<br/><strong>Modo mejor valorado:</strong> {kpis['best_mode']}",
            height=220,
        )
        st.subheader("Alertas operativas")
        st.dataframe(pd.DataFrame(bundle.get("fact_alerts", [])), use_container_width=True, hide_index=True)
    with right:
        st.subheader("Opciones de contingencia")
        st.dataframe(pd.DataFrame(bundle.get("fact_air_recovery_options", [])), use_container_width=True, hide_index=True)
        st.subheader("KPIs clave")
        summary_df = pd.DataFrame(
            [
                ["Coste logístico por contingencia", f"{kpis['avg_time_saved']}h ahorro medio"],
                ["Riesgo meteo alto/medio", str(medium_alerts)],
                ["Servicios NOK", str(nok_count)],
                ["Alertas críticas", str(critical_alerts)],
            ],
            columns=["Indicador", "Valor"],
        )
        summary_df["Valor"] = summary_df["Valor"].astype(str)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

elif current_page == "2. Control Tower Valladolid":
    st.subheader("Stock Valladolid")
    week_filter = None if selected_week == "Todas" else selected_week
    stock_df = build_stock_table(bundle, selected_customer, week_filter)
    if stock_df.empty:
        st.info("Todavia no hay datos de stock preparados para Valladolid.")
    else:
        st.dataframe(stock_df, use_container_width=True, hide_index=True)

    st.subheader("Stock por referencia")
    stock_fig = build_stock_bar_chart(bundle, selected_customer, week_filter)
    if stock_fig is None:
        st.info("Todavia no hay suficiente informacion para pintar el stock por referencia.")
    else:
        st.plotly_chart(stock_fig, use_container_width=True)

    stock_left, stock_right = st.columns([1.05, 0.95])
    with stock_left:
        st.subheader("Pedidos de cliente")
        orders_view = filter_orders(bundle, selected_customer, week_filter)
        if orders_view.empty:
            st.info("No hay pedidos para este cliente/semana en el bundle actual.")
        else:
            st.dataframe(orders_view, use_container_width=True, hide_index=True)
    with stock_right:
        st.subheader("Flota y ETA")
        st.dataframe(enrich_ship_eta_dates(pd.DataFrame(bundle.get("ships_latest", []))), use_container_width=True, hide_index=True)
        st.subheader("Gantt por semanas industriales")
        gantt_fig = build_gantt(bundle, week_filter)
        if gantt_fig is None:
            st.info("Todavia no hay tareas de planificacion para mostrar en el Gantt.")
        else:
            st.plotly_chart(gantt_fig, use_container_width=True)

    lower_left, lower_right = st.columns([1.15, 0.85])
    with lower_left:
        st.subheader("Gantt de cobertura de stock vs ETA maritima")
        stock_gantt = build_stock_rupture_gantt(bundle, selected_customer, week_filter)
        if stock_gantt is None:
            st.info("Todavia no hay suficiente informacion para calcular ruptura de stock por ETA maritima.")
        else:
            st.plotly_chart(stock_gantt, use_container_width=True)

    with lower_right:
        st.subheader("Propuesta de contingencia aerea")
        contingency_df = build_air_contingency_table(bundle, selected_customer, week_filter)
        if contingency_df.empty:
            st.info("No hay referencias en rotura de stock con propuesta aerea disponible en este momento.")
        else:
            st.dataframe(contingency_df, use_container_width=True, hide_index=True)

    st.subheader("Horizonte de 10 semanas industriales")
    render_stock_horizon_table(bundle, selected_customer)

elif current_page == "3. Arquitectura en vivo":
    st.subheader("Arquitectura en vivo")
    for row in service_rows:
        cols = st.columns([2, 3, 2, 1, 1])
        cols[0].write(f"**{row['service']}**")
        cols[1].write(row["status"])
        cols[2].markdown(f"<span class='status-pill {build_service_badge(row['badge'])}'>{row['badge']}</span>", unsafe_allow_html=True)
        service_name = row["service"] if row["service"] != "airflow" else "airflow-webserver"
        if cols[3].button("On", key=f"start_{row['service']}"):
            st.code(compose_service_action(service_name, "start"))
            st.rerun()
        if cols[4].button("Off", key=f"stop_{row['service']}"):
            st.code(compose_service_action(service_name, "stop"))
            st.rerun()

elif current_page == "4. KDD Fase I - Ingesta":
    st.subheader("NiFi + Kafka")
    ing_left, ing_right = st.columns([0.8, 1.2])
    with ing_left:
        render_panel(
            "Ingesta Robusta y Decisión",
            "fase I",
            "Monitorización de flota en tiempo real. La activación de incidentes manuales o alertas meteorológicas automáticas recalcula el ETA y el riesgo de stock.",
            height=200,
        )
        kafka_topics = pd.DataFrame(
            [
                ["datos_crudos", "raw"],
                ["datos_filtrados", "filtered"],
                ["alertas_globales", "alerts"],
            ],
            columns=["Topic", "Uso"],
        )
        st.dataframe(kafka_topics, use_container_width=True, hide_index=True)
    with ing_right:
        ports_df, routes_df, ships_df, alerts_df = build_map_data(bundle)
        
        # Capa de alertas por puerto
        layers = []
        if not ports_df.empty:
            # Unir puertos con alertas si existen
            ports_alerts = ports_df.copy()
            if not alerts_df.empty:
                ports_alerts = ports_alerts.merge(alerts_df[["via_port", "severity", "risk_level"]], left_on="port_name", right_on="via_port", how="left")
                ports_alerts["severity"] = ports_alerts["severity"].fillna(1)
            else:
                ports_alerts["severity"] = 1
            
            ports_alerts["fill_color"] = ports_alerts["severity"].apply(
                lambda sev: [220, 38, 38, 160] if sev >= 4 else [245, 158, 11, 160] if sev >= 3 else [34, 197, 94, 160]
            )
            layers.append(pdk.Layer("ScatterplotLayer", data=ports_alerts, get_position="[lon, lat]", get_radius=50000, get_fill_color="fill_color", pickable=True))

        # Capa de rutas
        if not routes_df.empty:
            layers.append(pdk.Layer("LineLayer", data=routes_df, get_source_position="[origin_lon, origin_lat]", get_target_position="[dest_lon, dest_lat]", get_color="color", get_width=3, pickable=True))

        # Capa de barcos (GPS)
        ship_options = ["Todos"]
        if not ships_df.empty and "ship_id" in ships_df.columns:
            ship_options.extend(sorted(ships_df["ship_id"].dropna().tolist()))
        selected_ship = st.selectbox("Filtrar por barco", ship_options, index=0, key="ingesta_selected_ship")
        
        # Filtro por destino
        dest_options = ["Todos", "Algeciras", "Valencia", "Barcelona"]
        selected_dest = st.selectbox("Filtrar por puerto destino", dest_options, index=0)

        if not ships_df.empty:
            ships_map_df = ships_df.copy()
            if selected_ship != "Todos":
                ships_map_df = ships_map_df[ships_map_df["ship_id"] == selected_ship]
            if selected_dest != "Todos":
                ships_map_df = ships_map_df[ships_map_df["dest_port"] == selected_dest]
            
            ships_map_df = enrich_ship_eta_dates(ships_map_df)
            # Corregido: Asignar color como una lista de listas para que coincida con la longitud del DataFrame
            ships_map_df["fill_color"] = [[37, 99, 235]] * len(ships_map_df)
            
            layers.append(pdk.Layer("ScatterplotLayer", data=ships_map_df, get_position="[lon, lat]", get_radius=35000, get_fill_color="[255, 255, 255]", get_line_color="[0, 0, 0]", line_width_min_pixels=2, pickable=True))
            layers.append(pdk.Layer("ScatterplotLayer", data=ships_map_df, get_position="[lon, lat]", get_radius=20000, get_fill_color="[37, 99, 235]", pickable=True))

        st.pydeck_chart(
            pdk.Deck(
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                initial_view_state=pdk.ViewState(latitude=20.0, longitude=60.0, zoom=1.2, pitch=0),
                layers=layers,
                tooltip={"text": "Entidad: {port_name}{ship_id}\nEstado: {risk_level}"},
            ),
            use_container_width=True,
        )

    st.subheader("Alertas y decisiones operativas")
    
    # Pre-calculo de simulacion e incidencias
    risk_cols = st.columns(6)
    active_constraints = []
    for idx, label in enumerate(SHIP_CONSTRAINTS.keys()):
        with risk_cols[idx]:
            if st.toggle(label, value=False, key=f"risk_toggle_{label}"):
                active_constraints.append(label)

    # Tabla de barcos y decisiones
    ships_table_df = ships_df.copy() if not ships_df.empty else pd.DataFrame()
    if not ships_table_df.empty:
        # Añadir coste maritimo ficticio/base
        ships_table_df["Coste Maritimo (EUR)"] = 15000.0
        
        # Columnas de ETA
        ships_table_df["ETA Original"] = ships_table_df["eta_hours_estimate"].apply(
            lambda h: (datetime.now() + timedelta(hours=float(h))).strftime("%Y-%m-%d %H:%M") if pd.notna(h) else "N/A"
        )
        
        # Recalcular ETA con incidencias
        extra_h = sum(SHIP_CONSTRAINTS[c] for c in active_constraints)
        # Tambien sumamos el retraso meteo del API si existe en fact_weather_operational
        weather_df = pd.DataFrame(bundle.get("fact_weather_operational", []))
        
        def get_final_eta(row):
            base_h = float(row["eta_hours_estimate"])
            # Buscar retraso meteo para el puerto destino
            w_delay = 0
            if not weather_df.empty:
                match = weather_df[weather_df["via_port"] == row["dest_port"]]
                if not match.empty:
                    w_delay = float(match.iloc[0].get("weather_delay_hours_estimate", 0))
            
            total_h = base_h + extra_h + w_delay
            return (datetime.now() + timedelta(hours=total_h)).strftime("%Y-%m-%d %H:%M"), total_h

        ships_table_df[["ETA Recalculada", "total_hours"]] = ships_table_df.apply(
            lambda r: pd.Series(get_final_eta(r)), axis=1
        )
        
        # Lógica de riesgo de ruptura e icono de avión
        stock_context = build_stock_context(bundle)
        def get_contingency(row):
            # Si el barco va a Valladolid (vía Santander/Bilbao/etc)
            # Simplificamos: si total_hours > stock_out_hours -> Avion
            # Buscamos la referencia que trae este barco (asumimos 1 ref por barco para la demo o la media)
            if stock_context.empty: return "🚢"
            
            # Simulamos chequeo contra horas hasta stockout (usamos 120h como umbral de seguridad si no hay datos)
            limit_h = 120.0
            if row["total_hours"] > limit_h:
                return "✈️ ACTIVAR AEREO"
            return "🚢 OK"

        ships_table_df["Contingencia"] = ships_table_df.apply(get_contingency, axis=1)
        
        st.dataframe(
            ships_table_df[["ship_id", "origin_port", "dest_port", "ETA Original", "ETA Recalculada", "Coste Maritimo (EUR)", "Contingencia"]].rename(
                columns={"ship_id": "Barco", "origin_port": "Origen", "dest_port": "Destino"}
            ),
            use_container_width=True,
            hide_index=True
        )

    # Eliminar bloques antiguos de la sección Ingesta que ya no encajan
    # (El código original continuaba con el mapa antiguo y simulación individual)

elif current_page == "5. KDD Fase II - Spark":
    st.subheader("Spark SQL + Streaming")
    sp_left, sp_right = st.columns([1, 1])
    with sp_left:
        st.dataframe(pd.DataFrame(bundle.get("fact_weather_operational", [])), use_container_width=True, hide_index=True)
    with sp_right:
        st.dataframe(pd.DataFrame(bundle.get("fact_air_recovery_options", [])), use_container_width=True, hide_index=True)

elif current_page == "6. GraphFrames":
    st.subheader("GraphFrames y criticidad")
    st.dataframe(pd.DataFrame(bundle.get("graph_centrality", [])), use_container_width=True, hide_index=True)

elif current_page == "7. Persistencia":
    st.subheader("Persistencia Hive vs Cassandra")
    p_left, p_right = st.columns(2)
    with p_left:
        hive_tables = pd.DataFrame(
            [
                ["fact_weather_operational", "Analítica operativa"],
                ["fact_air_recovery_options", "Contingencia"],
                ["fact_alerts", "Alertas"],
                ["fact_graph_centrality", "GraphFrames"],
                ["fact_article_gantt", "Planificación"],
            ],
            columns=["Tabla Hive", "Uso"],
        )
        st.dataframe(hive_tables, use_container_width=True, hide_index=True)
    with p_right:
        st.subheader("Cassandra")
        cass_df = enrich_ship_eta_dates(pd.DataFrame(bundle.get("ships_latest", [])))
        st.dataframe(cass_df[[c for c in cass_df.columns if c in ["ship_id", "route_id", "dest_port", "stock_on_hand", "eta_fecha"]]], use_container_width=True, hide_index=True)

elif current_page == "8. Orquestacion":
    st.subheader("Airflow y orquestación")
    airflow_df = pd.DataFrame(
        [
            ["logistica_kdd_microbatch", "15 min", "staging + facts + Cassandra"],
            ["logistica_kdd_monthly_retrain", "mensual", "grafo + limpieza HDFS"],
        ],
        columns=["DAG", "Frecuencia", "Función"],
    )
    st.dataframe(airflow_df, use_container_width=True, hide_index=True)

elif current_page == "9. Evidencias KDD":
    st.subheader("Evidencias KDD")
    render_diagram("Flujo KDD", "pipeline general", flow_diagram_svg())
    render_diagram("Diagrama de Secuencia", "interaccion entre componentes", sequence_diagram_svg())
    render_diagram("Diagrama de Clases", "modelo conceptual", class_diagram_svg())
    render_diagram("Casos de Uso", "escenarios de defensa", use_case_diagram_svg())
    stock_df = build_stock_table(bundle)
    if not stock_df.empty:
        st.dataframe(stock_df, use_container_width=True, hide_index=True)
