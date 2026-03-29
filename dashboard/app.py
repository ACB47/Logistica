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

CUSTOM_CSS = """
<style>
  .stApp {
    background: linear-gradient(180deg, #eef4fb 0%, #dfeaf7 45%, #d5e4f5 100%);
    color: #10233f;
  }
  .block-container {
    padding-top: 1rem;
    padding-bottom: 2rem;
    max-width: 1500px;
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
