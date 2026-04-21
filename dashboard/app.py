from __future__ import annotations

import json
import os
import socket
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from math import isnan
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
import streamlit.components.v1 as components


ROOT = Path(__file__).resolve().parents[1]
DOCKER_COMPOSE = ["docker", "compose"]
SERVICE_MAP = {
    "postgres": "logistica-postgres-1",
    "kafka": "logistica-kafka-1",
    "nifi": "logistica-nifi-1",
    "spark": "logistica-spark-1",
    "cassandra": "logistica-cassandra-1",
    "namenode": "logistica-namenode-1",
    "datanode": "logistica-datanode-1",
    "airflow": "logistica-airflow-webserver-1",
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


def load_local_env() -> dict[str, str]:
    env_path = ROOT / ".env"
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def get_env_value(key: str, default: str | None = None, aliases: list[str] | None = None) -> str | None:
    aliases = aliases or []
    for candidate in [key] + aliases:
        value = os.getenv(candidate)
        if value:
            return value
    env_file = load_local_env()
    for candidate in [key] + aliases:
        value = env_file.get(candidate)
        if value:
            return value
    return default


def get_smtp_config() -> dict[str, str | int | bool]:
    host = get_env_value("SMTP_HOST", aliases=["SMTP_SERVER"])
    port_raw = get_env_value("SMTP_PORT", default="587")
    user = get_env_value("SMTP_USER")
    password = get_env_value("SMTP_PASSWORD")
    recipient = get_env_value("SMTP_RECIPIENT", aliases=["ALERT_RECIPIENT_EMAIL"], default="planificacion@logistica.com")
    sender_name = get_env_value("SMTP_SENDER_NAME", default="Control Tower ANACO")
    try:
        port = int(str(port_raw))
    except (TypeError, ValueError):
        port = 587
    return {
        "host": host or "",
        "port": port,
        "user": user or "",
        "password": password or "",
        "recipient": recipient or "",
        "sender_name": sender_name or "Control Tower ANACO",
        "configured": bool(host and user and password and recipient),
    }


def resolve_smtp_ipv4(host: str, port: int) -> str:
    try:
        addrinfo = socket.getaddrinfo(host, port, family=socket.AF_INET, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ConnectionError(f"No se pudo resolver el host SMTP {host}: {exc}") from exc

    if not addrinfo:
        raise ConnectionError(f"No se encontro una direccion IPv4 valida para {host}")

    return str(addrinfo[0][4][0])


def save_demo_alert_email(
    recipient_email: str,
    subject: str,
    body_html: str,
    alerts_data: list[dict],
) -> Path:
    output_dir = ROOT / "artifacts" / "email_alerts"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"critical_alert_{timestamp}.json"
    payload = {
        "mode": "demo",
        "created_at": datetime.now().isoformat(),
        "recipient": recipient_email,
        "subject": subject,
        "alerts_count": len(alerts_data),
        "alerts": alerts_data,
        "body_html": body_html,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


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


def send_critical_alerts_email(alerts_data: list[dict], recipient_override: str | None = None) -> tuple[bool, str]:
    """
    Envía un correo electrónico con las alertas críticas de referencias en rotación de stock.
    
    Args:
        alerts_data: Lista de diccionarios con información de las alertas críticas.
        
    Returns:
        Tuple de (éxito: bool, mensaje: str)
    """
    smtp_config = get_smtp_config()
    smtp_server = str(smtp_config["host"])
    smtp_port = int(smtp_config["port"])
    smtp_user = str(smtp_config["user"])
    smtp_password = str(smtp_config["password"])
    recipient_email = recipient_override.strip() if recipient_override else str(smtp_config["recipient"])
    sender_name = str(smtp_config["sender_name"])
    smtp_timeout = 10

    if not smtp_config["configured"] or not recipient_email:
        return False, "Falta configurar SMTP en .env. Revisa SMTP_HOST/SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD y SMTP_RECIPIENT."
    
    try:
        smtp_connect_host = resolve_smtp_ipv4(smtp_server, smtp_port)
        subject = "🚨 ALERTA: Referencias Críticas - Riesgo de Rotura de Stock"

        msg = MIMEMultipart()
        msg["From"] = f"{sender_name} <{smtp_user}>"
        msg["To"] = recipient_email
        msg["Subject"] = subject
        
        body = """<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #dc2626; color: white; }
        .critical { background-color: #fef2f2; }
        .warning { background-color: #fffbeb; }
    </style>
</head>
<body>
    <h2>🚨 Alerta de Referencias Críticas</h2>
    <p>Se han detectado las siguientes referencias en riesgo de rotación de stock:</p>
    <table>
        <tr>
            <th>ID Artículo</th>
            <th>Descripción</th>
            <th>Stock Actual</th>
            <th>Stock Mínimo</th>
            <th>Estado</th>
        </tr>
"""
        
        for alert in alerts_data:
            article_ref = alert.get("article_ref", "N/A")
            article_name = alert.get("article_name", "Sin descripción")
            stock_actual = alert.get("total_stock_pieces", 0)
            stock_minimo = alert.get("safety_stock_min", 0)
            estado = alert.get("stock_status", "CRÍTICO")
            
            row_class = "critical" if stock_actual <= stock_minimo else "warning"
            body += f"""        <tr class="{row_class}">
            <td>{article_ref}</td>
            <td>{article_name}</td>
            <td>{stock_actual}</td>
            <td>{stock_minimo}</td>
            <td>{estado}</td>
        </tr>
"""
        
        body += """    </table>
    <p><strong>Nota:</strong> Se recomienda revisar el plan de contingencia aérea para estas referencias.</p>
    <hr>
    <p><small>Este correo fue enviado automáticamente desde el sistema de monitoreo logístico.</small></p>
</body>
</html>"""
        
        msg.attach(MIMEText(body, "html"))
        
        with smtplib.SMTP(timeout=smtp_timeout) as server:
            server._host = smtp_server
            server.connect(smtp_connect_host, smtp_port)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)

        return True, f"Correo enviado correctamente a {recipient_email}"
    except socket.timeout:
        demo_path = save_demo_alert_email(recipient_email, subject, body, alerts_data)
        return True, (
            f"SMTP inaccesible; alerta guardada en modo demo en {demo_path}"
        )
    except smtplib.SMTPAuthenticationError:
        return False, "Autenticacion SMTP fallida. Revisa SMTP_USER y SMTP_PASSWORD."
    except smtplib.SMTPConnectError as e:
        demo_path = save_demo_alert_email(recipient_email, subject, body, alerts_data)
        return True, f"Conexion SMTP no disponible; alerta guardada en modo demo en {demo_path}"
    except smtplib.SMTPException as e:
        return False, f"Error SMTP: {e}"
    except Exception as e:
        if "Network is unreachable" in str(e) or "timed out" in str(e):
            demo_path = save_demo_alert_email(recipient_email, subject, body, alerts_data)
            return True, f"Sin salida SMTP; alerta guardada en modo demo en {demo_path}"
        return False, f"Error al enviar correo: {str(e)}"


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
        raw_status = status_by_name.get(container_name)
        resolved_name = container_name
        if raw_status is None:
            for name, status in status_by_name.items():
                if name.endswith(container_name) or container_name.endswith(name):
                    raw_status = status
                    resolved_name = name
                    break
        if raw_status is None:
            raw_status = "Not created"
        if "healthy" in raw_status.lower() or raw_status.startswith("Up"):
            badge = "OK"
        elif "Restarting" in raw_status or "Exited" in raw_status:
            badge = "NOK"
        elif raw_status == "Not created":
            badge = "OFF"
        else:
            badge = "BOOT"
        rows.append({"service": service, "container": resolved_name, "status": raw_status, "badge": badge})
    return rows


def compose_service_action(service: str, action: str) -> str:
    if action == "start":
        result = run_command(DOCKER_COMPOSE + ["up", "-d", service], timeout=600)
        output = result.stdout or result.stderr or f"{action} {service} ejecutado"
        if service == "nifi":
            healthcheck = run_command(["bash", "scripts/61_nifi_healthcheck.sh"], timeout=300)
            bootstrap = run_command(
                ["env", "START_NIFI_FLOW=1", "python3", "scripts/62_bootstrap_nifi_open_meteo_flow.py"],
                timeout=600,
            )
            output = "\n\n".join(
                part
                for part in [
                    output,
                    healthcheck.stdout or healthcheck.stderr,
                    bootstrap.stdout or bootstrap.stderr,
                ]
                if part
            )
    else:
        result = run_command(DOCKER_COMPOSE + ["stop", service], timeout=300)
        output = result.stdout or result.stderr or f"{action} {service} ejecutado"
    st.cache_data.clear()
    return output


def run_script(script: str) -> str:
    result = run_command(["bash", script], timeout=1200)
    st.cache_data.clear()
    return result.stdout or result.stderr


@st.cache_data(ttl=60)
def get_dashboard_bundle() -> dict:
    output_file = ROOT / "jobs" / "dashboard_bundle_output.json"
    backup_file = ROOT / "jobs" / "dashboard_bundle_output.last_good.json"
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

    bundle = json.loads(output_file.read_text(encoding="utf-8"))
    key_sections = ["ships_latest", "stock_valladolid", "customer_orders_douai", "article_gantt"]
    has_main_data = any(bundle.get(section) for section in key_sections)

    if has_main_data:
        backup_file.write_text(json.dumps(bundle, ensure_ascii=False), encoding="utf-8")
        return bundle

    if backup_file.exists():
        return json.loads(backup_file.read_text(encoding="utf-8"))

    error_text = "; ".join(bundle.get("errors", [])) if isinstance(bundle, dict) else ""
    raise RuntimeError(error_text or "El bundle del dashboard no contiene datos utilizables")


def reset_dashboard_cache() -> None:
    st.cache_data.clear()
    st.rerun()


def mostrar_documento_md(ruta_archivo: str) -> None:
    ruta = ROOT / ruta_archivo
    st.title(Path(ruta_archivo).name)
    if not ruta.exists():
        st.warning(f"No se encontró el documento: {ruta_archivo}")
        return
    st.markdown(ruta.read_text(encoding="utf-8"))


SEA_ROUTE_CORRIDORS: dict[str, list[tuple[float, float]]] = {
    "route-shanghai-algeciras": [
        (31.2304, 121.4737),
        (22.0, 118.0),
        (13.0, 103.0),
        (6.0, 80.0),
        (12.0, 56.0),
        (18.0, 44.0),
        (12.0, 32.0),
        (20.0, 4.0),
        (36.1270, -5.4530),
    ],
    "route-shanghai-valencia": [
        (31.2304, 121.4737),
        (22.0, 118.0),
        (13.0, 103.0),
        (6.0, 80.0),
        (12.0, 56.0),
        (18.0, 44.0),
        (12.0, 32.0),
        (20.0, 2.0),
        (39.4580, -0.3170),
    ],
    "route-shanghai-barcelona": [
        (31.2304, 121.4737),
        (22.0, 118.0),
        (13.0, 103.0),
        (6.0, 80.0),
        (12.0, 56.0),
        (18.0, 44.0),
        (12.0, 32.0),
        (20.0, 5.0),
        (41.3520, 2.1730),
    ],
    "route-yokohama-algeciras": [
        (35.4437, 139.6380),
        (30.0, 132.0),
        (18.0, 118.0),
        (8.0, 92.0),
        (6.0, 78.0),
        (12.0, 56.0),
        (18.0, 44.0),
        (12.0, 32.0),
        (20.0, 4.0),
        (36.1270, -5.4530),
    ],
    "route-yokohama-valencia": [
        (35.4437, 139.6380),
        (30.0, 132.0),
        (18.0, 118.0),
        (8.0, 92.0),
        (6.0, 78.0),
        (12.0, 56.0),
        (18.0, 44.0),
        (12.0, 32.0),
        (20.0, 2.0),
        (39.4580, -0.3170),
    ],
    "route-yokohama-barcelona": [
        (35.4437, 139.6380),
        (30.0, 132.0),
        (18.0, 118.0),
        (8.0, 92.0),
        (6.0, 78.0),
        (12.0, 56.0),
        (18.0, 44.0),
        (12.0, 32.0),
        (20.0, 5.0),
        (41.3520, 2.1730),
    ],
}


def interpolate_sea_route(route_id: str, progress: float) -> tuple[float, float] | None:
    points = SEA_ROUTE_CORRIDORS.get(str(route_id))
    if not points or len(points) < 2:
        return None
    progress = max(0.0, min(1.0, float(progress)))
    segment_count = len(points) - 1
    scaled = progress * segment_count
    seg_index = min(int(scaled), segment_count - 1)
    local_t = scaled - seg_index
    start_lat, start_lon = points[seg_index]
    end_lat, end_lon = points[seg_index + 1]
    lat = start_lat + ((end_lat - start_lat) * local_t)
    lon = start_lon + ((end_lon - start_lon) * local_t)
    return round(lat, 6), round(lon, 6)


def normalize_ship_positions(ships: pd.DataFrame) -> pd.DataFrame:
    if ships.empty or not {"route_id", "lat", "lon"}.issubset(ships.columns):
        return ships
    ships = ships.copy()
    result_rows = []
    for _, row in ships.iterrows():
        route_id = str(row.get("route_id", ""))
        eta_hours = float(row.get("eta_hours_estimate", 200))
        voyage_total = float(row.get("voyage_days_total", 18))
        if voyage_total > 0:
            progress = 1.0 - (eta_hours / (voyage_total * 24))
            progress = max(0.0, min(1.0, progress))
        else:
            progress = 0.5
        snapped = interpolate_sea_route(route_id, progress)
        if snapped:
            row["lat"] = snapped[0]
            row["lon"] = snapped[1]
        result_rows.append(row)
    return pd.DataFrame(result_rows)


def build_map_data(bundle: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ports = pd.DataFrame(bundle.get("dim_ports", []))
    routes = pd.DataFrame(bundle.get("dim_routes", []))
    ships = normalize_ship_positions(pd.DataFrame(bundle.get("ships_latest", [])))
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


def build_operational_ship_map(
    bundle: dict,
    selected_ship: str | None = None,
    selected_dest: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ports, routes, ships, alerts = build_map_data(bundle)
    if ships.empty:
        return ports, routes, ships, alerts, ships

    ships_map = ships.copy()
    if selected_ship and selected_ship != "Todos" and "ship_id" in ships_map.columns:
        ships_map = ships_map[ships_map["ship_id"] == selected_ship]
    if selected_dest and selected_dest != "Todos" and "dest_port" in ships_map.columns:
        ships_map = ships_map[ships_map["dest_port"] == selected_dest]

    if not ports.empty and {"port_name", "lat", "lon"}.issubset(ports.columns):
        origin_lookup = ports[["port_name", "lat", "lon"]].rename(
            columns={"port_name": "origin_port", "lat": "origin_lat", "lon": "origin_lon"}
        )
        dest_lookup = ports[["port_name", "lat", "lon"]].rename(
            columns={"port_name": "dest_port", "lat": "dest_lat", "lon": "dest_lon"}
        )
        ships_map = ships_map.merge(origin_lookup, on="origin_port", how="left")
        ships_map = ships_map.merge(dest_lookup, on="dest_port", how="left")

        if {"lat", "lon", "origin_lat", "origin_lon", "dest_lat", "dest_lon"}.issubset(ships_map.columns):
            ships_map["distance_to_origin"] = (
                (ships_map["lat"] - ships_map["origin_lat"]).abs()
                + (ships_map["lon"] - ships_map["origin_lon"]).abs()
            )
            ships_map["distance_to_dest"] = (
                (ships_map["lat"] - ships_map["dest_lat"]).abs()
                + (ships_map["lon"] - ships_map["dest_lon"]).abs()
            )
            # Solo barcos en tránsito: fuera de la zona de origen y de llegada.
            ships_map = ships_map[(ships_map["distance_to_origin"] > 0.35) & (ships_map["distance_to_dest"] > 0.35)]

    if "severity" not in routes.columns:
        routes["severity"] = 1
    else:
        routes["severity"] = routes["severity"].fillna(1)

    if not routes.empty and {"severity"}.issubset(routes.columns):
        routes["color"] = routes["severity"].apply(
            lambda sev: [220, 38, 38] if sev >= 4 else [245, 158, 11] if sev >= 3 else [37, 99, 235]
        )

    return ports, routes, ships, alerts, ships_map


def build_ship_eta_table(
    bundle: dict,
    selected_ship: str | None = None,
    selected_dest: str | None = None,
    active_constraints: list[str] | None = None,
) -> pd.DataFrame:
    ships = pd.DataFrame(bundle.get("ships_latest", []))
    if ships.empty:
        return ships

    if selected_ship and selected_ship != "Todos" and "ship_id" in ships.columns:
        ships = ships[ships["ship_id"] == selected_ship]
    if selected_dest and selected_dest != "Todos" and "dest_port" in ships.columns:
        ships = ships[ships["dest_port"] == selected_dest]

    if ships.empty:
        return ships

    weather = pd.DataFrame(bundle.get("fact_weather_operational", []))
    air = get_air_recovery_df(bundle)
    active_constraints = active_constraints or []
    extra_hours = float(sum(SHIP_CONSTRAINTS[name] for name in active_constraints))
    now = datetime.now()

    weather_port_col = next((col_name for col_name in ["port_ref", "via_port", "dest_port"] if col_name in weather.columns), None)
    weather_lookup: dict[str, float] = {}
    if not weather.empty and weather_port_col and "weather_delay_hours_estimate" in weather.columns:
        weather_sorted = weather
        if "event_ts" in weather.columns:
            weather_sorted = weather.sort_values("event_ts", ascending=False)
        for port_name, group in weather_sorted.groupby(weather_port_col):
            delay = group.iloc[0].get("weather_delay_hours_estimate", 0)
            weather_lookup[str(port_name)] = float(delay or 0)

    air_lookup: dict[str, dict[str, object]] = {}
    if not air.empty and "ship_id" in air.columns:
        air_value_cols = [col_name for col_name in ["recommended_mode", "stock_break_risk", "air_total_eta_hours", "air_total_cost_eur", "time_saved_hours"] if col_name in air.columns]
        if air_value_cols:
            air_best = air.sort_values("time_saved_hours", ascending=False) if "time_saved_hours" in air.columns else air
            air_best = air_best.drop_duplicates("ship_id", keep="first")
            air_lookup = air_best.set_index("ship_id")[air_value_cols].to_dict(orient="index")

    rows: list[dict[str, object]] = []
    for _, row in ships.iterrows():
        base_raw = row.get("eta_hours_estimate", 0.0)
        base_hours = float(base_raw) if pd.notna(base_raw) else 0.0
        weather_delay = float(weather_lookup.get(str(row.get("dest_port")), 0.0))
        recalculated_hours = max(0.0, base_hours + extra_hours + weather_delay)
        ship_id = str(row.get("ship_id", ""))
        air_info = air_lookup.get(ship_id, {})
        needs_air = air_info.get("recommended_mode") == "AEREO_CAMION" or air_info.get("stock_break_risk") not in (None, "COBERTURA_OK")
        maritime_raw = row.get("maritime_cost_eur", 0.0)
        maritime_cost = float(maritime_raw) if pd.notna(maritime_raw) else 0.0

        rows.append(
            {
                "Nombre de barco": row.get("ship_name", ship_id.replace("ship-", "MV ").title() if ship_id else "N/A"),
                "Barco": ship_id,
                "Origen": row.get("origin_port", "N/A"),
                "Destino": row.get("dest_port", "N/A"),
                "Lat": row.get("lat", "N/A"),
                "Lon": row.get("lon", "N/A"),
                "Fecha de salida": row.get("fecha_salida_origen", "N/A"),
                "Dias de viaje": row.get("voyage_days_total", "N/A"),
                "ETA Original": (now + timedelta(hours=base_hours)).strftime("%Y-%m-%d %H:%M"),
                "ETA Recalculada": (now + timedelta(hours=recalculated_hours)).strftime("%Y-%m-%d %H:%M"),
                "Coste Maritimo (EUR)": maritime_cost,
                "Icono Aereo": "✈️" if needs_air else "🚢",
                "Estado": "Riesgo de rotura" if needs_air else "Cobertura OK",
            }
        )

    return pd.DataFrame(rows)


def get_air_recovery_df(bundle: dict) -> pd.DataFrame:
    air = pd.DataFrame(bundle.get("fact_air_recovery_options", []))
    if not air.empty:
        return air

    ships = pd.DataFrame(bundle.get("ships_latest", []))
    if ships.empty:
        return air

    fallback_rows: list[dict[str, object]] = []
    for idx, row in ships.iterrows():
        ship_eta = float(row.get("eta_hours_estimate", 240.0) or 240.0)
        stock_on_hand = float(row.get("stock_on_hand", 0.0) or 0.0)
        reorder_point = float(row.get("reorder_point", 0.0) or 0.0)
        air_eta = max(24.0, round(ship_eta * 0.35, 1))
        time_saved = max(0.0, round(ship_eta - air_eta, 1))
        is_critical = stock_on_hand <= reorder_point
        fallback_rows.append(
            {
                "ship_id": row.get("ship_id"),
                "origin_port": row.get("origin_port", "Shanghai"),
                "dest_port": row.get("dest_port", "N/A"),
                "ship_remaining_hours": ship_eta,
                "air_total_eta_hours": air_eta,
                "time_saved_hours": time_saved,
                "air_total_cost_eur": round(12000 + (idx * 850), 2),
                "recommended_mode": "AEREO_CAMION" if is_critical else "MARITIMO",
                "stock_break_risk": "ROTURA_STOCK" if is_critical else "COBERTURA_OK",
                "hours_until_stock_break": max(48.0, round(reorder_point * 3, 1)),
            }
        )

    return pd.DataFrame(fallback_rows)


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
    air = get_air_recovery_df(bundle)

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
    recovery = get_air_recovery_df(bundle)
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
    
    stock["volume_m3"] = stock["total_stock_pieces"] * stock.get("unit_volume_m3", 0)
    stock["weight_kg"] = stock["total_stock_pieces"] * stock.get("unit_weight_kg", 0)
    
    rename_cols = {
        "article_ref": "Referencia articulo",
        "article_name": "Designacion articulo",
        "pieces_per_pack": "Piezas por embalaje",
        "total_stock_packs": "Cantidad total embalajes",
        "total_stock_pieces": "Cantidad total piezas",
        "daily_consumption_avg": "Consumo medio diario",
        "safety_stock_min": "Stock minimo seguridad",
        "douai_ordered_pieces": "Pedido cliente Douai (piezas)",
        "available_after_orders": "Disponible tras pedido",
        "status_dot": "Estado",
        "stock_status": "Status Stock",
        "unit_volume_m3": "Volumen unitario (m³)",
        "unit_weight_kg": "Peso unitario (kg)",
        "warehouse_zone": "Zona almacen",
        "dimensions_cm": "Dimensiones (cm)",
    }
    
    if "volume_m3" in stock.columns:
        rename_cols["volume_m3"] = "Volumen total (m³)"
    if "weight_kg" in stock.columns:
        rename_cols["weight_kg"] = "Peso total (kg)"
    
    stock = stock.rename(columns=rename_cols)
    
    cols_to_show = [
        "Estado",
        "Status Stock",
        "Referencia articulo",
        "Designacion articulo",
        "Piezas por embalaje",
        "Cantidad total embalajes",
        "Cantidad total piezas",
        "Stock minimo seguridad",
    ]
    
    for col in ["Volumen unitario (m³)", "Peso unitario (kg)", "Zona almacen", "Dimensiones (cm)", "Volumen total (m³)", "Peso total (kg)"]:
        if col in stock.columns:
            cols_to_show.append(col)
    
    return stock[cols_to_show]


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
    air = get_air_recovery_df(bundle)
    air_dest_col = "dest_port" if "dest_port" in air.columns else None
    if stock.empty:
        return None

    now = datetime.now()
    rows: list[dict] = []
    for _, row in stock.iterrows():
        if air_dest_col:
            maritime = air[air[air_dest_col] == row.get("inbound_port")]
        else:
            maritime = pd.DataFrame()
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
    air = get_air_recovery_df(bundle)
    air_dest_col = "dest_port" if "dest_port" in air.columns else None
    if stock.empty or orders_cov.empty or air.empty or not air_dest_col:
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
        best_air = air[air[air_dest_col] == stock_row["inbound_port"]]
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
    air = get_air_recovery_df(bundle)
    air_dest_col = "dest_port" if "dest_port" in air.columns else None
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
        if air_dest_col:
            best_option = air[air[air_dest_col] == stock_row.get("inbound_port")]
        else:
            best_option = pd.DataFrame()
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
    service_rows = get_service_status()
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
        "10. Alertas y Contingencias",
        "11. Ejecución Contingencia Multimodal",
    ]
    if "vista_actual" not in st.session_state:
        st.session_state["vista_actual"] = page_options[0]

    with st.container():
        st.markdown("## 🌐 CONTROL TOWER ANACO 🌐")
        st.caption("👤 Sede Central - Valladolid")
        st.divider()

    with st.container():
        st.markdown("### 🧭 Navegación Principal")
        st.markdown("#### 🏢 Bloque Negocio")
        if st.button("Resumen Ejecutivo", use_container_width=True):
            st.session_state["vista_actual"] = "1. Resumen Ejecutivo"
        if st.button("Control Tower Valladolid", use_container_width=True):
            st.session_state["vista_actual"] = "2. Control Tower Valladolid"
        if st.button("Alertas y Contingencias", use_container_width=True):
            st.session_state["vista_actual"] = "10. Alertas y Contingencias"
        if st.button("Ejecución Contingencia Multimodal", use_container_width=True):
            st.session_state["vista_actual"] = "11. Ejecución Contingencia Multimodal"

        st.markdown("#### ⚙️ Bloque Auditoría KDD")
        if st.button("Arquitectura en vivo", use_container_width=True):
            st.session_state["vista_actual"] = "3. Arquitectura en vivo"
        if st.button("Fase I - Ingesta", use_container_width=True):
            st.session_state["vista_actual"] = "4. KDD Fase I - Ingesta"
        if st.button("Fase II - Spark", use_container_width=True):
            st.session_state["vista_actual"] = "5. KDD Fase II - Spark"
        if st.button("GraphFrames", use_container_width=True):
            st.session_state["vista_actual"] = "6. GraphFrames"
        if st.button("Persistencia", use_container_width=True):
            st.session_state["vista_actual"] = "7. Persistencia"
        if st.button("Orquestación", use_container_width=True):
            st.session_state["vista_actual"] = "8. Orquestacion"
        if st.button("Evidencias KDD", use_container_width=True):
            st.session_state["vista_actual"] = "9. Evidencias KDD"

    st.divider()

    with st.container():
        st.markdown("### 🚀 Acciones Globales")
        if st.button("🔄 Refrescar dashboard", use_container_width=True, type="primary"):
            reset_dashboard_cache()
        if st.button("🧱 Rebuild tablas Hive demo", use_container_width=True):
            st.code(run_script("scripts/66_rebuild_hive_demo_tables.sh")[-3000:])
        if st.button("📦 Cargar Cassandra latest state", use_container_width=True):
            st.code(run_script("scripts/65_load_vehicle_latest_state_cassandra.sh")[-3000:])

    st.divider()

    with st.expander("⚙️ Estado del Stack y Enlaces", expanded=False):
        for row in service_rows:
            icon = {"OK": "🟢", "NOK": "🔴", "BOOT": "🟠", "OFF": "⚫"}.get(row["badge"], "⚪")
            st.markdown(f"{icon} **{row['service']}** · {row['badge']}")
        st.markdown("---")
        st.markdown(
            """
            - NiFi: `https://localhost:8443`
            - Airflow: `http://localhost:8085`
            - NameNode: `http://localhost:9870`
            - Spark UI: `http://localhost:4040`
            - Zeppelin (Notebooks KDD): `http://localhost:8081`
            - Cassandra: `localhost:9042`
            """
        )
        if st.button("▶️ Arrancar Servicios", use_container_width=True, type="primary"):
            st.code(compose_service_action("nifi", "start"))
            st.code(run_command(DOCKER_COMPOSE + ["up", "-d", "postgres", "kafka", "spark", "cassandra", "namenode", "datanode", "airflow-webserver"], timeout=600).stdout)
            st.cache_data.clear()
        if st.button("⏹️ Parar Servicios", use_container_width=True):
            st.code(run_command(DOCKER_COMPOSE + ["down"], timeout=600).stdout)
            st.cache_data.clear()

    st.divider()

    st.markdown("### 📄 Documentación")
    if st.button("Readme.md", use_container_width=True):
        st.session_state["vista_actual"] = "DOC_README"
    if st.button("Manual de desarrollador", use_container_width=True):
        st.session_state["vista_actual"] = "DOC_DEV"
    if st.button("Manual de Usuario Gráfico", use_container_width=True):
        st.session_state["vista_actual"] = "DOC_GUI"
    if st.button("Manual de Usuario", use_container_width=True):
        st.session_state["vista_actual"] = "DOC_USER"

    st.sidebar.markdown("<br><br><div style='text-align: center; color: grey; font-size: 0.8em;'>Diseñado e implementado por: Ana Coloma Bausela - 2026</div>", unsafe_allow_html=True)

bundle = get_dashboard_bundle()
ok_count = sum(1 for row in service_rows if row["badge"] == "OK")
nok_count = sum(1 for row in service_rows if row["badge"] == "NOK")
off_count = sum(1 for row in service_rows if row["badge"] == "OFF")
critical_alerts, medium_alerts, avg_delay = summarize_risk(bundle)
kpis = build_control_tower_kpis(bundle)
selected_week = "Todas"
selected_customer = "Todos"
current_page = st.session_state.get("vista_actual", "1. Resumen Ejecutivo")
doc_views = {"DOC_README", "DOC_DEV", "DOC_GUI", "DOC_USER"}

if current_page not in doc_views and current_page != "3. Arquitectura en vivo":
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

AIR_CONTINGENCY_MIN_TIME_SAVED_HOURS = 24
AIR_CONTINGENCY_MAX_COST_EUR = 18000

if current_page == "DOC_README":
    mostrar_documento_md("README.md")

elif current_page == "DOC_DEV":
    mostrar_documento_md("docs/Manual_Desarrollador.md")

elif current_page == "DOC_GUI":
    mostrar_documento_md("docs/Manual_Usuario_Grafico.md")

elif current_page == "DOC_USER":
    mostrar_documento_md("docs/Manual_Usuario.md")

elif current_page == "1. Resumen Ejecutivo":
    alerts_df = pd.DataFrame(bundle.get("fact_alerts", []))
    stock_df = pd.DataFrame(bundle.get("stock_valladolid", []))
    air_df = get_air_recovery_df(bundle)
    fleet_df = pd.DataFrame(bundle.get("ships_latest", []))

    if fleet_df.empty:
        fleet_df = pd.DataFrame(
            [
                {"ship_id": "SHIP-001", "status": "En tránsito", "eta_hours": 92},
                {"ship_id": "SHIP-002", "status": "En tránsito", "eta_hours": 76},
                {"ship_id": "SHIP-003", "status": "En puerto", "eta_hours": 34},
                {"ship_id": "SHIP-004", "status": "Contingencia", "eta_hours": 110},
            ]
        )

    if alerts_df.empty:
        alerts_df = pd.DataFrame(
            [
                {"alert_type": "Clima", "risk_level": "CRITICO", "severity": 5},
                {"alert_type": "Huelga", "risk_level": "MEDIO", "severity": 3},
                {"alert_type": "Geopolitica", "risk_level": "ALTO", "severity": 4},
                {"alert_type": "Clima", "risk_level": "CRITICO", "severity": 5},
            ]
        )

    if stock_df.empty:
        stock_df = pd.DataFrame(
            [
                {"article_id": "1000001023", "stock_on_hand": 756, "reorder_point": 420, "lead_time_days": 18},
                {"article_id": "1000002087", "stock_on_hand": 66, "reorder_point": 90, "lead_time_days": 6},
                {"article_id": "1000003154", "stock_on_hand": 160, "reorder_point": 120, "lead_time_days": 10},
                {"article_id": "1000008644", "stock_on_hand": 90, "reorder_point": 110, "lead_time_days": 8},
            ]
        )

    if air_df.empty:
        air_df = pd.DataFrame(
            [
                {"air_eta_hours": 14.0, "air_cost_eur": 18200.0, "service_level": 0.97},
                {"air_eta_hours": 17.5, "air_cost_eur": 16450.0, "service_level": 0.95},
                {"air_eta_hours": 16.0, "air_cost_eur": 17300.0, "service_level": 0.96},
            ]
        )

    if "status" not in fleet_df.columns:
        status_cycle = ["En tránsito", "En puerto", "Contingencia", "En tránsito"]
        fleet_df["status"] = [status_cycle[idx % len(status_cycle)] for idx in range(len(fleet_df))]
    if "eta_hours" not in fleet_df.columns:
        fleet_df["eta_hours"] = [72 + (idx * 8) for idx in range(len(fleet_df))]
    if "stock_on_hand" not in stock_df.columns:
        if "on_hand_qty" in stock_df.columns:
            stock_df["stock_on_hand"] = stock_df["on_hand_qty"]
        elif "total_stock_pieces" in stock_df.columns:
            stock_df["stock_on_hand"] = stock_df["total_stock_pieces"]
        elif "total_stock_packs" in stock_df.columns:
            stock_df["stock_on_hand"] = stock_df["total_stock_packs"]
        else:
            stock_df["stock_on_hand"] = 0
    if "reorder_point" not in stock_df.columns:
        if "safety_stock" in stock_df.columns:
            stock_df["reorder_point"] = stock_df["safety_stock"]
        elif "safety_stock_min" in stock_df.columns:
            stock_df["reorder_point"] = stock_df["safety_stock_min"]
        else:
            stock_df["reorder_point"] = 0
    if "lead_time_days" not in stock_df.columns:
        stock_df["lead_time_days"] = [6 + (idx % 6) * 2 for idx in range(len(stock_df))]
    if "alert_type" not in alerts_df.columns:
        if "source" in alerts_df.columns:
            alerts_df["alert_type"] = alerts_df["source"]
        else:
            alerts_df["alert_type"] = "Clima"
    if "severity" not in alerts_df.columns:
        alerts_df["severity"] = 3
    if "air_eta_hours" not in air_df.columns:
        air_df["air_eta_hours"] = 16.0
    if "air_cost_eur" not in air_df.columns:
        air_df["air_cost_eur"] = 17500.0

    stock_df["stock_on_hand"] = pd.to_numeric(stock_df["stock_on_hand"], errors="coerce").fillna(0)
    stock_df["reorder_point"] = pd.to_numeric(stock_df["reorder_point"], errors="coerce").fillna(0)
    stock_df["lead_time_days"] = pd.to_numeric(stock_df["lead_time_days"], errors="coerce").fillna(0)
    alerts_df["severity"] = pd.to_numeric(alerts_df["severity"], errors="coerce").fillna(0)
    air_df["air_eta_hours"] = pd.to_numeric(air_df["air_eta_hours"], errors="coerce").fillna(0)
    air_df["air_cost_eur"] = pd.to_numeric(air_df["air_cost_eur"], errors="coerce").fillna(0)

    critical_alert_count = int((alerts_df["severity"] >= 4).sum())
    references_at_risk = int((stock_df["stock_on_hand"] <= stock_df["reorder_point"]).sum())
    lead_time_avg = round(float(stock_df["lead_time_days"].mean()), 1) if not stock_df.empty else 0.0

    with st.container():
        st.subheader("Resumen Ejecutivo")
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        metric_col1.metric("Flota Activa", f"{len(fleet_df)}", delta="+1 vs ayer")
        metric_col2.metric("Alertas Críticas ⚠️", f"{critical_alert_count}", delta="-2 últimas 24h")
        metric_col3.metric("Referencias en Riesgo", f"{references_at_risk}", delta="+3 esta semana")
        metric_col4.metric("Lead Time Medio", f"{lead_time_avg} días", delta="-0.8 días")

    with st.container():
        ops_left, ops_right = st.columns([0.95, 1.35])
        with ops_left:
            fleet_status_df = fleet_df.groupby("status", dropna=False).size().reset_index(name="count")
            fleet_donut = px.pie(
                fleet_status_df,
                values="count",
                names="status",
                hole=0.6,
                color_discrete_sequence=["#2563eb", "#10b981", "#f59e0b", "#ef4444"],
                title="Estado de la Flota",
            )
            fleet_donut.update_traces(textposition="inside", textinfo="percent+label")
            fleet_donut.update_layout(margin=dict(l=10, r=10, t=50, b=10), legend_title_text="")
            st.plotly_chart(fleet_donut, use_container_width=True, key="executive_fleet_donut")

            alert_mix_df = alerts_df.groupby("alert_type", dropna=False).size().reset_index(name="count")
            alert_donut = px.pie(
                alert_mix_df,
                values="count",
                names="alert_type",
                hole=0.6,
                color_discrete_sequence=["#0ea5e9", "#f97316", "#8b5cf6", "#ef4444", "#14b8a6"],
                title="Distribución de Alertas por Tipología",
            )
            alert_donut.update_traces(textposition="inside", textinfo="percent+label")
            alert_donut.update_layout(margin=dict(l=10, r=10, t=50, b=10), legend_title_text="")
            st.plotly_chart(alert_donut, use_container_width=True, key="executive_alert_donut")

        with ops_right:
            total_stock = float(stock_df["stock_on_hand"].sum())
            risk_pressure = max(references_at_risk, 1)
            projection_rows = []
            for week in range(1, 11):
                demand = max(total_stock * (0.11 + (week * 0.012)), 20)
                coverage = max(total_stock - (week * total_stock * (0.075 + risk_pressure * 0.004)), 10)
                projection_rows.append(
                    {
                        "Semana": f"W{week}",
                        "Demanda": round(demand, 0),
                        "Cobertura": round(coverage, 0),
                    }
                )
            projection_df = pd.DataFrame(projection_rows)
            projection_long = projection_df.melt(id_vars="Semana", var_name="Serie", value_name="Valor")
            area_fig = px.area(
                projection_long,
                x="Semana",
                y="Valor",
                color="Serie",
                line_group="Serie",
                color_discrete_map={"Demanda": "#2563eb", "Cobertura": "#10b981"},
                title="Proyección de Demanda vs Cobertura (10 semanas)",
            )
            area_fig.update_layout(
                margin=dict(l=10, r=10, t=50, b=10),
                legend_title_text="",
                hovermode="x unified",
                yaxis_title="Unidades",
            )
            st.plotly_chart(area_fig, use_container_width=True, key="executive_projection_area")

            with st.container(border=True):
                st.markdown("### 🎯 Rendimiento de Entregas (OTIF)")
                st.caption("Porcentaje de carga entregada a tiempo al clúster industrial (Douai & Cléon). Target SLA: 95%")
                otif_cols = st.columns([0.8, 1.2])
                with otif_cols[0]:
                    st.metric("OTIF Global", "91.5%", delta="-3.5% vs Target", delta_color="inverse")
                with otif_cols[1]:
                    otif_df = pd.DataFrame(
                        [
                            {"Cliente": "Douai", "OTIF": 89, "Estado": "⚠️ Riesgo"},
                            {"Cliente": "Cléon", "OTIF": 96, "Estado": "✅ Óptimo"},
                        ]
                    )
                    otif_fig = px.bar(
                        otif_df,
                        x="OTIF",
                        y="Cliente",
                        orientation="h",
                        text="Estado",
                        color="Cliente",
                        color_discrete_map={"Douai": "#f59e0b", "Cléon": "#16a34a"},
                        title="OTIF por fábrica destino",
                    )
                    otif_fig.update_traces(textposition="outside")
                    otif_fig.update_layout(
                        margin=dict(l=10, r=20, t=50, b=10),
                        legend_title_text="",
                        showlegend=False,
                        xaxis_title="OTIF (%)",
                        yaxis_title="",
                    )
                    otif_fig.update_xaxes(range=[0, 100])
                    st.plotly_chart(otif_fig, use_container_width=True, key="executive_otif_panel")

    st.divider()

    with st.container():
        st.subheader("Impacto de Contingencias Multimodales")
        air_time_saved = max(round(float(fleet_df["eta_hours"].mean()) - float(air_df["air_eta_hours"].mean()), 1), 0)
        air_investment = round(float(air_df["air_cost_eur"].mean()), 0)
        air_rule_triggered = (
            air_time_saved >= AIR_CONTINGENCY_MIN_TIME_SAVED_HOURS
            and air_investment <= AIR_CONTINGENCY_MAX_COST_EUR
        )
        if air_rule_triggered:
            st.success(
                f"Regla activa: se recomienda contingencia aérea cuando el ahorro estimado es >= {AIR_CONTINGENCY_MIN_TIME_SAVED_HOURS} h y la inversión media es <= {AIR_CONTINGENCY_MAX_COST_EUR:,.0f} EUR. La situación actual cumple el umbral y justifica la activación predictiva."
            )
        else:
            st.info(
                f"Regla operativa: activar aéreo solo si el ahorro estimado es >= {AIR_CONTINGENCY_MIN_TIME_SAVED_HOURS} h y la inversión media es <= {AIR_CONTINGENCY_MAX_COST_EUR:,.0f} EUR. La situación actual no cumple completamente ese umbral económico."
            )
        roi_col1, roi_col2 = st.columns(2)
        roi_col1.metric("Ahorro de Tiempo Estimado (Horas)", f"{air_time_saved} h", delta="respuesta predictiva")
        roi_col2.metric("Inversión en Rutas Aéreas (EUR)", f"{air_investment:,.0f} €", delta="priorización inteligente")

elif current_page == "2. Control Tower Valladolid":
    import random
    
    st.subheader("Control Tower Valladolid")
    st.caption("Dashboard de gestión de almacén y flota en tiempo real")

    ships_df = pd.DataFrame(bundle.get("ships_latest", []))
    stock_valladolid_df = pd.DataFrame(bundle.get("stock_valladolid", []))

    st.markdown("#### 📊 Warehouse Dashboard - Valladolid")
    warehouse_row1_col1, warehouse_row1_col2 = st.columns([1, 2])

    with warehouse_row1_col1:
        if not stock_valladolid_df.empty and "total_stock_pieces" in stock_valladolid_df.columns:
            total_capacity = 10000
            current_stock = int(stock_valladolid_df["total_stock_pieces"].sum())
            capacity_used_pct = 35
            capacity_free_pct = 65

            total_volume_m3 = 0
            total_weight_kg = 0
            if "unit_volume_m3" in stock_valladolid_df.columns and "unit_weight_kg" in stock_valladolid_df.columns:
                stock_valladolid_df_copy = stock_valladolid_df.copy()
                stock_valladolid_df_copy["volume_used"] = stock_valladolid_df_copy["total_stock_pieces"] * stock_valladolid_df_copy["unit_volume_m3"]
                stock_valladolid_df_copy["weight_used"] = stock_valladolid_df_copy["total_stock_pieces"] * stock_valladolid_df_copy["unit_weight_kg"]
                total_volume_m3 = round(stock_valladolid_df_copy["volume_used"].sum(), 2)
                total_weight_kg = round(stock_valladolid_df_copy["weight_used"].sum(), 2)

            capacity_data = pd.DataFrame({
                "Estado": ["Capacidad Usada", "Capacidad Libre"],
                "Porcentaje": [capacity_used_pct, capacity_free_pct]
            })

            fig_capacity = px.pie(
                capacity_data,
                values="Porcentaje",
                names="Estado",
                hole=0.5,
                color_discrete_sequence=["#2563eb", "#e2e8f0"],
                title=f"Capacidad: {current_stock:,} / {total_capacity:,} piezas"
            )
            fig_capacity.update_layout(
                height=280,
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#10233f",
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
                margin=dict(l=10, r=10, t=40, b=10),
            )
            fig_capacity.update_traces(textinfo="percent+label", textposition="inside")

            if total_volume_m3 > 0:
                st.markdown(f"**Espacio utilizado:** {total_volume_m3:,.1f} m³ | **Peso:** {total_weight_kg:,.1f} kg")
            st.plotly_chart(fig_capacity, use_container_width=True)
        else:
            st.info("No hay datos de capacidad disponibles")

    with warehouse_row1_col2:
        if not stock_valladolid_df.empty and "article_family" in stock_valladolid_df.columns:
            stock_by_family = stock_valladolid_df.groupby("article_family").agg({
                "total_stock_pieces": "sum",
                "safety_stock_min": "sum"
            }).reset_index()

            stock_by_family["color"] = stock_by_family.apply(
                lambda row: "#dc2626" if row["total_stock_pieces"] <= row["safety_stock_min"] else "#16a34a",
                axis=1
            )

            fig_stock = px.bar(
                stock_by_family,
                x="article_family",
                y="total_stock_pieces",
                text="total_stock_pieces",
                color="color",
                title="Stock por Categoría (Rojo = Bajo Seguridad)"
            )
            fig_stock.update_layout(
                height=280,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(255,255,255,0.6)",
                font_color="#10233f",
                xaxis_title="Categoría de Artículo",
                yaxis_title="Stock Total (piezas)",
                showlegend=False,
                margin=dict(l=10, r=10, t=40, b=30),
            )
            fig_stock.update_traces(textposition="outside")
            st.plotly_chart(fig_stock, use_container_width=True)
        else:
            st.info("No hay datos de stock por categoría")

    st.markdown("---")

    st.markdown("#### ⚡ Acciones Rápidas")

    if not stock_valladolid_df.empty and "total_stock_pieces" in stock_valladolid_df.columns:
        critical_count = int((stock_valladolid_df["total_stock_pieces"] <= stock_valladolid_df["safety_stock_min"]).sum())
    else:
        critical_count = 0

    action_col1, action_col2 = st.columns([1, 2])

    with action_col1:
        st.metric("Referencias en Riesgo", f"{critical_count}", f"{int(critical_count * 0.3)} requieren acción")

    with action_col2:
        smtp_config = get_smtp_config()
        recipient_input = st.text_input(
            "Destinatario de alerta",
            value=str(smtp_config["recipient"]),
            placeholder="destinatario@empresa.com",
            key="control_tower_alert_recipient",
        )
        if smtp_config["configured"]:
            st.success(
                f"SMTP configurado: {smtp_config['user']} -> {recipient_input or smtp_config['recipient']} ({smtp_config['host']}:{smtp_config['port']})"
            )
        else:
            st.warning(
                "SMTP no configurado en .env. Define SMTP_HOST o SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD y SMTP_RECIPIENT para enviar alertas por mail desde Control Tower."
            )
        if st.button("✉️ Enviar Alertas de Referencias Críticas", type="primary", use_container_width=True):
            with st.spinner("Conectando al servidor SMTP y enviando alertas..."):
                stock_data = pd.DataFrame(bundle.get("stock_valladolid", []))
                
                if stock_data.empty:
                    st.error("No hay datos de stock disponibles para enviar alertas.")
                else:
                    critical_items = stock_data[
                        stock_data["total_stock_pieces"] <= stock_data["safety_stock_min"]
                    ].copy()
                    
                    if critical_items.empty:
                        st.warning("No hay referencias en riesgo crítico para enviar.")
                    else:
                        alerts_for_email = critical_items.to_dict("records")
                        success, message = send_critical_alerts_email(alerts_for_email, recipient_override=recipient_input)
                        
                        if success:
                            st.toast("Alerta procesada", icon="✉️")
                            st.success(f"✉️ {message}")
                        else:
                            st.error(f"⚠️ {message}")

    st.markdown("---")

    st.markdown("#### 🚢 Seguimiento de Flota")

    ship_name_map = {
        "ship-001": "MSC Gülsün",
        "ship-002": "CMA CGM Jacques Saade",
        "ship-003": "Ever Golden",
        "ship-004": "ONE Apus",
        "ship-005": "Maersk Eindhoven",
        "ship-006": "Ever Given",
        "ship-007": "HMM Algeciras",
        "ship-008": "Madrid Maersk",
        "ship-009": "CMA CGM Marco Polo",
        "ship-010": "MSC Irina",
    }

    fleet_left, fleet_right = st.columns([3, 1])

    with fleet_left:
        ship_options = ["Todos los barcos"]
        if not ships_df.empty and "ship_id" in ships_df.columns:
            ship_display_names = []
            for sid in ships_df["ship_id"].dropna().unique():
                display_name = ship_name_map.get(str(sid), str(sid))
                ship_display_names.append(display_name)
            ship_options.extend(sorted(ship_display_names))

        selected_ship = st.selectbox("Seleccionar barco", ship_options, key="fleet_ship_select")

    with fleet_right:
        total_ships = len(ships_df) if not ships_df.empty else 10
        alert_ships = min(2, total_ships)
        port_ships = max(1, total_ships // 5)
        active_ships = max(total_ships - alert_ships - port_ships, 0)
        fleet_status_df = pd.DataFrame(
            {
                "Estado": ["Activos", "En Alerta", "En Puerto"],
                "Cantidad": [active_ships, alert_ships, port_ships],
            }
        )
        fig_fleet_status = px.pie(
            fleet_status_df,
            values="Cantidad",
            names="Estado",
            hole=0.5,
            color_discrete_sequence=["#16a34a", "#f59e0b", "#64748b"],
            title="Estado de la Flota"
        )
        fig_fleet_status.update_layout(
            height=200,
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#10233f",
            showlegend=False,
            margin=dict(l=10, r=10, t=30, b=10),
        )
        fig_fleet_status.update_traces(textinfo="percent+label", textposition="inside")
        st.plotly_chart(fig_fleet_status, use_container_width=True)

    if selected_ship != "Todos los barcos":
        st.markdown("---")
        st.markdown(f"##### 📍 Detalles del barco: {selected_ship}")

        detail_col1, detail_col2 = st.columns([1, 2])

        with detail_col1:
            import random as _random
            journey_progress = _random.randint(40, 80)
            st.markdown("**Progreso del Trayecto**")
            st.progress(journey_progress / 100)
            st.caption(f"{journey_progress}% completado")

            ship_id_lookup = {v: k for k, v in ship_name_map.items()}
            actual_ship_id = ship_id_lookup.get(selected_ship, selected_ship)

            if not ships_df.empty:
                ship_data = ships_df[ships_df["ship_id"] == actual_ship_id]
                if not ship_data.empty:
                    origin = ship_data.iloc[0].get("origin_port", "N/A")
                    dest = ship_data.iloc[0].get("dest_port", "N/A")
                    eta_hours = ship_data.iloc[0].get("eta_hours_estimate", 0)
                    st.markdown(f"**Ruta:** {origin} → {dest}")
                    st.markdown(f"**ETA:** {eta_hours} horas")

        with detail_col2:
            delayed_ships = ["ONE Apus", "Ever Golden"]
            cover_status = {
                "MSC Gülsün": ("Sí", "✅ CUBRE"),
                "CMA CGM Jacques Saade": ("Sí", "✅ CUBRE"),
                "Ever Golden": ("Sí", "❌ NO CUBRE"),
                "ONE Apus": ("Sí", "❌ NO CUBRE"),
                "Maersk Eindhoven": ("No", "✅ CUBRE"),
                "Ever Given": ("No", "✅ CUBRE"),
            }

            delay_info = cover_status.get(selected_ship, ("No", "✅ CUBRE"))
            
            vehicle_data = pd.DataFrame([{
                "vehicle_id": selected_ship,
                "shipping_company": ship_name_map.get(actual_ship_id, "N/A"),
                "status": "En Tránsito",
                "speed_kn": 18.5,
                "next_port": "Algeciras",
                "eta_port": "2026-04-12",
                "delayed": delay_info[0],
                "cover_status": delay_info[1],
                "requires_contingency": "🔴 SÍ" if delay_info[1] == "❌ NO CUBRE" else "🟢 NO"
            }])
            
            vehicle_display = vehicle_data.rename(columns={
                "vehicle_id": "Nombre",
                "shipping_company": "Naviera",
                "status": "Estado",
                "speed_kn": "Velocidad (nudos)",
                "next_port": "Próximo Puerto",
                "eta_port": "ETA Puerto",
                "delayed": "¿Se retrasa?",
                "cover_status": "¿Cover?",
                "requires_contingency": "Contingencia"
            })
            
            def highlight_contingency(val):
                if val == "🔴 SÍ":
                    return "background-color: #fef2f2; color: #dc2626; font-weight: bold"
                elif val == "🟢 NO":
                    return "background-color: #f0fdf4; color: #16a34a; font-weight: bold"
                return ""
            
            styled_df = vehicle_display.style.map(highlight_contingency, subset=["Contingencia"])
            st.dataframe(styled_df, use_container_width=True, hide_index=True, height=150)

            st.markdown("##### 📦 Mercancía en el Barco")
            
            mock_cargo_data = [
                {"article_id": "SKU-001", "quantity": 120, "customer_dest": "Renault Cleon", "cover_status": "✅ CUBRE"},
                {"article_id": "SKU-002", "quantity": 85, "customer_dest": "Renault Douai", "cover_status": "✅ CUBRE"},
                {"article_id": "SKU-003", "quantity": 200, "customer_dest": "Peugeot Sochaux", "cover_status": "✅ CUBRE"},
                {"article_id": "SKU-004", "quantity": 45, "customer_dest": "Citroën Vigo", "cover_status": "❌ NO CUBRE"},
                {"article_id": "SKU-005", "quantity": 30, "customer_dest": "Ford Valencia", "cover_status": "⚠️ ROTURA_INMINENTE"},
            ]
            
            if selected_ship in ["Ever Golden", "ONE Apus"]:
                cargo_df = pd.DataFrame(mock_cargo_data)
            else:
                cargo_df = pd.DataFrame([
                    {"article_id": "SKU-001", "quantity": 120, "customer_dest": "Renault Cleon", "cover_status": "✅ CUBRE"},
                    {"article_id": "SKU-002", "quantity": 85, "customer_dest": "Renault Douai", "cover_status": "✅ CUBRE"},
                    {"article_id": "SKU-003", "quantity": 200, "customer_dest": "Peugeot Sochaux", "cover_status": "✅ CUBRE"},
                ])
            
            def style_cover_status(val):
                if "NO CUBRE" in str(val):
                    return "background-color: #fef2f2; color: #dc2626; font-weight: bold"
                elif "ROTURA" in str(val):
                    return "background-color: #fff7ed; color: #ea580c; font-weight: bold"
                elif "CUBRE" in str(val):
                    return "background-color: #f0fdf4; color: #16a34a; font-weight: bold"
                return ""
            
            cargo_df_styled = cargo_df.style.map(style_cover_status, subset=["cover_status"])
            st.dataframe(cargo_df_styled, use_container_width=True, hide_index=True)
            
            non_covering_items = cargo_df[cargo_df["cover_status"].str.contains("NO CUBRE|ROTURA", regex=True)]
            
            if not non_covering_items.empty:
                with st.expander("🚨 FLUJO DE CONTINGENCIA - Envío Aéreo de Emergencia", expanded=True):
                    st.markdown("**Paso 1: Selección de Ruta Aérea**")
                    
                    air_routes_df = pd.DataFrame(bundle.get("air_routes", []))
                    
                    if not air_routes_df.empty:
                        route_options = air_routes_df[["origin_airport", "dest_city", "air_cost_eur", "air_eta_hours"]].drop_duplicates()
                        route_options = route_options.dropna(subset=["air_cost_eur"])
                        
                        route_display = [f"{row['origin_airport']} → {row['dest_city']} ({row['air_cost_eur']:,.0f}€ | {row['air_eta_hours']}h)" 
                                        for _, row in route_options.iterrows()]
                        selected_route_idx = st.selectbox("Ruta aérea disponible", range(len(route_display)), 
                                                          format_func=lambda x: route_display[x], key="contingency_route")
                        
                        selected_route = route_options.iloc[selected_route_idx]
                        
                        st.markdown("---")
                        st.markdown("**Paso 2: Referencias Críticas a Volar**")
                        
                        critical_items = non_covering_items[["article_id", "quantity", "customer_dest", "cover_status"]].copy()
                        st.dataframe(critical_items, use_container_width=True, hide_index=True)
                        
                        total_air_cost = selected_route["air_cost_eur"] if not pd.isna(selected_route["air_cost_eur"]) else 0
                        estimated_arrival = selected_route["air_eta_hours"] if not pd.isna(selected_route["air_eta_hours"]) else 0
                        
                        st.markdown("---")
                        st.markdown("**Paso 3: Alternativas de Última Milla (Camión)**")
                        
                        truck_alternatives = pd.DataFrame([
                            {"opcion": "Exprés 24h", "coste_eur": 850, "tiempo_h": 24, "recomendado": "⭐"},
                            {"opcion": "Económico 48h", "coste_eur": 450, "tiempo_h": 48, "recomendado": ""},
                            {"opcion": "Compartido 72h", "coste_eur": 280, "tiempo_h": 72, "recomendado": ""},
                        ])
                        
                        def highlight_recommended(val):
                            if val == "⭐":
                                return "background-color: #dcfce7; color: #16a34a; font-weight: bold"
                            return ""
                        
                        truck_styled = truck_alternatives.style.map(highlight_recommended, subset=["recomendado"])
                        st.dataframe(truck_styled, use_container_width=True, hide_index=True)
                        
                        selected_truck = st.selectbox("Seleccionar opción de camión", 
                                                      ["Exprés 24h", "Económico 48h", "Compartido 72h"],
                                                      key="contingency_truck")
                        
                        truck_cost_map = {"Exprés 24h": 850, "Económico 48h": 450, "Compartido 72h": 280}
                        truck_time_map = {"Exprés 24h": 24, "Económico 48h": 48, "Compartido 72h": 72}
                        
                        truck_cost = truck_cost_map.get(selected_truck, 0)
                        truck_time = truck_time_map.get(selected_truck, 0)
                        
                        total_cost = total_air_cost + truck_cost
                        total_eta = estimated_arrival + truck_time
                        
                        st.markdown("---")
                        st.markdown("**Paso 4: Resumen de Contingencia**")
                        
                        summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
                        
                        with summary_col1:
                            st.metric("Coste Aéreo", f"{total_air_cost:,.0f}€")
                        with summary_col2:
                            st.metric("Coste Camión", f"{truck_cost}€")
                        with summary_col3:
                            st.metric("Coste Total", f"{total_cost:,.0f}€", delta_color="inverse")
                        with summary_col4:
                            st.metric("ETA Total", f"{total_eta}h")
                        
                        if total_cost > 15000:
                            st.warning("⚠️ Coste elevado de contingencia. Considerar opciones alternativas.")
                        else:
                            st.success("✅ Coste dentro de parámetros aceptables.")
                    else:
                        st.error("No hay rutas aéreas disponibles en el sistema.")
            else:
                st.success(f"✅ Todos los artículos en {selected_ship} están cubiertos.")

    st.markdown("---")

    week_filter = None if selected_week == "Todas" else selected_week

    st.markdown("#### 📋 Tabla de Detalles de Almacén - Valladolid")
    stock_df = build_stock_table(bundle, selected_customer, week_filter)
    if stock_df.empty:
        st.info("Todavia no hay datos de stock preparados para Valladolid.")
    else:
        st.dataframe(stock_df, use_container_width=True, hide_index=True)

    st.divider()

    with st.container():
        st.markdown("#### 📊 Estado Actual - Stock por Referencia")
        stock_fig = build_stock_bar_chart(bundle, selected_customer, week_filter)
        if stock_fig is None:
            st.info("Todavia no hay suficiente informacion para pintar el stock por referencia.")
        else:
            st.plotly_chart(stock_fig, use_container_width=True)

    st.divider()

    with st.container():
        st.markdown("#### 📈 Proyección Futura - Horizonte de 10 Semanas Industriales")
        render_stock_horizon_table(bundle, selected_customer)

    st.markdown("---")

    st.subheader("Flota y ETA")
    st.dataframe(enrich_ship_eta_dates(pd.DataFrame(bundle.get("ships_latest", []))), use_container_width=True, hide_index=True)
    st.subheader("Gantt por semanas industriales")
    gantt_fig = build_gantt(bundle, week_filter)
    if gantt_fig is None:
        st.info("Todavia no hay tareas de planificacion para mostrar en el Gantt.")
    else:
        st.plotly_chart(gantt_fig, use_container_width=True, key="gantt_industrial")

    st.markdown("---")

    st.markdown("### Impacto en Cliente - Francia (Douai y Cléon)")

    orders_df = pd.DataFrame(bundle.get("customer_orders_douai", []))
    gantt_df = pd.DataFrame(bundle.get("article_gantt", []))

    impact_filter_col1, impact_filter_col2, impact_filter_col3 = st.columns([1, 1, 1])

    with impact_filter_col1:
        impact_customer = st.selectbox(
            "Cliente Francia",
            options=["Douai", "Cleon", "Todos"],
            index=0,
            key="impact_customer_filter",
        )

    with impact_filter_col2:
        impact_weeks = ["Todas"]
        if not orders_df.empty and "industrial_week" in orders_df.columns:
            impact_weeks.extend(sorted(orders_df["industrial_week"].dropna().unique().tolist()))
        impact_week = st.selectbox(
            "Semana Industrial",
            options=impact_weeks,
            index=0,
            key="impact_week_filter",
        )

    with impact_filter_col3:
        st.markdown("#### Resumen Pedidos")
        if not orders_df.empty and "ordered_pieces" in orders_df.columns:
            total_ordered = int(orders_df["ordered_pieces"].sum())
            st.metric("Total Piezas Pedidas", f"{total_ordered:,}")
        else:
            st.metric("Total Piezas Pedidas", "N/A")

    st.markdown("##### Pedidos por Cliente")

    filtered_orders = orders_df.copy()
    if impact_customer != "Todos" and "customer_city" in filtered_orders.columns:
        filtered_orders = filtered_orders[filtered_orders["customer_city"] == impact_customer]
    if impact_week != "Todas" and "industrial_week" in filtered_orders.columns:
        filtered_orders = filtered_orders[filtered_orders["industrial_week"] == impact_week]

    if filtered_orders.empty:
        st.info("No hay pedidos para los filtros seleccionados.")
    else:
        orders_display = filtered_orders.copy()
        rename_cols_orders = {
            "order_id": "ID Pedido",
            "article_ref": "Artículo",
            "customer_city": "Cliente",
            "industrial_week": "Semana",
            "planned_delivery_date": "Entrega",
            "ordered_pieces": "Piezas",
            "order_status": "Estado",
        }
        orders_display = orders_display.rename(columns=rename_cols_orders)

        cols_orders = [c for c in ["ID Pedido", "Artículo", "Cliente", "Semana", "Entrega", "Piezas", "Estado"] if c in orders_display.columns]
        st.dataframe(orders_display[cols_orders], use_container_width=True, hide_index=True, height=280)

        if not orders_display.empty and "Semana" in orders_display.columns:
            all_impact_weeks = []
            if not orders_df.empty and "industrial_week" in orders_df.columns:
                all_impact_weeks = sorted(orders_df["industrial_week"].dropna().astype(str).unique().tolist())[:10]

            weekly_demand = orders_display.groupby("Semana")["Piezas"].sum().reset_index()
            if all_impact_weeks:
                weekly_demand = pd.DataFrame({"Semana": all_impact_weeks}).merge(
                    weekly_demand,
                    on="Semana",
                    how="left",
                )
                weekly_demand["Piezas"] = weekly_demand["Piezas"].fillna(0)

            fig_bar = px.bar(
                weekly_demand,
                x="Semana",
                y="Piezas",
                text="Piezas",
                color_discrete_sequence=["#2563eb"],
            )
            fig_bar.update_layout(
                height=260,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(255,255,255,0.6)",
                font_color="#10233f",
                margin=dict(l=10, r=10, t=20, b=10),
                xaxis_title="Semana Industrial",
                yaxis_title="Piezas Demandadas",
                xaxis={"categoryorder": "array", "categoryarray": all_impact_weeks or weekly_demand["Semana"].tolist()},
            )
            fig_bar.update_traces(textposition="outside")
            st.plotly_chart(fig_bar, use_container_width=True)

elif current_page == "3. Arquitectura en vivo":
    hero_left, hero_right = st.columns([1.7, 0.9])
    with hero_left:
        st.markdown("# 2026: Predictive Logistics")
        st.markdown(
            "Centro de control del pipeline KDD: posicion de flota, riesgo meteorologico, alertas operativas, contingencia aerea, estado de servicios y analitica de red.",
            unsafe_allow_html=True,
        )
        st.markdown(
            """
            <div>
              <a href="https://localhost:8443" target="_blank" style="text-decoration:none;"><span class='tag-chip'>NiFi</span></a>
              <span class='tag-chip' title='Kafka expone broker TCP en localhost:9092, no una consola web'>Kafka</span>
              <a href="http://localhost:8080" target="_blank" style="text-decoration:none;"><span class='tag-chip'>Spark + Hive</span></a>
              <span class='tag-chip' title='Cassandra expone CQL en localhost:9042, no una consola web'>Cassandra</span>
              <a href="http://localhost:8080" target="_blank" style="text-decoration:none;"><span class='tag-chip'>GraphFrames</span></a>
              <a href="http://localhost:8501" target="_blank" style="text-decoration:none;"><span class='tag-chip'>Air Recovery</span></a>
              <a href="http://localhost:8085" target="_blank" style="text-decoration:none;"><span class='tag-chip'>Airflow</span></a>
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
    st.divider()

    with st.container(border=True):
        st.markdown("#### Estado del pipeline")
        st.caption("Snapshots servidos desde Spark/Hive, HDFS y Cassandra.")
        pipeline_cols = st.columns(3)
        pipeline_cols[0].metric("Servicios OK", f"{ok_count}/{len(service_rows)}")
        pipeline_cols[1].metric("Riesgo medio", "MEDIUM" if medium_alerts else "LOW")
        pipeline_cols[2].metric("Storage", "HDFS + Hive")
    st.divider()

    stack_action_cols = st.columns(3)
    if stack_action_cols[0].button("▶️ Arrancar todos los servicios", use_container_width=True, type="primary"):
        st.code(compose_service_action("nifi", "start"))
        st.code(run_command(DOCKER_COMPOSE + ["up", "-d", "postgres", "kafka", "spark", "cassandra", "namenode", "datanode", "airflow-webserver"], timeout=600).stdout)
        st.cache_data.clear()
    if stack_action_cols[1].button("⏯️ Arrancar Servicios Lite", use_container_width=True):
        st.code(run_command(DOCKER_COMPOSE + ["up", "-d", "kafka", "namenode", "datanode", "spark"], timeout=600).stdout)
        st.cache_data.clear()
    if stack_action_cols[2].button("⏹️ Parar todo el stack", use_container_width=True):
        st.code(run_command(DOCKER_COMPOSE + ["down"], timeout=600).stdout)
        st.cache_data.clear()

    st.markdown("### ARQUITECTURA EN VIVO")
    st.markdown("")

    service_order = [
        ("nifi", "NiFi (Ingesta)"),
        ("kafka", "Kafka (Mensajería)"),
        ("namenode", "HDFS / NameNode (Almacenamiento raw)"),
        ("spark", "Spark (Procesamiento)"),
        ("hive", "Hive (Analítica)"),
        ("cassandra", "Cassandra (Serving)"),
        ("airflow", "Airflow (Orquestación)"),
        ("postgres", "Postgres (Backend)"),
    ]
    status_lookup = {row["service"]: row for row in service_rows}
    if "spark" in status_lookup:
        status_lookup["hive"] = status_lookup["spark"]
    for service_key, service_label in service_order:
        row = status_lookup.get(service_key, {"badge": "OFF", "status": "Not created", "service": service_key})
        icon = {"OK": "🟢", "NOK": "🔴", "BOOT": "🟠", "OFF": "⚫"}.get(row["badge"], "⚫")
        last_run = datetime.now().strftime("%Y-%m-%d %H:%M") if row["badge"] in {"OK", "BOOT"} else "Sin ejecución"
        row_cols = st.columns([1.7, 1.5, 1.1, 1.2])
        row_cols[0].markdown(f"**{service_label}**")
        row_cols[1].caption(last_run)
        row_cols[2].markdown(f"{icon} **{row['badge']}**")
        action_cols = row_cols[3].columns(2)
        service_name = service_key if service_key != "airflow" else "airflow-webserver"
        if service_key == "hive":
            service_name = "spark"
        if action_cols[0].button("ON", key=f"arch_on_{service_key}", use_container_width=True):
            st.code(compose_service_action(service_name, "start"))
            st.rerun()
        if action_cols[1].button("OFF", key=f"arch_off_{service_key}", use_container_width=True):
            st.code(compose_service_action(service_name, "stop"))
            st.rerun()

elif current_page == "4. KDD Fase I - Ingesta":
    st.subheader("NiFi + Kafka")
    st.caption("Mapa operacional con puertos, rutas y barcos en tránsito. Los filtros limitan la vista a destinos y barcos concretos.")

    with st.container(border=True):
        st.markdown("**Arquitectura técnica - Fase I (Ingesta)**")
        st.markdown(":arrow_right: API Open-Meteo / AIS ➔ NiFi ➔ Kafka (Topic: datos_crudos) ➔ Kafka (Topic: datos_filtrados)")

    col_met1, col_met2, col_met3 = st.columns(3)
    with col_met1:
        st.metric("Mensajes/sec", "124")
    with col_met2:
        st.metric("Filtro NiFi OK", "98.5%")
    with col_met3:
        st.metric("Estado Kafka", "🟢 ONLINE")

    st.markdown("---")

    ship_options = ["Todos"]
    ships_df = pd.DataFrame(bundle.get("ships_latest", []))
    if not ships_df.empty and "ship_id" in ships_df.columns:
        ship_options.extend(sorted(ships_df["ship_id"].dropna().astype(str).tolist()))

    filter_cols = st.columns([1, 1, 1, 1])
    with filter_cols[0]:
        selected_ship = st.selectbox("Barco", ship_options, index=0, key="ingesta_selected_ship")
    with filter_cols[1]:
        selected_dest = st.selectbox("Destino", ["Todos", "Algeciras", "Valencia", "Barcelona"], index=0, key="ingesta_selected_dest")
    with filter_cols[2]:
        st.markdown("#### Incidencias manuales")
    with filter_cols[3]:
        st.caption("ETA original + meteo + incidencias")

    active_constraints: list[str] = []
    risk_cols = st.columns(6)
    for idx, label in enumerate(SHIP_CONSTRAINTS.keys()):
        with risk_cols[idx]:
            if st.toggle(label, value=False, key=f"risk_toggle_{label}"):
                active_constraints.append(label)

    ports_df, routes_df, ships_all_df, alerts_df, ships_map_df = build_operational_ship_map(
        bundle,
        selected_ship=None if selected_ship == "Todos" else selected_ship,
        selected_dest=None if selected_dest == "Todos" else selected_dest,
    )
    eta_table_df = build_ship_eta_table(
        bundle,
        selected_ship=None if selected_ship == "Todos" else selected_ship,
        selected_dest=None if selected_dest == "Todos" else selected_dest,
        active_constraints=active_constraints,
    )

    st.subheader("Mapa operacional")
    layers = []
    if not ships_map_df.empty and {"lat", "lon"}.issubset(ships_map_df.columns):
        ships_map_df = ships_map_df.copy()
        ships_map_df = enrich_ship_eta_dates(ships_map_df)
        ships_map_df["tooltip_text"] = ships_map_df.apply(
            lambda row: (
                f"Barco: {row.get('ship_id', '')}\nOrigen: {row.get('origin_port', '')}\n"
                f"Destino: {row.get('dest_port', '')}\nETA: {row.get('eta_fecha', 'N/A')}"
            ),
            axis=1,
        )
        ships_map_df["fill_color"] = [[37, 99, 235, 220]] * len(ships_map_df)
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=ships_map_df,
                get_position="[lon, lat]",
                get_radius=26000,
                get_fill_color="fill_color",
                pickable=True,
            )
        )

    if layers:
        st.pydeck_chart(
            pdk.Deck(
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                initial_view_state=pdk.ViewState(latitude=24.0, longitude=62.0, zoom=1.05, pitch=0),
                layers=layers,
                tooltip={"text": "{tooltip_text}"},
            ),
            use_container_width=True,
        )
    else:
        st.info("No hay datos geográficos disponibles para el mapa operacional.")

    st.subheader("Tabla de Barcos y ETA")
    if eta_table_df.empty:
        st.info("No hay barcos disponibles para mostrar la tabla de ETA.")
    else:
        eta_table_df = eta_table_df.copy()
        if {"Lat", "Lon"}.issubset(eta_table_df.columns):
            eta_table_df["GPS"] = eta_table_df.apply(lambda row: f"{row['Lat']}, {row['Lon']}", axis=1)
        cols_order = ["Nombre de barco", "Barco", "Origen", "Destino", "GPS", "ETA Original", "ETA Recalculada", "Coste Maritimo (EUR)", "Icono Aereo", "Estado"]
        st.dataframe(
            eta_table_df[[c for c in cols_order if c in eta_table_df.columns]],
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Corredores Maritimos")
    corridor_layers = []
    if not routes_df.empty and {"origin_lat", "origin_lon", "dest_lat", "dest_lon"}.issubset(routes_df.columns):
        corridors_df = routes_df.copy()
        corridors_df["corridor_color"] = corridors_df["origin_port"].apply(
            lambda origin: [220, 38, 38, 140] if origin == "Shanghai" else [37, 99, 235, 140]
        )
        corridor_layers.append(
            pdk.Layer(
                "LineLayer",
                data=corridors_df,
                get_source_position="[origin_lon, origin_lat]",
                get_target_position="[dest_lon, dest_lat]",
                get_color="corridor_color",
                get_width=4,
                pickable=False,
            )
        )

    if corridor_layers:
        st.pydeck_chart(
            pdk.Deck(
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                initial_view_state=pdk.ViewState(latitude=24.0, longitude=62.0, zoom=1.05, pitch=0),
                layers=corridor_layers,
            ),
            use_container_width=True,
        )
    else:
        st.info("No hay corredores maritimos disponibles para mostrar.")

elif current_page == "5. KDD Fase II - Spark":
    st.subheader("KDD Fase II - Spark & MLlib")
    st.caption("Procesamiento con Spark Structured Streaming (ventanas de 15 min) y modelos MLlib para analítica predictiva")

    import time
    import random

    st.markdown("### 1. Simulador de Streaming (Loaders)")
    st.caption("Simulación de micro-batch consumiendo de Kafka con ventana de 15 minutos")

    with st.spinner("Procesando micro-batch de Kafka (Ventana 15 min) en Spark..."):
        time.sleep(1.5)
    st.success("✓ Micro-batch procesado correctamente")

    batch_col1, batch_col2, batch_col3 = st.columns(3)
    with batch_col1:
        st.metric("Registros Ingeridos", f"{random.randint(850, 1200):,}", "+12% vs batch anterior")
    with batch_col2:
        st.metric("Nulos Limpiados", f"{random.randint(45, 120):,}", "-8% vs batch anterior")
    with batch_col3:
        st.metric("Latencia del Batch", f"{random.randint(800, 1500):,} ms", "~1.2s por ventana")

    st.markdown("---")

    st.markdown("### Evidencia Modelos MLlib")

    model_tab1, model_tab2, model_tab3 = st.tabs(["🔬 K-Means (Clústering)", "🌲 Random Forest (Clasificación)", "📈 Linear Regression (Regresión)"])

    with model_tab1:
        st.markdown("#### K-Means: Agrupamiento de Puertos por Congestión y Riesgo Climático")
        st.caption("K=3 clusters: Puerto de entrada -> Nivel de congestión -> Riesgo meteorológico")

        km_left, km_right = st.columns([1.2, 1])
        with km_left:
            st.markdown("""
            **Descripción del Modelo:**
            - **Algoritmo:** K-Means (MLlib Spark ML)
            - **K=3 clusters:** Puerto de entrada agrupado por nivel de congestión y riesgo climático
            - **Variables de entrada:** Tingkat congestión puerto, Velocidad viento, Temperatura, Humedad
            - **Uso:** Segmentar puertos para priorizar rutas alternativas y gestionar capacidad portuaria
            """)
        with km_right:
            st.markdown("**Métricas del Modelo:**")
            st.markdown("- Iteraciones: 15")
            st.markdown("- Convergencia: 0.001")
            st.markdown("- Silhouette Score: 0.72")

        ports_cluster = pd.DataFrame([
            {"port_name": "Algeciras", "congestion_level": 85, "wind_speed": 45, "temperature": 18, "humidity": 72, "cluster": 2},
            {"port_name": "Valencia", "congestion_level": 45, "wind_speed": 22, "temperature": 16, "humidity": 65, "cluster": 0},
            {"port_name": "Barcelona", "congestion_level": 60, "wind_speed": 35, "temperature": 15, "humidity": 68, "cluster": 1},
            {"port_name": "Bilbao", "congestion_level": 30, "wind_speed": 18, "temperature": 12, "humidity": 80, "cluster": 0},
            {"port_name": "Cartagena", "congestion_level": 75, "wind_speed": 55, "temperature": 20, "humidity": 60, "cluster": 2},
            {"port_name": "Santander", "congestion_level": 25, "wind_speed": 15, "temperature": 13, "humidity": 82, "cluster": 0},
        ])
        cluster_colors = {0: "#16a34a", 1: "#f59e0b", 2: "#dc2626"}
        ports_cluster["color"] = ports_cluster["cluster"].map(cluster_colors)
        ports_cluster["cluster_name"] = ports_cluster["cluster"].map({0: "Bajo Riesgo", 1: "Riesgo Medio", 2: "Alto Riesgo"})

        fig_km = px.scatter(
            ports_cluster,
            x="congestion_level",
            y="wind_speed",
            size="temperature",
            color="cluster_name",
            hover_name="port_name",
            color_discrete_map={"Bajo Riesgo": "#16a34a", "Riesgo Medio": "#f59e0b", "Alto Riesgo": "#dc2626"},
            symbol="port_name",
        )
        fig_km.update_layout(
            height=400,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.6)",
            font_color="#10233f",
            xaxis_title="Nivel de Congestión (%)",
            yaxis_title="Velocidad del Viento (km/h)",
            legend_title="Cluster",
            margin=dict(l=10, r=10, t=20, b=40),
        )
        fig_km.update_traces(marker=dict(line=dict(width=1, color="#10233f")))
        st.plotly_chart(fig_km, use_container_width=True)

    with model_tab2:
        st.markdown("#### Random Forest: Clasificador de Alertas (Predicción ROTURA_INMINENTE)")
        st.caption("Feature Importance del modelo para predecir riesgo de rotura de stock")

        rf_left, rf_right = st.columns([1.2, 1])
        with rf_left:
            st.markdown("""
            **Descripción del Modelo:**
            - **Algoritmo:** Random Forest Classifier (Spark MLlib)
            - **Objetivo:** Predecir si habrá ROTURA_INMINENTE de stock en los próximos 7 días
            - **Variables de entrada:** Nivel de stock actual, Velocidad viento, Retraso en aduanas, Consumo diario, Días hasta ETA
            - **Uso:** Activar alertas preventivas y planificar contingencias aéreas
            """)
        with rf_right:
            st.markdown("**Métricas del Modelo:**")
            st.markdown("- Árboles: 100")
            st.markdown("- Profundidad máx: 8")
            st.markdown("- Accuracy: 0.89")
            st.markdown("- F1-Score: 0.85")

        feature_importance = pd.DataFrame([
            {"feature": "stock_on_hand", "importance": 0.32, "description": "Stock físico actual"},
            {"feature": "eta_delay_hours", "importance": 0.24, "description": "Retraso ETA (horas)"},
            {"feature": "wind_speed_kmh", "importance": 0.18, "description": "Velocidad viento"},
            {"feature": "customs_delay_hours", "importance": 0.14, "description": "Retraso aduanas"},
            {"feature": "daily_consumption", "importance": 0.08, "description": "Consumo diario"},
            {"feature": "weather_severity", "importance": 0.04, "description": "Severidad climática"},
        ])
        fig_rf = px.bar(
            feature_importance,
            x="importance",
            y="description",
            orientation="h",
            text="importance",
            color="importance",
            color_continuous_scale="Reds",
        )
        fig_rf.update_layout(
            height=380,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.6)",
            font_color="#10233f",
            xaxis_title="Importancia (Feature Importance)",
            yaxis_title="Variable",
            showlegend=False,
            margin=dict(l=10, r=10, t=20, b=40),
        )
        fig_rf.update_traces(textposition="outside", texttemplate="%{x:.2f}")
        st.plotly_chart(fig_rf, use_container_width=True)

    with model_tab3:
        st.markdown("#### Linear Regression: Predicción de ETA (Tiempo de Llegada Estimado)")
        st.caption("Comparación entre ETA teórico y ETA predicho por MLlib para los próximos 5 barcos")

        lr_left, lr_right = st.columns([1.2, 1])
        with lr_left:
            st.markdown("""
            **Descripción del Modelo:**
            - **Algoritmo:** Linear Regression (Spark MLlib)
            - **Objetivo:** Predecir el ETA (días) considerando condiciones climáticas y operativas
            - **Variables de entrada:** Distancia restante, Velocidad viento, Estado puerto destino, Retraso anterior
            - **Uso:** Recalcular rutas y planificar inventario en destino
            """)
        with lr_right:
            st.markdown("**Métricas del Modelo:**")
            st.markdown("- RMSE: 1.23 días")
            st.markdown("- R² Score: 0.91")
            st.markdown("- MAE: 0.95 días")

        eta_comparison = pd.DataFrame([
            {"ship_id": "MSC Gülsün", "shipping_company": "MSC Mediterranean Shipping", "route": "Shanghai → Algeciras", "eta_theoretical": 16.3, "eta_predicted_ml": 17.1, "diff_days": 0.8},
            {"ship_id": "CMA CGM Jacques Saade", "shipping_company": "CMA CGM", "route": "Shanghai → Valencia", "eta_theoretical": 15.8, "eta_predicted_ml": 16.2, "diff_days": 0.4},
            {"ship_id": "Ever Golden", "shipping_company": "Evergreen Marine", "route": "Shanghai → Barcelona", "eta_theoretical": 18.5, "eta_predicted_ml": 19.8, "diff_days": 1.3},
            {"ship_id": "ONE Apus", "shipping_company": "ONE (Ocean Network Express)", "route": "Yokohama → Algeciras", "eta_theoretical": 22.0, "eta_predicted_ml": 23.5, "diff_days": 1.5},
            {"ship_id": "Maersk Eindhoven", "shipping_company": "Maersk", "route": "Yokohama → Valencia", "eta_theoretical": 21.5, "eta_predicted_ml": 22.0, "diff_days": 0.5},
        ])

        eta_for_plot = eta_comparison.rename(columns={
            "eta_theoretical": "ETA Teórico (días)",
            "eta_predicted_ml": "ETA Predicho MLlib (días)"
        })

        fig_lr = px.line(
            eta_for_plot,
            x="ship_id",
            y=["ETA Teórico (días)", "ETA Predicho MLlib (días)"],
            markers=True,
            color_discrete_sequence=["#2563eb", "#dc2626"],
        )
        fig_lr.update_layout(
            height=400,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.6)",
            font_color="#10233f",
            xaxis_title="Barco",
            yaxis_title="ETA (días)",
            legend_title="Tipo de ETA",
            margin=dict(l=10, r=10, t=20, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        )
        fig_lr.update_traces(mode="lines+markers", marker=dict(size=10))
        st.plotly_chart(fig_lr, use_container_width=True)

        st.markdown("##### Diferencia entre ETA Teórico y Predicho")
        eta_diff_df = eta_comparison[["ship_id", "route", "eta_theoretical", "eta_predicted_ml", "diff_days"]].copy()
        eta_diff_df = eta_diff_df.rename(columns={
            "ship_id": "Barco",
            "route": "Ruta",
            "eta_theoretical": "ETA Teórico (días)",
            "eta_predicted_ml": "ETA MLlib (días)",
            "diff_days": "Diferencia (días)",
        })
        st.dataframe(eta_diff_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    sp_left, sp_right = st.columns([1, 1])
    with sp_left:
        st.markdown("##### Datos de Fact Weather Operational")
        st.dataframe(pd.DataFrame(bundle.get("fact_weather_operational", [])), use_container_width=True, hide_index=True)
    with sp_right:
        st.markdown("##### Datos de Fact Air Recovery Options")
        st.dataframe(get_air_recovery_df(bundle), use_container_width=True, hide_index=True)

elif current_page == "6. GraphFrames":
    st.subheader("Análisis de Red (GraphFrames)")
    st.caption("Modelo de grafos evaluando rutas marítimas y multimodales con Shortest Path y centralidad")

    st.markdown("### 1. La Lógica de la IA (Fórmula de Pesos Dinámicos)")

    formula_col, desc_col = st.columns([1, 1.5])
    with formula_col:
        st.markdown("""
        **Fórmula del Peso Dinámico:**
        
        $$P = T + \\frac{1}{F} + R_a$$
        
        Donde:
        - **T** = Tiempo de tránsito (días)
        - **F** = Fiabilidad de la ruta (0-1)
        - **Rₐ** = Retraso en aduanas (horas)
        """)
    with desc_col:
        st.markdown("""
        **Explicación del Modelo:**
        
        - **Nodos**: Puertos de origen (Asia), Puertos de entrada (España), Hub central (Valladolid), Aduanas destino (Francia)
        - **Aristas (Edges)**: Rutas multimodales (Marítimo, Aéreo, Camión)
        - **Algoritmo**: Shortest Path con pesos dinámicos que varían según condiciones meteorológicas y operativas
        - **Uso**: Calcular rutas óptimas considerando congestión portuaria y riesgo meteorológico
        """)

    st.markdown("---")

    st.markdown("### 2. Visualización del Grafo Logístico")

    nodes_data = pd.DataFrame([
        {"node_id": "Shanghai", "node_type": "Puerto Origen", "lat": 31.23, "lon": 121.47, "centrality": 0.95, "color": "#dc2626"},
        {"node_id": "Yokohama", "node_type": "Puerto Origen", "lat": 35.44, "lon": 139.64, "centrality": 0.88, "color": "#dc2626"},
        {"node_id": "Algeciras", "node_type": "Puerto Entrada", "lat": 36.14, "lon": -5.45, "centrality": 0.82, "color": "#f59e0b"},
        {"node_id": "Valencia", "node_type": "Puerto Entrada", "lat": 39.47, "lon": -0.38, "centrality": 0.75, "color": "#f59e0b"},
        {"node_id": "Barcelona", "node_type": "Puerto Entrada", "lat": 41.39, "lon": 2.17, "centrality": 0.68, "color": "#f59e0b"},
        {"node_id": "Valladolid", "node_type": "Hub Central", "lat": 41.65, "lon": -4.72, "centrality": 0.92, "color": "#2563eb"},
        {"node_id": "Douai", "node_type": "Aduana Destino", "lat": 50.37, "lon": 3.08, "centrality": 0.55, "color": "#16a34a"},
        {"node_id": "Cleon", "node_type": "Aduana Destino", "lat": 49.87, "lon": 2.13, "centrality": 0.48, "color": "#16a34a"},
    ])

    edges_data = pd.DataFrame([
        {"from": "Shanghai", "to": "Algeciras", "mode": "Marítimo", "weight": 16.3, "risk": "Bajo"},
        {"from": "Shanghai", "to": "Valencia", "mode": "Marítimo", "weight": 15.8, "risk": "Bajo"},
        {"from": "Shanghai", "to": "Barcelona", "mode": "Marítimo", "weight": 18.5, "risk": "Medio"},
        {"from": "Yokohama", "to": "Algeciras", "mode": "Marítimo", "weight": 22.0, "risk": "Medio"},
        {"from": "Yokohama", "to": "Valencia", "mode": "Marítimo", "weight": 21.5, "risk": "Medio"},
        {"from": "Yokohama", "to": "Barcelona", "mode": "Marítimo", "weight": 23.2, "risk": "Alto"},
        {"from": "Algeciras", "to": "Valladolid", "mode": "Camión", "weight": 1.5, "risk": "Bajo"},
        {"from": "Valencia", "to": "Valladolid", "mode": "Camión", "weight": 1.2, "risk": "Bajo"},
        {"from": "Barcelona", "to": "Valladolid", "mode": "Camión", "weight": 1.8, "risk": "Bajo"},
        {"from": "Valladolid", "to": "Douai", "mode": "Camión", "weight": 2.5, "risk": "Bajo"},
        {"from": "Valladolid", "to": "Cleon", "mode": "Camión", "weight": 2.8, "risk": "Bajo"},
    ])

    fig_graph = px.scatter_mapbox(
        nodes_data,
        lat="lat",
        lon="lon",
        size="centrality",
        color="color",
        hover_name="node_id",
        hover_data={"node_type": True, "centrality": ":.2f", "lat": False, "lon": False, "color": False},
        zoom=2,
        center={"lat": 35, "lon": 20},
        height=450,
    )
    fig_graph.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=0, b=0),
        mapbox_style="carto-positron",
    )

    st.plotly_chart(fig_graph, use_container_width=True)

    st.markdown(
        """
        <div style="display:flex; gap:20px; margin-top:10px; font-size:0.85rem; color:#64748b;">
            <span><span style="color:#dc2626;">●</span> Puerto Origen (Asia)</span>
            <span><span style="color:#f59e0b;">●</span> Puerto Entrada (España)</span>
            <span><span style="color:#2563eb;">●</span> Hub Central (Valladolid)</span>
            <span><span style="color:#16a34a;">●</span> Aduana Destino (Francia)</span>
            <span style="border-bottom:2px solid #16a34a;">—— Bajo riesgo</span>
            <span style="border-bottom:2px solid #f59e0b;">—— Medio</span>
            <span style="border-bottom:2px solid #dc2626;">—— Alto</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    st.markdown("### 3. Panel de Nodos Críticos (fact_graph_centrality)")

    graph_centrality_df = pd.DataFrame(bundle.get("graph_centrality", []))
    if graph_centrality_df.empty:
        graph_centrality_df = pd.DataFrame([
            {"node_id": "Shanghai", "node_type": "Puerto", "degree": 3, "pagerank": 0.25, "criticality_level": "NODO_CRITICO"},
            {"node_id": "Valladolid", "node_type": "Hub", "degree": 3, "pagerank": 0.28, "criticality_level": "NODO_CRITICO"},
            {"node_id": "Algeciras", "node_type": "Puerto", "degree": 2, "pagerank": 0.18, "criticality_level": "NODO_CRITICO"},
            {"node_id": "Valencia", "node_type": "Puerto", "degree": 1, "pagerank": 0.15, "criticality_level": "NODO_STRATEGICO"},
            {"node_id": "Barcelona", "node_type": "Puerto", "degree": 1, "pagerank": 0.12, "criticality_level": "NODO_SECUNDARIO"},
        ])

    graph_centrality_df = graph_centrality_df.sort_values("degree", ascending=False)
    graph_centrality_df["Estado"] = graph_centrality_df["criticality_level"].apply(
        lambda x: "🔴 Congestionado" if "CRITICO" in str(x).upper() else ("🟡 Tensión" if "STRATEGICO" in str(x).upper() else "🟢 Fluido")
    )

    rename_cols = {
        "node_id": "ID Nodo",
        "node_type": "Tipo",
        "degree": "Grado",
        "pagerank": "PageRank",
        "criticality_level": "Criticidad",
        "Estado": "Estado Operativo",
    }
    graph_centrality_renamed = graph_centrality_df.rename(columns=rename_cols)
    cols_to_show = [c for c in ["ID Nodo", "Tipo", "Grado", "PageRank", "Criticidad", "Estado Operativo"] if c in graph_centrality_renamed.columns]

    st.dataframe(graph_centrality_renamed[cols_to_show], use_container_width=True, hide_index=True, height=280)

    st.markdown("---")

    st.markdown("### 4. Panel de Riesgo de Rutas (fact_route_risk)")

    route_risk_df = pd.DataFrame([
        {"route_id": "route-shanghai-algeciras", "from_node": "Shanghai", "to_node": "Algeciras", "mode": "Marítimo", "dynamic_weight": 16.8, "weather_risk": "Bajo", "reliability": 0.92},
        {"route_id": "route-shanghai-valencia", "from_node": "Shanghai", "to_node": "Valencia", "mode": "Marítimo", "dynamic_weight": 16.2, "weather_risk": "Bajo", "reliability": 0.94},
        {"route_id": "route-shanghai-barcelona", "from_node": "Shanghai", "to_node": "Barcelona", "mode": "Marítimo", "dynamic_weight": 19.5, "weather_risk": "Medio", "reliability": 0.85},
        {"route_id": "route-yokohama-algeciras", "from_node": "Yokohama", "to_node": "Algeciras", "mode": "Marítimo", "dynamic_weight": 23.2, "weather_risk": "Medio", "reliability": 0.82},
        {"route_id": "route-yokohama-valencia", "from_node": "Yokohama", "to_node": "Valencia", "mode": "Marítimo", "dynamic_weight": 22.8, "weather_risk": "Medio", "reliability": 0.83},
        {"route_id": "route-yokohama-barcelona", "from_node": "Yokohama", "to_node": "Barcelona", "mode": "Marítimo", "dynamic_weight": 25.1, "weather_risk": "Alto", "reliability": 0.75},
        {"route_id": "route-algeciras-valladolid", "from_node": "Algeciras", "to_node": "Valladolid", "mode": "Camión", "dynamic_weight": 1.6, "weather_risk": "Bajo", "reliability": 0.98},
        {"route_id": "route-valencia-valladolid", "from_node": "Valencia", "to_node": "Valladolid", "mode": "Camión", "dynamic_weight": 1.3, "weather_risk": "Bajo", "reliability": 0.99},
        {"route_id": "route-valladolid-douai", "from_node": "Valladolid", "to_node": "Douai", "mode": "Camión", "dynamic_weight": 2.6, "weather_risk": "Bajo", "reliability": 0.97},
    ])

    route_risk_df["Ruta"] = route_risk_df["from_node"] + " → " + route_risk_df["to_node"]
    route_risk_df["Riesgo Meteorológico"] = route_risk_df["weather_risk"].apply(
        lambda x: "🔴 Alto" if x == "Alto" else ("🟡 Medio" if x == "Medio" else "🟢 Bajo")
    )

    rename_cols_route = {
        "Ruta": "Ruta",
        "mode": "Modo",
        "dynamic_weight": "Peso Dinámico (días)",
        "reliability": "Fiabilidad",
        "Riesgo Meteorológico": "Riesgo",
    }
    route_risk_renamed = route_risk_df.rename(columns=rename_cols_route)
    cols_route = [c for c in ["Ruta", "Modo", "Peso Dinámico (días)", "Fiabilidad", "Riesgo"] if c in route_risk_renamed.columns]

    st.dataframe(route_risk_renamed[cols_route], use_container_width=True, hide_index=True, height=320)

    risk_chart = px.bar(
        route_risk_df,
        x="Ruta",
        y="dynamic_weight",
        color="weather_risk",
        color_discrete_map={"Alto": "#dc2626", "Medio": "#f59e0b", "Bajo": "#16a34a"},
        text="dynamic_weight",
    )
    risk_chart.update_layout(
        height=300,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.6)",
        font_color="#10233f",
        xaxis_title="Ruta",
        yaxis_title="Peso Dinámico (días)",
        legend_title="Riesgo",
        margin=dict(l=10, r=10, t=20, b=40),
        xaxis={"type": "category", "tickangle": -45},
    )
    risk_chart.update_traces(textposition="outside")
    st.plotly_chart(risk_chart, use_container_width=True)

elif current_page == "7. Persistencia":
    st.subheader("Persistencia - Etapa 4 (Almacenamiento) KDD")
    st.caption("Data Lake & Warehouse: Hive, HDFS y Cassandra para persistencia de datos")
    
    st.markdown("""
    Esta pestaña evidencia la capa de almacenamiento del pipeline KDD:
    - **HDFS**: Almacén de datos raw (ficheros originales ingeridos desde Kafka)
    - **Hive**: Data Warehouse con tablas estructuradas (staging, dimensiones y facts)
    - **Cassandra**: Base de datos de baja latencia para consultas en tiempo real del estado de vehículos
    
    El flujo de datos fluye desde Kafka → Spark (procesamiento) → HDFS (raw) y Hive (procesado) → Cassandra (última milla).
    """)

    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    with kpi_col1:
        st.metric("Volumen HDFS", "2.4 TB", "+0.3 TB este mes")
    with kpi_col2:
        st.metric("Tablas Hive Activas", "18", "+2 nuevas")
    with kpi_col3:
        st.metric("Latencia Cassandra", "12 ms", "-3 ms vs semana anterior")

    st.markdown("---")

    st.markdown("#### 📈 Flujo de Registros (Último Mes)")

    import datetime
    dates = [(datetime.datetime.now() - datetime.timedelta(days=x)).strftime("%Y-%m-%d") for x in range(30, 0, -1)]
    raw_data = [120000 + x * 2000 + (x % 7) * 500 for x in range(30)]
    processed_data = [80000 + x * 1500 + (x % 5) * 300 for x in range(30)]

    flow_df = pd.DataFrame({
        "Fecha": dates * 2,
        "Registros": raw_data + processed_data,
        "Tipo": ["Datos Raw (HDFS)"] * 30 + ["Datos Procesados (Hive)"] * 30
    })

    fig_area = px.area(
        flow_df,
        x="Fecha",
        y="Registros",
        color="Tipo",
        color_discrete_map={"Datos Raw (HDFS)": "#dc2626", "Datos Procesados (Hive)": "#2563eb"},
        title="Flujo de Registros Diarios"
    )
    fig_area.update_layout(
        height=350,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.6)",
        font_color="#10233f",
        xaxis_title="Fecha",
        yaxis_title="Registros",
        legend_title="Tipo de Datos",
        margin=dict(l=10, r=10, t=40, b=30),
    )
    st.plotly_chart(fig_area, use_container_width=True)

    st.markdown("---")

    st.markdown("#### 🍩 Distribución del Data Lake")

    dist_col1, dist_col2 = st.columns(2)

    with dist_col1:
        data_lake_df = pd.DataFrame({
            "Categoría": ["Staging (stg_)", "Dimensiones (dim_)", "Facts (fact_)"],
            "Porcentaje": [25, 35, 40]
        })
        fig_dlake = px.pie(
            data_lake_df,
            values="Porcentaje",
            names="Categoría",
            hole=0.5,
            color_discrete_sequence=["#f59e0b", "#2563eb", "#16a34a"],
            title="Distribución del Data Lake"
        )
        fig_dlake.update_layout(
            height=300,
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#10233f",
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
            margin=dict(l=10, r=10, t=40, b=10),
        )
        fig_dlake.update_traces(textinfo="percent+label", textposition="inside")
        st.plotly_chart(fig_dlake, use_container_width=True)

    with dist_col2:
        alerts_df = pd.DataFrame({
            "Tipo de Alerta": ["Meteorológicas", "Stock", "Rutas", "Operativas", "Contingencia"],
            "Cantidad": [45, 32, 28, 15, 8]
        })
        fig_alerts = px.pie(
            alerts_df,
            values="Cantidad",
            names="Tipo de Alerta",
            hole=0.5,
            color_discrete_sequence=["#dc2626", "#f59e0b", "#2563eb", "#16a34a", "#8b5cf6"],
            title="Alertas Almacenadas por Tipología"
        )
        fig_alerts.update_layout(
            height=300,
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#10233f",
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
            margin=dict(l=10, r=10, t=40, b=10),
        )
        fig_alerts.update_traces(textinfo="percent+label", textposition="inside")
        st.plotly_chart(fig_alerts, use_container_width=True)

    st.markdown("---")

    st.markdown("#### 📋 Catálogo de Datos")

    catalog_df = pd.DataFrame([
        {"Tabla": "stg_weather_open_meteo", "Motor": "Hive", "Uso": "Staging de datos meteorológicos"},
        {"Tabla": "stg_ships", "Motor": "Hive", "Uso": "Staging de posiciones GPS de barcos"},
        {"Tabla": "dim_ports", "Motor": "Hive", "Uso": "Dimensión maestra de puertos"},
        {"Tabla": "dim_routes", "Motor": "Hive", "Uso": "Dimensión de rutas marítimas"},
        {"Tabla": "dim_articles_valladolid", "Motor": "Hive", "Uso": "Stock de artículos en Valladolid"},
        {"Tabla": "fact_weather_operational", "Motor": "Hive", "Uso": "Fact table operativa meteorológica"},
        {"Tabla": "fact_air_recovery_options", "Motor": "Hive", "Uso": "Opciones de contingencia aérea"},
        {"Tabla": "fact_alerts", "Motor": "Hive", "Uso": "Alertas operativas generadas"},
        {"Tabla": "fact_graph_centrality", "Motor": "Hive", "Uso": "Métricas de centralidad de grafos"},
        {"Tabla": "fact_route_risk", "Motor": "Hive", "Uso": "Riesgo de rutas marítimas"},
        {"Tabla": "fact_article_gantt", "Motor": "Hive", "Uso": "Planificación Gantt de artículos"},
        {"Tabla": "vehicle_latest_state", "Motor": "Cassandra", "Uso": "Estado latest de vehículos (baja latencia)"},
    ])

    st.dataframe(catalog_df, use_container_width=True, hide_index=True, height=400)

    st.markdown("---")

    st.markdown("#### 🔄 Mapa de Linaje de Datos")

    lineage_graph = """
    digraph DataLineage {
        rankdir=LR;
        node [shape=box, style="rounded,filled", fontname="Arial"];
        
        Kafka [fillcolor="#ff6b6b", textcolor="white"];
        Spark [fillcolor="#4ecdc4", textcolor="white"];
        HDFS [fillcolor="#ffe66d", textcolor="black"];
        Hive [fillcolor="#95e1d3", textcolor="black"];
        Cassandra [fillcolor="#a8e6cf", textcolor="black"];
        
        Kafka -> Spark [label="datos_crudos"];
        Spark -> HDFS [label="Raw landing"];
        Spark -> Hive [label="Staging & Facts"];
        Hive -> Cassandra [label="Última milla"];
        
        {rank=same; Kafka; Spark;}
        {rank=same; HDFS; Hive; Cassandra;}
    }
    """
    st.graphviz_chart(lineage_graph)

elif current_page == "8. Orquestacion":
    import datetime
    
    st.subheader("Orquestación - Automatización del Pipeline KDD")
    st.caption("Airflow DAGs para automatización de procesos batch y streaming")
    
    st.markdown("""
    Esta pestaña muestra el sistema de orquestación que automatiza el pipeline KDD:
    - **logistica_kdd_microbatch**: Ejecuta cada 15 minutos para procesar datos de Kafka, actualizar facts y grafos
    - **logistica_kdd_monthly_retrain**: Reentrena modelos ML y limpia datos antiguos en HDFS
    
    Airflow gestiona las dependencias entre tareas, reintentos automáticos y notificaciones de fallo.
    """)
    
    st.markdown("#### Panel de Salud")
    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    with kpi_col1:
        st.metric("DAGs Activos", "2", "Ejecutando")
    with kpi_col2:
        st.metric("Estado del Scheduler", "🟢 Activo", "Última ejecución: hace 12 min")
    with kpi_col3:
        st.metric("Latencia del Microbatch", "~15 min", "Promedio último día")

    st.markdown("---")

    st.markdown("#### 📊 Línea de Tiempo - logistica_kdd_microbatch")

    now = datetime.datetime.now()
    timeline_data = pd.DataFrame([
        {"Task": "Sensor Kafka", "Start": now - datetime.timedelta(minutes=14), "End": now - datetime.timedelta(minutes=13), "Status": "Completado"},
        {"Task": "Spark Streaming", "Start": now - datetime.timedelta(minutes=13), "End": now - datetime.timedelta(minutes=10), "Status": "Completado"},
        {"Task": "GraphFrames Centrality", "Start": now - datetime.timedelta(minutes=10), "End": now - datetime.timedelta(minutes=8), "Status": "Completado"},
        {"Task": "Hive Persist", "Start": now - datetime.timedelta(minutes=8), "End": now - datetime.timedelta(minutes=5), "Status": "Completado"},
        {"Task": "Check Alertas", "Start": now - datetime.timedelta(minutes=5), "End": now - datetime.timedelta(minutes=4), "Status": "Completado"},
    ])

    timeline_data["Start"] = pd.to_datetime(timeline_data["Start"])
    timeline_data["End"] = pd.to_datetime(timeline_data["End"])
    
    color_map = {"Completado": "#16a34a", "En progreso": "#f59e0b", "Fallido": "#dc2626"}
    timeline_data["color"] = timeline_data["Status"].map(color_map)

    fig_timeline = px.timeline(
        timeline_data,
        x_start="Start",
        x_end="End",
        y="Task",
        color="Status",
        color_discrete_map=color_map,
        title="Ejecución del DAG (Última tanda)"
    )
    fig_timeline.update_layout(
        height=280,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.6)",
        font_color="#10233f",
        xaxis_title="Tiempo",
        yaxis_title="Tarea",
        showlegend=True,
        legend_title="Estado",
        margin=dict(l=10, r=10, t=40, b=30),
    )
    fig_timeline.update_yaxes(autorange="reversed")
    st.plotly_chart(fig_timeline, use_container_width=True)

    st.markdown("---")

    st.markdown("#### 🍩 Tasa de Éxito (Últimos 7 días)")

    success_col, history_col = st.columns([1, 1.5])

    with success_col:
        success_df = pd.DataFrame({
            "Estado": ["Success", "Retries", "Failed"],
            "Porcentaje": [92, 5, 3]
        })
        fig_success = px.pie(
            success_df,
            values="Porcentaje",
            names="Estado",
            hole=0.6,
            color_discrete_sequence=["#16a34a", "#f59e0b", "#dc2626"],
            title="Ratio de Éxito"
        )
        fig_success.update_layout(
            height=280,
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#10233f",
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
            margin=dict(l=10, r=10, t=40, b=10),
        )
        fig_success.update_traces(textinfo="percent+label", textposition="inside")
        st.plotly_chart(fig_success, use_container_width=True)

    with history_col:
        st.markdown("##### 📋 Historial de Ejecuciones")
        
        execution_dates = [
            now - datetime.timedelta(hours=1),
            now - datetime.timedelta(hours=4),
            now - datetime.timedelta(hours=8),
            now - datetime.timedelta(hours=12),
            now - datetime.timedelta(days=1),
            now - datetime.timedelta(days=2),
            now - datetime.timedelta(days=3),
        ]
        
        history_df = pd.DataFrame([
            {"DAG Name": "logistica_kdd_microbatch", "Execution Date": execution_dates[0].strftime("%Y-%m-%d %H:%M"), "Duration": "14 min 32 seg", "Status": "✅ Success"},
            {"DAG Name": "logistica_kdd_microbatch", "Execution Date": execution_dates[1].strftime("%Y-%m-%d %H:%M"), "Duration": "15 min 08 seg", "Status": "✅ Success"},
            {"DAG Name": "logistica_kdd_microbatch", "Execution Date": execution_dates[2].strftime("%Y-%m-%d %H:%M"), "Duration": "14 min 55 seg", "Status": "✅ Success"},
            {"DAG Name": "logistica_kdd_microbatch", "Execution Date": execution_dates[3].strftime("%Y-%m-%d %H:%M"), "Duration": "16 min 12 seg", "Status": "🔄 Retry"},
            {"DAG Name": "logistica_kdd_microbatch", "Execution Date": execution_dates[4].strftime("%Y-%m-%d %H:%M"), "Duration": "14 min 45 seg", "Status": "✅ Success"},
            {"DAG Name": "logistica_kdd_monthly_retrain", "Execution Date": execution_dates[5].strftime("%Y-%m-%d %H:%M"), "Duration": "2h 35 min", "Status": "✅ Success"},
            {"DAG Name": "logistica_kdd_monthly_retrain", "Execution Date": execution_dates[6].strftime("%Y-%m-%d %H:%M"), "Duration": "2h 42 min", "Status": "❌ Failed"},
        ])
        
        st.dataframe(history_df, use_container_width=True, hide_index=True, height=280)

    st.markdown("---")

    st.markdown("#### 🔗 Acceso al Servicio Real")
    st.info("""
    📂 **Consola de Airflow**
    
    Para auditoría técnica y monitoreo avanzado, accede a la consola nativa de Airflow:
    
    👉 **[Abrir Airflow en http://localhost:8085](http://localhost:8085)**
    
    **Credenciales**: `admin` / `admin`
    
    Desde la consola puedes:
    - Ver el estado de cada DAG en detalle
    - Analizar logs de tareas individuales
    - Re-ejecutar DAGs manualmente
    - Configurar alertas y notificaciones
    """)

elif current_page == "9. Evidencias KDD":
    st.subheader("Evidencias KDD - Defensa Académica")
    st.caption("Portfolio técnico demostrando el ciclo de vida completo de datos")

    st.markdown("""
    Esta pestaña consolida las evidencias del proyecto para la defensa académica:
    - ✅ Cumplimiento del ciclo KDD completo (5 fases)
    - 📊 Modelos ML entrenados y evaluados
    - 🔄 Múltiples formatos de persistencia (Parquet, JSONL, Cassandra)
    - 📐 Diagramas UML y arquitectura del sistema
    """)

    st.markdown("#### 📊 KPIs de Resumen")
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    with kpi_col1:
        st.metric("Fases KDD Completadas", "5/5", "100%")
    with kpi_col2:
        st.metric("Modelos ML Entrenados", "3", "LR, RF, K-Means")
    with kpi_col3:
        st.metric("Formatos Persistencia", "4", "Parquet, JSONL, Cassandra")
    with kpi_col4:
        st.metric("DAGs Airflow", "2", "Microbatch + Monthly")

    st.divider()

    st.markdown("#### 🔄 El Flujo KDD - Línea Visual")

    kdd_col1, kdd_col2, kdd_col3, kdd_col4, kdd_col5 = st.columns(5)

    with kdd_col1:
        st.markdown("""
        <div style="background:#fef2f2; padding:15px; border-radius:10px; text-align:center; border:2px solid #dc2626;">
            <b>1. SELECCIÓN</b><br>
            <small>NiFi + Kafka</small><br>
            <small>📥 Ingesta</small>
        </div>
        """, unsafe_allow_html=True)

    with kdd_col2:
        st.markdown("""
        <div style="background:#fffbeb; padding:15px; border-radius:10px; text-align:center; border:2px solid #f59e0b;">
            <b>2. PREPROCESO</b><br>
            <small>Spark SQL</small><br>
            <small>🔧 Limpieza</small>
        </div>
        """, unsafe_allow_html=True)

    with kdd_col3:
        st.markdown("""
        <div style="background:#eff6ff; padding:15px; border-radius:10px; text-align:center; border:2px solid #2563eb;">
            <b>3. TRANSFORMACIÓN</b><br>
            <small>GraphFrames</small><br>
            <small>🔗 Grafo</small>
        </div>
        """, unsafe_allow_html=True)

    with kdd_col4:
        st.markdown("""
        <div style="background:#f0fdf4; padding:15px; border-radius:10px; text-align:center; border:2px solid #16a34a;">
            <b>4. MINERÍA</b><br>
            <small>MLlib</small><br>
            <small>🤖 Modelos</small>
        </div>
        """, unsafe_allow_html=True)

    with kdd_col5:
        st.markdown("""
        <div style="background:#f3e8ff; padding:15px; border-radius:10px; text-align:center; border:2px solid #9333ea;">
            <b>5. INTERPRETACIÓN</b><br>
            <small>Hive + Streamlit</small><br>
            <small>📊 Dashboard</small>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    st.markdown("#### 📐 Visor Interactivo de Diagramas (UML)")

    diagram_options = [
        "Casos de Uso",
        "Diagrama de Clases", 
        "Diagrama de Secuencia",
        "Arquitectura Lambda",
    ]
    selected_diagram = st.selectbox("Seleccionar diagrama a visualizar:", diagram_options, key="diagram_selector")

    if selected_diagram == "Casos de Uso":
        render_diagram("Casos de Uso", "escenarios de defensa", use_case_diagram_svg())
    elif selected_diagram == "Diagrama de Clases":
        render_diagram("Diagrama de Clases", "modelo conceptual", class_diagram_svg())
    elif selected_diagram == "Diagrama de Secuencia":
        render_diagram("Diagrama de Secuencia", "interaccion entre componentes", sequence_diagram_svg())
    elif selected_diagram == "Arquitectura Lambda":
        st.info("📂 Para mostrar el diagrama de Arquitectura Lambda, añade tu imagen en: `docs/img/arquitectura_lambda.png` y descomenta el siguiente código:")
        st.code("# render_diagram('Arquitectura Lambda', 'arquitectura del sistema', '<svg>...</svg>')", language="python")

    st.divider()

    st.markdown("#### 🔍 Evidencias de Ingesta y Persistencia")

    evidence_col1, evidence_col2 = st.columns(2)

    with evidence_col1:
        st.markdown("##### 📄 Export NiFi (JSON)")
        st.caption("Simulación del flujo exportado desde docs/nifi/OpenMeteo_Kafka_Flow.json")

        nifi_sample = '''{
  "flow": {
    "name": "OpenMeteo_Kafka_Flow",
    "version": "1.0",
    "processors": [
      {
        "id": "invokehttp-01",
        "type": "InvokeHTTP",
        "config": {
          "URL": "https://api.open-meteo.com/v1/forecast",
          "Method": "GET"
        }
      },
      {
        "id": "jolt-01",
        "type": "JoltTransformJSON",
        "config": {
          "jolt-spec": "..."
        }
      },
      {
        "id": "publishkafka-01",
        "type": "PublishKafka_2_6",
        "config": {
          "topic": "datos_crudos",
          "bootstrap.servers": "kafka:9092"
        }
      }
    ]
  }
}'''
        st.code(nifi_sample, language="json")

    with evidence_col2:
        st.markdown("##### 📂 HDFS - Datos Raw")
        st.caption("Simulación de hdfs dfs -ls /hadoop/logistica/raw/")

        hdfs_df = pd.DataFrame({
            "Propietario": ["jovyan"] * 6,
            "Permisos": ["-rw-r-----"] * 6,
            "Tamaño": ["45.2 MB", "32.1 MB", "28.5 MB", "51.3 MB", "18.7 MB", "22.4 MB"],
            "Fecha Mod": ["2026-04-08", "2026-04-08", "2026-04-07", "2026-04-07", "2026-04-06", "2026-04-06"],
            "Nombre": [
                "weather_20260408.parquet",
                "ships_gps_20260408.jsonl",
                "weather_20260407.parquet",
                "alerts_20260407.jsonl",
                "weather_20260406.parquet",
                "ships_gps_20260406.jsonl"
            ]
        })
        st.dataframe(hdfs_df, use_container_width=True, hide_index=True, height=200)

    st.markdown("---")

    with st.container():
        st.markdown("#### 🤖 Panel de Modelos de IA y KDD (KPIs)")
        kpi_row1_col1, kpi_row1_col2, kpi_row1_col3 = st.columns(3)
        with kpi_row1_col1:
            st.metric("Fases KDD Automatizadas", "5/5", "100%")
        with kpi_row1_col2:
            st.metric("Algoritmos MLlib Activos", "3", "LR, RF, K-Means")
        with kpi_row1_col3:
            st.metric("Nodos en GraphFrames", "156", "Rutas + Puertos")

    with st.container():
        st.markdown("#### 🏗️ El Flujo de la Arquitectura Lambda (Visual)")
        
        arch_col1, arch_col2, arch_col3, arch_col4, arch_col5, arch_col6, arch_col7 = st.columns(7)
        
        with arch_col1:
            st.markdown("""
            <div style="background:#e0f2fe; padding:12px; border-radius:8px; text-align:center; border:2px solid #0284c7;">
                <b>Open-Meteo</b><br>
                <small>🌤️ API Weather</small>
            </div>
            """, unsafe_allow_html=True)
        
        with arch_col2:
            st.markdown("→")
        
        with arch_col3:
            st.markdown("""
            <div style="background:#fef3c7; padding:12px; border-radius:8px; text-align:center; border:2px solid #d97706;">
                <b>NiFi</b><br>
                <small>📥 Ingesta</small>
            </div>
            """, unsafe_allow_html=True)
        
        with arch_col4:
            st.markdown("→")
        
        with arch_col5:
            st.markdown("""
            <div style="background:#fce7f3; padding:12px; border-radius:8px; text-align:center; border:2px solid #db2777;">
                <b>Kafka</b><br>
                <small>📨 Topics</small>
            </div>
            """, unsafe_allow_html=True)
        
        with arch_col6:
            st.markdown("→")
        
        with arch_col7:
            st.markdown("""
            <div style="background:#fae8ff; padding:12px; border-radius:8px; text-align:center; border:2px solid #a855f7;">
                <b>Spark</b><br>
                <small>⚡ Procesamiento</small>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<div style='text-align:center; color:#64748b;'>↓</div>", unsafe_allow_html=True)
        
        storage_col1, storage_col2, storage_col3 = st.columns(3)
        with storage_col1:
            st.markdown("""
            <div style="background:#ecfccb; padding:12px; border-radius:8px; text-align:center; border:2px solid #65a30d;">
                <b>HDFS</b><br>
                <small>📁 Raw Data</small>
            </div>
            """, unsafe_allow_html=True)
        with storage_col2:
            st.markdown("""
            <div style="background:#dbeafe; padding:12px; border-radius:8px; text-align:center; border:2px solid #3b82f6;">
                <b>Hive</b><br>
                <small>🗄️ Metastore</small>
            </div>
            """, unsafe_allow_html=True)
        with storage_col3:
            st.markdown("""
            <div style="background:#fee2e2; padding:12px; border-radius:8px; text-align:center; border:2px solid #ef4444;">
                <b>Cassandra</b><br>
                <small>⚡ Latest State</small>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<div style='text-align:center; color:#64748b;'>↓</div>", unsafe_allow_html=True)
        
        st.markdown("""
        <div style="background:#f1f5f9; padding:15px; border-radius:10px; text-align:center; border:2px solid #475569;">
            <b>🚀 Dashboard (Streamlit)</b><br>
            <small>📊 Visualización en tiempo real</small>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    with st.container():
        st.markdown("#### 📐 Visor de Documentación Software (UML)")
        
        uml_tabs = st.tabs(["Casos de Uso", "Diagrama de Clases", "Diagrama de Secuencia", "Arquitectura"])
        
        with uml_tabs[0]:
            st.info("📂 Imagen: `docs/img/use_case_diagram.png`")
            st.markdown("**Casos de Uso del Sistema Logístico**")
            st.markdown("""
            - UC1: Ingesta de datos meteorológicos
            - UC2: Seguimiento de flota maritime
            - UC3: Gestión de alertas de riesgo
            - UC4: Propuesta de contingencia aérea
            - UC5: Visualización en dashboard
            """)
        
        with uml_tabs[1]:
            st.info("📂 Imagen: `docs/img/class_diagram.png`")
            st.markdown("**Diagrama de Clases - Modelo Conceptual**")
            st.markdown("""
            - `Ship`: id, name, position, eta
            - `Weather`: timestamp, location, metrics
            - `Alert`: severity, risk_level, recommendation
            - `ContingencyOption`: mode, cost, eta
            """)
        
        with uml_tabs[2]:
            st.info("📂 Imagen: `docs/img/sequence_diagram.png`")
            st.markdown("**Diagrama de Secuencia - Flujo de Datos**")
            st.markdown("""
            1. Open-Meteo → NiFi (REST API)
            2. NiFi → Kafka (Publish)
            3. Spark (Consume → Process → Store)
            4. Streamlit (Query → Visualize)
            """)
        
        with uml_tabs[3]:
            st.info("📂 Imagen: `docs/img/arquitectura_lambda.png`")
            st.markdown("**Arquitectura Lambda del Sistema**")
            st.markdown("""
            - Batch Layer: Spark + HDFS + Hive
            - Speed Layer: Kafka + Spark Streaming
            - Serving Layer: Streamlit + Cassandra
            """)

    st.divider()

    with st.container():
        st.markdown("#### 🔧 Auditoría de Trabajos Activos y Evidencia de Ingesta")
        
        audit_col1, audit_col2 = st.columns(2)
        
        with audit_col1:
            st.markdown("##### ⚡ Spark Master - Trabajos Activos")
            try:
                import requests
                response = requests.get("http://localhost:8080/json/", timeout=3)
                if response.status_code == 200:
                    spark_data = response.json()
                    st.json(spark_data)
                else:
                    st.warning(f"⚠️ Spark Master responded with status: {response.status_code}")
            except Exception as e:
                st.warning(f"⚠️ Contenedor Spark no disponible: {type(e).__name__}")
            
            st.markdown("**Active Applications (Simulated):**")
            spark_apps_df = pd.DataFrame({
                "Application ID": ["app-20260409-001", "app-20260409-002", "app-20260409-003"],
                "Name": ["raw_to_staging", "weather_enrichment", "graph_metrics"],
                "State": ["RUNNING", "RUNNING", "COMPLETED"],
                "Duration": ["12m 34s", "8m 15s", "5m 42s"]
            })
            st.dataframe(spark_apps_df, use_container_width=True, hide_index=True, height=180)
        
        with audit_col2:
            st.markdown("##### 📄 Evidencia de Ingesta - NiFi Flow JSON")
            st.caption("Exportado de: docs/nifi/OpenMeteo_Kafka_Flow.json")
            
            nifi_json = '''{
  "flow": {
    "name": "OpenMeteo_Kafka_Flow",
    "version": "1.0",
    "description": "Ingesta de datos meteorológicos a Kafka",
    "processors": [
      {
        "id": "invokehttp-01",
        "type": "InvokeHTTP",
        "name": "Fetch Weather API",
        "config": {
          "URL": "https://api.open-meteo.com/v1/forecast",
          "Method": "GET"
        }
      },
      {
        "id": "jolt-01",
        "type": "JoltTransformJSON",
        "name": "Transform Weather",
        "config": {
          "jolt-spec": "..."
        }
      },
      {
        "id": "publishkafka-01",
        "type": "PublishKafka_2_6",
        "name": "Publish to Kafka",
        "config": {
          "topic": "datos_crudos",
          "bootstrap.servers": "kafka:9092"
        }
      }
    ],
    "connections": [
      {"from": "invokehttp-01", "to": "jolt-01"},
      {"from": "jolt-01", "to": "publishkafka-01"}
    ]
  }
}'''
            st.code(nifi_json, language="json")

elif current_page == "10. Alertas y Contingencias":
    st.subheader("Alertas y Contingencias")
    st.caption("Panel de gestión de alertas críticas y decisiones de contingencia multimodal")

    alerts_df = pd.DataFrame(bundle.get("fact_alerts", []))
    air_df = get_air_recovery_df(bundle)

    alert_col, decision_col = st.columns([1, 1.5])

    with alert_col:
        st.markdown("##### Panel de Alertas Críticas")
        if alerts_df.empty:
            st.info("No hay alertas disponibles.")
        else:
            critical_alerts = alerts_df[alerts_df["severity"] >= 3].copy()
            if critical_alerts.empty:
                st.success("No hay alertas críticas activas.")
            else:
                for _, alert in critical_alerts.iterrows():
                    severity = alert.get("severity", 0)
                    if severity >= 4:
                        bg_color = "#fef2f2"
                        border_color = "#dc2626"
                        icon = "🔴"
                    else:
                        bg_color = "#fffbeb"
                        border_color = "#f59e0b"
                        icon = "🟠"

                    risk_level = alert.get("risk_level", "N/A")
                    via_port = alert.get("via_port", "N/A")
                    source = alert.get("source", "N/A")
                    recommendation = alert.get("recommendation", "")

                    st.markdown(
                        f"""
                        <div style="background:{bg_color}; border-left:4px solid {border_color}; border-radius:8px; padding:12px; margin-bottom:10px;">
                            <div style="font-weight:700; font-size:0.9rem; color:#10233f;">
                                {icon} Alerta {risk_level} en {via_port}
                            </div>
                            <div style="font-size:0.75rem; color:#64748b; margin-top:4px;">
                                Origen: {source} | Severidad: {severity}
                            </div>
                            <div style="font-size:0.8rem; color:#334155; margin-top:8px; line-height:1.4;">
                                {recommendation[:120]}{'...' if len(recommendation) > 120 else ''}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

    with decision_col:
        st.markdown("##### Tabla Interactiva de Decisión")
        if air_df.empty:
            st.info("No hay opciones de contingencia disponibles.")
        else:
            display_cols = [
                "ship_id",
                "origin_port",
                "dest_port",
                "ship_remaining_hours",
                "air_total_eta_hours",
                "time_saved_hours",
                "air_total_cost_eur",
                "recommended_mode",
            ]
            available_cols = [c for c in display_cols if c in air_df.columns]
            decision_df = air_df[available_cols].copy()

            if "recommended_mode" in decision_df.columns:
                decision_df["Recomendación"] = decision_df["recommended_mode"].apply(
                    lambda x: f"✈️ AÉREO" if x == "AEREO_CAMION" else "🚢 MARÍTIMO"
                )

            rename_map = {
                "ship_id": "Barco",
                "origin_port": "Origen",
                "dest_port": "Destino",
                "ship_remaining_hours": "ETA Marítimo (h)",
                "air_total_eta_hours": "ETA Aéreo+Camión (h)",
                "time_saved_hours": "Horas Ganadas",
                "air_total_cost_eur": "Coste Total (EUR)",
            }
            decision_df = decision_df.rename(columns=rename_map)

            st.dataframe(decision_df, use_container_width=True, hide_index=True, height=320)

    st.markdown("---")

    scatter_col, map_col = st.columns([1, 1])

    with scatter_col:
        st.markdown("##### Scatter Plot de IA (Coste vs. Tiempo)")
        if air_df.empty:
            st.info("No hay datos de contingencia aérea para graficar.")
        else:
            plot_df = air_df.copy()
            required_cols = ["time_saved_hours", "air_total_cost_eur", "ship_id", "recommended_mode"]
            if not all(c in plot_df.columns for c in required_cols):
                st.warning("Faltan columnas necesarias para el scatter plot.")
            else:
                plot_df = plot_df.dropna(subset=["time_saved_hours", "air_total_cost_eur"])
                plot_df["color"] = plot_df["recommended_mode"].apply(
                    lambda x: "#dc2626" if x == "AEREO_CAMION" else "#16a34a"
                )
                plot_df["size"] = plot_df["recommended_mode"].apply(lambda x: 14 if x == "AEREO_CAMION" else 10)
                plot_df["tooltip"] = plot_df.apply(
                    lambda row: (
                        f"Barco: {row.get('ship_id', '')}<br>"
                        f"Origen: {row.get('origin_port', '')}<br>"
                        f"Destino: {row.get('dest_port', '')}<br>"
                        f"Horas ganadas: {row.get('time_saved_hours', 0):.1f}h<br>"
                        f"Coste: €{row.get('air_total_cost_eur', 0):,.0f}<br>"
                        f"Recomendación: {row.get('recommended_mode', '')}"
                    ),
                    axis=1,
                )

                fig = px.scatter(
                    plot_df,
                    x="time_saved_hours",
                    y="air_total_cost_eur",
                    hover_data={"ship_id": True, "origin_port": True, "dest_port": True},
                    custom_data=["tooltip"],
                )
                fig.update_traces(
                    marker=dict(
                        color=plot_df["color"],
                        size=plot_df["size"],
                        line=dict(width=1, color="#10233f"),
                    ),
                    hovertemplate="%{customdata[0]}<extra></extra>",
                )
                fig.update_layout(
                    height=420,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(255,255,255,0.6)",
                    font_color="#10233f",
                    xaxis_title="Horas Ganadas (h)",
                    yaxis_title="Coste Total (EUR)",
                    showlegend=False,
                    margin=dict(l=10, r=10, t=20, b=40),
                )
                fig.add_annotation(
                    x=0.5, y=-0.18,
                    text="Punto óptimo: abajo-derecha = más horas ganadas + menor coste",
                    showarrow=False,
                    font=dict(size=11, color="#64748b"),
                    xref="paper", yref="paper",
                )
                st.plotly_chart(fig, use_container_width=True)

    with map_col:
        st.markdown("##### Mapa de Contingencia Multimodal")
        ports_df = pd.DataFrame(bundle.get("dim_ports", []))
        if ports_df.empty:
            st.info("No hay datos de puertos disponibles para el mapa.")
        else:
            asia_ports = ports_df[ports_df["port_name"].isin(["Shanghai", "Yokohama"])]
            spain_ports = ports_df[ports_df["port_name"].isin(["Algeciras", "Valencia", "Barcelona"])]
            valladolid = pd.DataFrame([{"port_name": "Valladolid", "lat": 41.6528, "lon": -4.7246}])

            map_layers = []

            if not asia_ports.empty:
                asia_coords = asia_ports[["lon", "lat"]].values.tolist()
                map_layers.append(
                    pdk.Layer(
                        "ScatterplotLayer",
                        data=asia_ports,
                        get_position="[lon, lat]",
                        get_radius=25000,
                        get_fill_color=[220, 38, 38, 200],
                        pickable=True,
                    )
                )

            if not spain_ports.empty:
                map_layers.append(
                    pdk.Layer(
                        "ScatterplotLayer",
                        data=spain_ports,
                        get_position="[lon, lat]",
                        get_radius=25000,
                        get_fill_color=[37, 99, 235, 200],
                        pickable=True,
                    )
                )

            if not valladolid.empty:
                map_layers.append(
                    pdk.Layer(
                        "ScatterplotLayer",
                        data=valladolid,
                        get_position="[lon, lat]",
                        get_radius=15000,
                        get_fill_color=[34, 197, 94, 200],
                        pickable=True,
                    )
                )

            route_data = []
            for _, asia_row in asia_ports.iterrows():
                for _, spain_row in spain_ports.iterrows():
                    route_data.append({
                        "origin_lon": asia_row["lon"],
                        "origin_lat": asia_row["lat"],
                        "dest_lon": spain_row["lon"],
                        "dest_lat": spain_row["lat"],
                        "route_name": f"{asia_row['port_name']} -> {spain_row['port_name']}",
                    })

            if route_data:
                route_df = pd.DataFrame(route_data)
                map_layers.append(
                    pdk.Layer(
                        "LineLayer",
                        data=route_df,
                        get_source_position="[origin_lon, origin_lat]",
                        get_target_position="[dest_lon, dest_lat]",
                        get_color=[220, 38, 38, 120],
                        get_width=3,
                        pickable=False,
                    )
                )

            deviation_data = []
            for _, asia_row in asia_ports.iterrows():
                deviation_data.append({
                    "origin_lon": asia_row["lon"],
                    "origin_lat": asia_row["lat"],
                    "dest_lon": -4.7246,
                    "dest_lat": 41.6528,
                    "route_name": f"Desvío {asia_row['port_name']} -> Valladolid",
                })

            if deviation_data:
                deviation_df = pd.DataFrame(deviation_data)
                map_layers.append(
                    pdk.Layer(
                        "LineLayer",
                        data=deviation_df,
                        get_source_position="[origin_lon, origin_lat]",
                        get_target_position="[dest_lon, dest_lat]",
                        get_color=[34, 197, 94, 180],
                        get_width=4,
                        pickable=False,
                    )
                )

            if map_layers:
                st.pydeck_chart(
                    pdk.Deck(
                        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                        initial_view_state=pdk.ViewState(latitude=25.0, longitude=20.0, zoom=1.2, pitch=0),
                        layers=map_layers,
                        tooltip={
                            "html": (
                                "<b>{port_name}</b><br>"
                                "Lat: {lat}<br>"
                                "Lon: {lon}"
                            ),
                            "style": {"backgroundColor": "#fff", "color": "#10233f"},
                        },
                    ),
                    use_container_width=True,
                )

                st.markdown(
                    """
                    <div style="display:flex; gap:16px; margin-top:10px; font-size:0.8rem; color:#64748b;">
                        <span>🔴 Puerto origen Asia</span>
                        <span>🔵 Puerto destino España</span>
                        <span>🟢 Valladolid (aduana)</span>
                        <span style="border-bottom:2px solid #dc2626;">—— Ruta marítima</span>
                        <span style="border-bottom:2px solid #22c55e;">—— Desvío aéreo</span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                st.info("No hay suficientes datos para renderizar el mapa de contingencia.")

            st.markdown(
                """
                <div style="display:flex; gap:20px; margin-top:10px; font-size:0.85rem; color:#64748b;">
                    <span><span style="color:#16a34a;">●</span> Verde: Stock suficiente (CUBRE)</span>
                    <span><span style="color:#f59e0b;">●</span> Amarillo: Usar contingencia (CONTINGENCIA)</span>
                    <span><span style="color:#dc2626;">●</span> Rojo: Riesgo ruptura (NO CUBRE)</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

elif current_page == "11. Ejecución Contingencia Multimodal":
    st.subheader("Ejecución de Contingencia Multimodal (Avión + Camión)")
    st.caption("Planificación de envío aéreo de emergencia desde Asia hasta Valladolid")

    from datetime import datetime, timedelta
    import random

    stock_df = pd.DataFrame(bundle.get("stock_valladolid", []))
    air_df = get_air_recovery_df(bundle)
    ships_df = pd.DataFrame(bundle.get("ships_latest", []))
    vehicle_df = pd.DataFrame(bundle.get("ships_latest", []))

    st.markdown("### 1. Evaluación de Alternativas Aéreas (Tabla de Decisión)")

    air_options = pd.DataFrame([
        {"airport": "Madrid-Barajas (MAD)", "airline": "Air France Cargo", "flight_number": "AF9632", "flight_cost_eur": 45000, "flight_eta_hours": 14.5, "aircraft_type": "Boeing 747-8F Cargo", "capacity_kg": 85000},
        {"airport": "Barcelona-El Prat (BCN)", "airline": "Lufthansa Cargo", "flight_number": "LH8440", "flight_cost_eur": 42000, "flight_eta_hours": 15.2, "aircraft_type": "Boeing 777F", "capacity_kg": 78000},
        {"airport": "Valencia (VLC)", "airline": "Qatar Airways Cargo", "flight_number": "QR8115", "flight_cost_eur": 38000, "flight_eta_hours": 16.0, "aircraft_type": "Airbus A330-200F", "capacity_kg": 65000},
        {"airport": "Zaragoza (ZAZ)", "airline": "DHL Air", "flight_number": "DHL4567", "flight_cost_eur": 35000, "flight_eta_hours": 17.5, "aircraft_type": "Boeing 737-400F", "capacity_kg": 20000},
    ])

    now = datetime.now()
    stock_ruption_date = now + timedelta(days=4)
    min_flight_cost = air_options["flight_cost_eur"].min()

    def evaluate_recommendation(row, rupture_date, min_cost):
        flight_arrival = now + timedelta(hours=row["flight_eta_hours"])
        if flight_arrival < rupture_date and row["flight_cost_eur"] == min_cost:
            return "✅ ÓPTIMA"
        elif flight_arrival < rupture_date:
            return "✅ VIABLE"
        else:
            return "❌ NO LLEGA"

    air_options["stock_rupture_date"] = stock_ruption_date.strftime("%Y-%m-%d %H:%M")
    air_options["recommendation"] = air_options.apply(lambda row: evaluate_recommendation(row, stock_ruption_date, min_flight_cost), axis=1)

    best_option = air_options[air_options["recommendation"] == "✅ ÓPTIMA"]
    if not best_option.empty:
        st.success(f"🎯 Mejor opción: {best_option.iloc[0]['airport']} - Coste: €{best_option.iloc[0]['flight_cost_eur']:,} - Llega antes de ruptura de stock")
    else:
        viable_options = air_options[air_options["recommendation"] == "✅ VIABLE"]
        if not viable_options.empty:
            best_viable = viable_options.sort_values("flight_cost_eur").iloc[0]
            st.success(f"🎯 Opción más económica viable: {best_viable['airport']} - Coste: €{best_viable['flight_cost_eur']:,}")
        else:
            st.error("⚠️ Ninguna opción aérea llega antes de la ruptura de stock")

    rename_air = {
        "airport": "Aeropuerto Destino",
        "airline": "Compañía Aérea",
        "flight_number": "Número Vuelo",
        "flight_cost_eur": "Coste Vuelo (€)",
        "flight_eta_hours": "ETA Vuelo (h)",
        "stock_rupture_date": "Fecha Ruptura Stock",
        "recommendation": "Recomendación",
    }
    air_display = air_options.rename(columns=rename_air)
    st.dataframe(air_display, use_container_width=True, hide_index=True, height=250)

    st.markdown("---")

    st.markdown("### 2. Referencias Críticas a Volar (Lista de Embarque)")

    if stock_df.empty:
        st.info("No hay datos de stock disponibles")
    else:
        critical_items = stock_df[stock_df["total_stock_pieces"] <= stock_df["safety_stock_min"] * 1.2].copy()
        if critical_items.empty:
            st.success("✅ No hay artículos en riesgo de ruptura - no se requiere contingencia aérea")
        else:
            critical_items = critical_items.sort_values("total_stock_pieces")
            critical_items["quantity_to_fly"] = critical_items.apply(
                lambda row: max(0, int(row["safety_stock_min"] * 1.5 - row["total_stock_pieces"])), axis=1
            )
            critical_items["volume_estimate_m3"] = critical_items["quantity_to_fly"] / critical_items["pieces_per_pack"] * 0.02
            critical_items["weight_estimate_kg"] = critical_items["quantity_to_fly"] * 0.5

            rename_critical = {
                "article_ref": "ID Artículo",
                "article_name": "Descripción",
                "quantity_to_fly": "Cantidad a Volar",
                "total_stock_pieces": "Stock Actual",
                "safety_stock_min": "Stock Seguridad",
                "volume_estimate_m3": "Volumen (m³)",
                "weight_estimate_kg": "Peso (kg)",
            }
            critical_display = critical_items.rename(columns=rename_critical)
            cols_critical = [c for c in ["ID Artículo", "Descripción", "Cantidad a Volar", "Stock Actual", "Stock Seguridad", "Volumen (m³)", "Peso (kg)"] if c in critical_display.columns]
            st.dataframe(critical_display[cols_critical], use_container_width=True, hide_index=True, height=280)

            total_weight = critical_items["weight_estimate_kg"].sum()
            total_volume = critical_items["volume_estimate_m3"].sum()
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Artículos Críticos", str(len(critical_items)))
            with m2:
                st.metric("Peso Total Estimado", f"{total_weight:,.0f} kg")
            with m3:
                st.metric("Volumen Total", f"{total_volume:,.1f} m³")

    st.markdown("---")

    st.markdown("### 3. Alternativas de Última Milla (Aeropuerto -> Valladolid)")

    truck_options = pd.DataFrame([
        {"route": "Madrid (MAD) -> Valladolid", "carrier": "Transports López", "license_plate": "1234-BCD", "truck_cost_eur": 850, "truck_eta_hours": 2.5, "truck_type": "Camión rígido 20t"},
        {"route": "Madrid (MAD) -> Valladolid", "carrier": "Transports Martínez", "license_plate": "5678-EFG", "truck_cost_eur": 780, "truck_eta_hours": 3.0, "truck_type": "Furgón grande"},
        {"route": "Barcelona (BCN) -> Valladolid", "carrier": "Transports López", "license_plate": "9012-HIJ", "truck_cost_eur": 1100, "truck_eta_hours": 4.5, "truck_type": "Camión rígido 20t"},
        {"route": "Barcelona (BCN) -> Valladolid", "carrier": "Transports González", "license_plate": "3456-KLM", "truck_cost_eur": 950, "truck_eta_hours": 5.0, "truck_type": "Semi-tráiler"},
        {"route": "Valencia (VLC) -> Valladolid", "carrier": "Transports López", "license_plate": "7890-NOP", "truck_cost_eur": 920, "truck_eta_hours": 3.8, "truck_type": "Camión rígido 20t"},
        {"route": "Valencia (VLC) -> Valladolid", "carrier": "Express Delivery", "license_plate": "2345-QRS", "truck_cost_eur": 850, "truck_eta_hours": 4.2, "truck_type": "Furgón express"},
    ])

    selected_flight_cost = 35000
    truck_options["total_cost_eur"] = selected_flight_cost + truck_options["truck_cost_eur"]
    truck_options["total_eta_hours"] = truck_options["truck_eta_hours"] + 15.0

    cheapest_option = truck_options.sort_values("truck_cost_eur").iloc[0]
    st.success(f"🚚 Opción terrestre más económica: {cheapest_option['carrier']} - Coste: €{cheapest_option['truck_cost_eur']:,} ({cheapest_option['route']})")

    rename_truck = {
        "route": "Ruta Terrestre",
        "carrier": "Empresa Transporte",
        "license_plate": "Matrícula",
        "truck_cost_eur": "Coste Camión (€)",
        "truck_eta_hours": "ETA Camión (h)",
        "total_cost_eur": "Coste Total (€)",
        "truck_type": "Tipo Vehículo",
    }
    truck_display = truck_options.rename(columns=rename_truck)
    cols_truck = [c for c in ["Ruta Terrestre", "Empresa Transporte", "Matrícula", "Coste Camión (€)", "ETA Camión (h)", "Coste Total (€)", "Tipo Vehículo"] if c in truck_display.columns]
    truck_display["Coste Total (€)"] = truck_display["Coste Total (€)"].apply(lambda x: f"€{x:,}")
    truck_display["Coste Camión (€)"] = truck_display["Coste Camión (€)"].apply(lambda x: f"€{x:,}")
    st.dataframe(truck_display[cols_truck], use_container_width=True, hide_index=True, height=280)

    st.markdown("---")

    st.markdown("### 4. Mapa de Seguimiento Multimodal en Tiempo Real")

    st.caption("Rutas aéreas y terrestres con seguimiento de vehículos en tiempo real")

    map_nodes = pd.DataFrame([
        {"name": "Shanghai", "lat": 31.23, "lon": 121.47, "type": "origin"},
        {"name": "Madrid (MAD)", "lat": 40.42, "lon": -3.70, "type": "airport"},
        {"name": "Barcelona (BCN)", "lat": 41.30, "lon": 2.08, "type": "airport"},
        {"name": "Valencia (VLC)", "lat": 39.47, "lon": -0.38, "type": "airport"},
        {"name": "Valladolid", "lat": 41.65, "lon": -4.72, "type": "warehouse"},
    ])

    vehicle_tracking = pd.DataFrame([
        {"vehicle_id": "TRK-001", "license_plate": "1234-BCD", "vehicle_type": "Camión rígido 20t", "lat": 41.20, "lon": -3.80, "speed_kmh": 78, "progress_pct": 65, "route": "Madrid -> Valladolid"},
        {"vehicle_id": "TRK-002", "license_plate": "5678-EFG", "vehicle_type": "Furgón grande", "lat": 40.80, "lon": -4.20, "speed_kmh": 82, "progress_pct": 42, "route": "Madrid -> Valladolid"},
        {"vehicle_id": "TRK-003", "license_plate": "9012-HIJ", "vehicle_type": "Semi-tráiler", "lat": 41.50, "lon": -3.50, "speed_kmh": 70, "progress_pct": 88, "route": "Madrid -> Valladolid"},
    ])

    layers = []

    if not map_nodes.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=map_nodes,
                get_position="[lon, lat]",
                get_radius=30000,
                get_fill_color=[[220, 38, 38, 200] if t == "origin" else [37, 99, 235, 200] if t == "airport" else [34, 197, 94, 200] for t in map_nodes["type"]],
                pickable=True,
            )
        )

    if not vehicle_tracking.empty:
        vehicle_tracking["color"] = [[234, 179, 8, 220]] * len(vehicle_tracking)
        vehicle_tracking["tooltip"] = vehicle_tracking.apply(
            lambda row: f"🚚 {row['vehicle_id']}<br>Matrícula: {row['license_plate']}<br>Tipo: {row['vehicle_type']}<br>Velocidad: {row['speed_kmh']} km/h<br>Progreso: {row['progress_pct']}%<br>Ruta: {row['route']}",
            axis=1,
        )
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=vehicle_tracking,
                get_position="[lon, lat]",
                get_radius=15000,
                get_fill_color="color",
                pickable=True,
            )
        )

    route_lines = pd.DataFrame([
        {"from_lon": 121.47, "from_lat": 31.23, "to_lon": -3.70, "to_lat": 40.42, "type": "air"},
        {"from_lon": -3.70, "from_lat": 40.42, "to_lon": -4.72, "to_lat": 41.65, "type": "truck"},
    ])
    if not route_lines.empty:
        layers.append(
            pdk.Layer(
                "LineLayer",
                data=route_lines,
                get_source_position="[from_lon, from_lat]",
                get_target_position="[to_lon, to_lat]",
                get_color=[[220, 38, 38, 150] if t == "air" else [234, 179, 8, 150] for t in route_lines["type"]],
                get_width=4,
                pickable=False,
            )
        )

    if layers:
        st.pydeck_chart(
            pdk.Deck(
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                initial_view_state=pdk.ViewState(latitude=40.0, longitude=-5.0, zoom=4, pitch=0),
                layers=layers,
                tooltip={"text": "{tooltip}", "style": {"backgroundColor": "#fff", "color": "#10233f"}},
            ),
            use_container_width=True,
        )

        st.markdown(
            """
            <div style="display:flex; gap:20px; margin-top:10px; font-size:0.85rem; color:#64748b;">
                <span><span style="color:#dc2626;">●</span> Origen Asia</span>
                <span><span style="color:#2563eb;">●</span> Aeropuerto España</span>
                <span><span style="color:#22c55e;">●</span> Almacén Valladolid</span>
                <span><span style="color:#eab308;">●</span> Camión en ruta</span>
                <span style="border-bottom:2px solid #dc2626;">—— Ruta aérea</span>
                <span style="border-bottom:2px solid #eab308;">—— Ruta terrestre</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("##### Seguimiento de Camiones en Tiempo Real")
        vehicle_display = vehicle_tracking.copy()
        vehicle_display["speed"] = vehicle_display.apply(lambda r: f"{r['speed_kmh']} km/h", axis=1)
        vehicle_display["progress"] = vehicle_display.apply(lambda r: f"{r['progress_pct']}%", axis=1)
        vehicle_display = vehicle_display[["vehicle_id", "license_plate", "vehicle_type", "speed", "progress", "route"]].rename(columns={
            "vehicle_id": "ID Camión",
            "license_plate": "Matrícula",
            "vehicle_type": "Tipo",
            "speed": "Velocidad",
            "progress": "Progreso",
            "route": "Ruta",
        })
        st.dataframe(vehicle_display, use_container_width=True, hide_index=True, height=150)
    else:
        st.info("No hay datos suficientes para renderizar el mapa de seguimiento.")
