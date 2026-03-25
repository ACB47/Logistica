from __future__ import annotations

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


SMTP_HOST = "smtp-mail.outlook.com"
SMTP_PORT = 587
SMTP_USER = "logistica-alerts@outlook.com"
SMTP_PASSWORD = "CHANGE_ME_TO_REAL_PASSWORD"
ALERT_RECIPIENTS = ["operaciones@empresa.com", "logistica@empresa.com"]


def send_alert_email(
    subject: str,
    body_html: str,
    recipients: list[str] = None
) -> bool:
    if recipients is None:
        recipients = ALERT_RECIPIENTS

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = SMTP_USER
        msg["To"] = ", ".join(recipients)

        part = MIMEText(body_html, "html")
        msg.attach(part)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, recipients, msg.as_string())

        print(f"[EMAIL] Alert sent: {subject}")
        return True
    except Exception as e:
        print(f"[EMAIL] Failed to send alert: {e}")
        return False


def format_alert_html(alerts_df) -> str:
    html = """
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; }
            .alert { border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px; }
            .critical { background-color: #ffcccc; border-color: #ff0000; }
            .high { background-color: #ffe6cc; border-color: #ff6600; }
            .medium { background-color: #fff3cc; border-color: #ffcc00; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #4CAF50; color: white; }
        </style>
    </head>
    <body>
        <h2>Logistica Maritima - Alerta Operativa</h2>
        <p>Generado: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC") + """</p>
    """
    return html


def send_route_alert(
    dest_port: str,
    risk_level: str,
    risk_score: float,
    recommendation: str,
    severity: int
) -> bool:
    if severity < 3:
        return False

    severity_label = {3: "MEDIO", 4: "ALTO", 5: "CRITICO"}.get(severity, "DESCONOCIDO")

    subject = f"[ALERTA {severity_label}] Riesgo en ruta {dest_port}"

    body = f"""
    <html>
    <body>
        <h2 style="color: {'red' if severity >= 4 else 'orange'};">ALERTA OPERATIVA - LOGISTICA</h2>
        <div class="alert {'critical' if severity >= 4 else 'high'}">
            <h3>Ruta: Shanghai → {dest_port}</h3>
            <table>
                <tr><td><strong>Nivel de Riesgo:</strong></td><td>{risk_level}</td></tr>
                <tr><td><strong>Score:</strong></td><td>{risk_score:.2f}</td></tr>
                <tr><td><strong>Severidad:</strong></td><td>{severity}/5</td></tr>
                <tr><td><strong>Recomendación:</strong></td><td>{recommendation}</td></tr>
            </table>
        </div>
        <p>Por favor tome las acciones necesarias segun el plan de contingencia.</p>
        <hr>
        <p><small>Sistema de Monitorizacion Logistica - Big Data Project</small></p>
    </body>
    </html>
    """

    return send_alert_email(subject, body)


def send_daily_summary(
    total_ships_tracked: int,
    total_alerts: int,
    critical_routes: list,
    avg_risk_score: float
) -> bool:
    critical_list_html = "".join(
        f"<li>{route}</li>" for route in critical_routes
    ) if critical_routes else "<li>Ninguna</li>"

    body = f"""
    <html>
    <body>
        <h2>Resumen Diario de Operaciones</h2>
        <p>Fecha: {datetime.now().strftime('%Y-%m-%d')}</p>
        <table>
            <tr><td><strong>Barcos rastreados:</strong></td><td>{total_ships_tracked}</td></tr>
            <tr><td><strong>Total alertas:</strong></td><td>{total_alerts}</td></tr>
            <tr><td><strong>Riesgo promedio:</strong></td><td>{avg_risk_score:.2f}</td></tr>
        </table>
        <h3>Rutas criticas:</h3>
        <ul>{critical_list_html}</ul>
        <hr>
        <p><small>Generado automaticamente por el sistema</small></p>
    </body>
    </html>
    """

    return send_alert_email(
        f"Resumen Diario - {datetime.now().strftime('%Y-%m-%d')}",
        body,
        recipients=["gerencia@empresa.com"]
    )


if __name__ == "__main__":
    send_route_alert(
        dest_port="Algeciras",
        risk_level="ALTO",
        risk_score=4.2,
        recommendation="Activar vuelo urgente",
        severity=5
    )
