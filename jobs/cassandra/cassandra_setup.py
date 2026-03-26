#!/usr/bin/env python3
from __future__ import annotations

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import os


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = "logistica"


def create_keyspace_and_tables():
    cluster = Cluster([CASSANDRA_HOST], port=9042)
    session = cluster.connect()

    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)

    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS ship_metrics (
            window_start timestamp,
            window_end timestamp,
            dest_port text,
            route_id text,
            avg_speed double,
            speed_std double,
            ship_count int,
            avg_lat double,
            avg_lon double,
            processed_at timestamp,
            PRIMARY KEY (dest_port, window_start)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS alert_metrics (
            window_start timestamp,
            window_end timestamp,
            region text,
            source text,
            avg_severity double,
            avg_confidence double,
            alert_count int,
            processed_at timestamp,
            PRIMARY KEY (region, window_start)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS ml_predictions (
            prediction_id timeuuid,
            ship_id text,
            dest_port text,
            predicted_delay_hours double,
            risk_category text,
            model_used text,
            confidence double,
            created_at timestamp,
            PRIMARY KEY (ship_id, created_at)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS route_alerts (
            alert_id timeuuid,
            dest_port text,
            risk_level text,
            risk_score double,
            severity int,
            recommendation text,
            email_sent boolean,
            created_at timestamp,
            PRIMARY KEY (dest_port, created_at)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_latest_state (
            ship_id text PRIMARY KEY,
            event_ts timestamp,
            route_id text,
            origin_port text,
            dest_port text,
            warehouse text,
            lat double,
            lon double,
            speed_kn double,
            heading double,
            stock_on_hand int,
            reorder_point int,
            updated_at timestamp
        )
    """)

    print("OK - Cassandra keyspace y tablas creadas")

    result = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", [KEYSPACE])
    print("Tablas creadas:")
    for row in result:
        print(f"  - {row.table_name}")

    cluster.shutdown()


def insert_ship_metrics(
    dest_port: str,
    route_id: str,
    avg_speed: float,
    speed_std: float,
    ship_count: int,
    avg_lat: float,
    avg_lon: float,
    window_start: str,
    window_end: str
):
    cluster = Cluster([CASSANDRA_HOST], port=9042)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)

    session.execute("""
        INSERT INTO ship_metrics
        (window_start, window_end, dest_port, route_id, avg_speed, speed_std,
         ship_count, avg_lat, avg_lon, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, toTimestamp(now()))
    """, (window_start, window_end, dest_port, route_id, avg_speed, speed_std,
          ship_count, avg_lat, avg_lon))

    cluster.shutdown()


def insert_alert(
    dest_port: str,
    risk_level: str,
    risk_score: float,
    severity: int,
    recommendation: str,
    email_sent: bool = False
):
    cluster = Cluster([CASSANDRA_HOST], port=9042)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)

    session.execute("""
        INSERT INTO route_alerts
        (alert_id, dest_port, risk_level, risk_score, severity,
         recommendation, email_sent, created_at)
        VALUES (now(), %s, %s, %s, %s, %s, %s, toTimestamp(now()))
    """, (dest_port, risk_level, risk_score, severity, recommendation, email_sent))

    cluster.shutdown()


def query_recent_alerts(hours: int = 24):
    cluster = Cluster([CASSANDRA_HOST], port=9042)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)

    result = session.execute("""
        SELECT * FROM route_alerts
        WHERE created_at >= toTimestamp(now()) - %s
        ORDER BY created_at DESC
    """, [hours * 3600])

    alerts = list(result)
    cluster.shutdown()
    return alerts


if __name__ == "__main__":
    create_keyspace_and_tables()
