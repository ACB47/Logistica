from __future__ import annotations

import argparse
import json
import math
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def interpolate_corridor(points: list[tuple[float, float]], progress: float) -> tuple[float, float]:
    progress = max(0.0, min(1.0, progress))
    segments = len(points) - 1
    scaled = progress * segments
    seg_index = min(int(scaled), segments - 1)
    local_t = scaled - seg_index
    start_lat, start_lon = points[seg_index]
    end_lat, end_lon = points[seg_index + 1]
    return (
        lerp(start_lat, end_lat, local_t),
        lerp(start_lon, end_lon, local_t),
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Simulador GPS barcos → Kafka datos_crudos")
    p.add_argument("--bootstrap", default="master:9092")
    p.add_argument("--topic", default="datos_crudos")
    p.add_argument("--ships", type=int, default=10)
    p.add_argument("--interval-sec", type=float, default=1.0)
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=50,
    )

    # Rutas simplificadas (Asia -> destino ES)
    routes = [
        {"route_id": "route-shanghai-algeciras", "origin_port": "Shanghai", "origin_lat": 31.2304, "origin_lon": 121.4737, "dest_port": "Algeciras", "dest_lat": 36.127, "dest_lon": -5.453},
        {"route_id": "route-shanghai-valencia", "origin_port": "Shanghai", "origin_lat": 31.2304, "origin_lon": 121.4737, "dest_port": "Valencia", "dest_lat": 39.458, "dest_lon": -0.317},
        {"route_id": "route-shanghai-barcelona", "origin_port": "Shanghai", "origin_lat": 31.2304, "origin_lon": 121.4737, "dest_port": "Barcelona", "dest_lat": 41.352, "dest_lon": 2.173},
        {"route_id": "route-yokohama-algeciras", "origin_port": "Yokohama", "origin_lat": 35.4437, "origin_lon": 139.6380, "dest_port": "Algeciras", "dest_lat": 36.127, "dest_lon": -5.453},
        {"route_id": "route-yokohama-valencia", "origin_port": "Yokohama", "origin_lat": 35.4437, "origin_lon": 139.6380, "dest_port": "Valencia", "dest_lat": 39.458, "dest_lon": -0.317},
        {"route_id": "route-yokohama-barcelona", "origin_port": "Yokohama", "origin_lat": 35.4437, "origin_lon": 139.6380, "dest_port": "Barcelona", "dest_lat": 41.352, "dest_lon": 2.173},
    ]
    corridors = {
        "route-shanghai-algeciras": [(31.2304, 121.4737), (22.0, 118.0), (13.0, 103.0), (6.0, 80.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 4.0), (36.1270, -5.4530)],
        "route-shanghai-valencia": [(31.2304, 121.4737), (22.0, 118.0), (13.0, 103.0), (6.0, 80.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 2.0), (39.4580, -0.3170)],
        "route-shanghai-barcelona": [(31.2304, 121.4737), (22.0, 118.0), (13.0, 103.0), (6.0, 80.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 5.0), (41.3520, 2.1730)],
        "route-yokohama-algeciras": [(35.4437, 139.6380), (30.0, 132.0), (18.0, 118.0), (8.0, 92.0), (6.0, 78.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 4.0), (36.1270, -5.4530)],
        "route-yokohama-valencia": [(35.4437, 139.6380), (30.0, 132.0), (18.0, 118.0), (8.0, 92.0), (6.0, 78.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 2.0), (39.4580, -0.3170)],
        "route-yokohama-barcelona": [(35.4437, 139.6380), (30.0, 132.0), (18.0, 118.0), (8.0, 92.0), (6.0, 78.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 5.0), (41.3520, 2.1730)],
    }

    ships = []
    for i in range(args.ships):
        r = routes[i % len(routes)]
        ships.append(
            {
                "ship_id": f"ship-{i+1:03d}",
                "route_id": r["route_id"],
                "origin_port": r["origin_port"],
                "dest_port": r["dest_port"],
                "t": random.random(),  # progreso 0..1
                "speed_kn": random.uniform(12.0, 20.0),
            }
        )

    print(f"Produciendo {len(ships)} barcos en topic={args.topic} bootstrap={args.bootstrap}")

    while True:
        for s in ships:
            # Avanza con algo de ruido
            s["t"] = min(1.0, max(0.0, s["t"] + random.uniform(0.001, 0.01)))
            r = next(x for x in routes if x["route_id"] == s["route_id"])

            lat, lon = interpolate_corridor(corridors[r["route_id"]], s["t"])
            lat += random.uniform(-0.08, 0.08)
            lon += random.uniform(-0.08, 0.08)
            heading = (math.degrees(math.atan2(r["dest_lon"] - r["origin_lon"], r["dest_lat"] - r["origin_lat"])) + 360) % 360

            event = {
                "event_type": "ship_position",
                "ts": utc_now_iso(),
                "ship_id": s["ship_id"],
                "route_id": s["route_id"],
                "origin_port": s["origin_port"],
                "dest_port": s["dest_port"],
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "speed_kn": round(s["speed_kn"] + random.uniform(-1.0, 1.0), 2),
                "heading": round(heading, 1),
            }

            producer.send(args.topic, key=s["ship_id"], value=event)

        producer.flush(timeout=5)
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    main()
