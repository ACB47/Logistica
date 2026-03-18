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


def main() -> None:
    p = argparse.ArgumentParser(description="Simulador GPS barcos → Kafka datos_crudos")
    p.add_argument("--bootstrap", default="master:9092")
    p.add_argument("--topic", default="datos_crudos")
    p.add_argument("--ships", type=int, default=5)
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

    # Rutas simplificadas (Shanghai → destino ES)
    routes = [
        {"route_id": "SHA-ALG", "dest_port": "Algeciras", "dest_lat": 36.127, "dest_lon": -5.453},
        {"route_id": "SHA-VLC", "dest_port": "Valencia", "dest_lat": 39.458, "dest_lon": -0.317},
        {"route_id": "SHA-BCN", "dest_port": "Barcelona", "dest_lat": 41.352, "dest_lon": 2.173},
    ]
    origin = {"port": "Shanghai", "lat": 31.2304, "lon": 121.4737}

    ships = []
    for i in range(args.ships):
        r = random.choice(routes)
        ships.append(
            {
                "ship_id": f"SHIP-{1000+i}",
                "route_id": r["route_id"],
                "origin_port": origin["port"],
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

            lat = lerp(origin["lat"], r["dest_lat"], s["t"]) + random.uniform(-0.05, 0.05)
            lon = lerp(origin["lon"], r["dest_lon"], s["t"]) + random.uniform(-0.05, 0.05)
            heading = (math.degrees(math.atan2(r["dest_lon"] - origin["lon"], r["dest_lat"] - origin["lat"])) + 360) % 360

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

