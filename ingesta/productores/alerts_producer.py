from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    p = argparse.ArgumentParser(description="Simulador alertas globales → Kafka alertas_globales")
    p.add_argument("--bootstrap", default="master:9092")
    p.add_argument("--topic", default="alertas_globales")
    p.add_argument("--interval-sec", type=float, default=5.0)
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=50,
    )

    regions = ["Suez", "Golfo de Adén", "Mediterráneo Occidental", "Mar de China", "Estrecho de Gibraltar"]
    weather_events = [
        "Viento fuerte",
        "Oleaje alto",
        "Tormenta eléctrica",
        "Lluvias intensas",
        "Niebla",
    ]
    geo_events = [
        "Tensión geopolítica",
        "Huelga portuaria",
        "Retrasos aduaneros",
        "Incidente de seguridad marítima",
    ]

    print(f"Produciendo alertas en topic={args.topic} bootstrap={args.bootstrap}")

    while True:
        is_weather = random.random() < 0.6
        severity = random.choice([1, 2, 3, 4, 5])
        region = random.choice(regions)

        if is_weather:
            text = random.choice(weather_events)
            source = "clima"
        else:
            text = random.choice(geo_events)
            source = "noticias"

        event = {
            "event_type": "global_alert",
            "ts": utc_now_iso(),
            "source": source,
            "severity": severity,
            "region": region,
            "text": text,
            "confidence": round(random.uniform(0.6, 0.98), 2),
        }

        producer.send(args.topic, key=source, value=event)
        producer.flush(timeout=5)
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    main()

