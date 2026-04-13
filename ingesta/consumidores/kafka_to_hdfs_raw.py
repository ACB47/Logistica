from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def utc_local_stamp(dt: datetime) -> str:
    # timestamp seguro para nombre de fichero local (sin "/")
    return dt.strftime("%Y%m%d_%H%M")


def utc_hdfs_stamp(dt: datetime) -> str:
    # yyyy/mm/dd/hhmm (particionado por micro-lote en HDFS)
    return dt.strftime("%Y/%m/%d/%H%M")


@dataclass(frozen=True)
class TopicSink:
    topic: str
    hdfs_base: str


SINKS = {
    "datos_crudos": TopicSink(topic="datos_crudos", hdfs_base="/hadoop/logistica/raw/ships"),
    # alertas_globales contiene clima + noticias; lo separamos por source para cumplir rutas
    "alertas_globales": TopicSink(topic="alertas_globales", hdfs_base="/hadoop/logistica/raw"),
}


def hdfs_put(local_file: Path, hdfs_dir: str) -> None:
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", str(local_file), hdfs_dir.rstrip("/") + "/"], check=True)


def main() -> None:
    p = argparse.ArgumentParser(description="Consume Kafka y aterriza JSONL raw en HDFS (rotación por minuto)")
    p.add_argument("--bootstrap", default="master:9092")
    p.add_argument("--group-id", default="logistica-raw-sink")
    p.add_argument("--topics", default="datos_crudos,alertas_globales")
    p.add_argument("--spool-dir", default="/tmp/logistica_spool")
    p.add_argument("--flush-every-sec", type=float, default=30.0, help="Volcar buffers a HDFS cada N segundos")
    args = p.parse_args()

    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    spool = Path(args.spool_dir)
    spool.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=args.bootstrap,
        group_id=args.group_id,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: (k.decode("utf-8") if k else None),
        consumer_timeout_ms=1000,
    )

    current_minute_local = utc_local_stamp(utc_now())
    current_minute_hdfs = utc_hdfs_stamp(utc_now())
    open_files: dict[str, Path] = {}
    buffers: dict[str, list[str]] = {}
    last_flush = time.time()

    def flush_and_ship() -> None:
        nonlocal open_files, buffers
        for name, lines in list(buffers.items()):
            if not lines:
                continue
            local_path = open_files[name]
            with local_path.open("a", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            buffers[name] = []

        # sube y limpia
        for name, local_path in list(open_files.items()):
            if not local_path.exists() or local_path.stat().st_size == 0:
                continue
            # name puede ser ships/clima/noticias
            hdfs_dir = f"/hadoop/logistica/raw/{name}/{current_minute_hdfs}"
            try:
                hdfs_put(local_path, hdfs_dir)
                print(f"[AUDIT] {utc_now_iso()} {name} -> {hdfs_dir}")
                local_path.unlink(missing_ok=True)
            except Exception as e:
                print(f"[ERROR] {utc_now_iso()} Fallo en {name} -> {hdfs_dir}: {e}")

    def ensure_files(minute_stamp: str) -> None:
        nonlocal open_files, buffers
        # ships siempre existe si consumimos datos_crudos
        for name in ("ships", "clima", "noticias"):
            local_path = spool / f"{name}_{minute_stamp}.jsonl"
            open_files[name] = local_path
            buffers.setdefault(name, [])

    ensure_files(current_minute_local)
    print(f"Consumiendo topics={topics} bootstrap={args.bootstrap} -> HDFS raw. Spool={spool}")

    while True:
        now_local = utc_local_stamp(utc_now())
        now_hdfs = utc_hdfs_stamp(utc_now())
        if now_hdfs != current_minute_hdfs:
            # rotación: sube el minuto anterior
            flush_and_ship()
            current_minute_local = now_local
            current_minute_hdfs = now_hdfs
            ensure_files(current_minute_local)

        got_any = False
        for msg in consumer:
            got_any = True
            topic = msg.topic
            value = msg.value

            value["_ingestion_ts"] = utc_now_iso()
            value["_ingestion_host"] = socket.gethostname()
            value["_ingestion_partition"] = msg.partition
            value["_ingestion_offset"] = msg.offset

            if topic == "datos_crudos":
                buffers["ships"].append(json.dumps(value, ensure_ascii=False))
            elif topic == "alertas_globales":
                source = (value.get("source") or "").strip().lower()
                if source == "clima":
                    buffers["clima"].append(json.dumps(value, ensure_ascii=False))
                elif source == "noticias":
                    buffers["noticias"].append(json.dumps(value, ensure_ascii=False))
                else:
                    buffers["noticias"].append(json.dumps(value, ensure_ascii=False))

            # flush por tamaño para no perder si se para
            if sum(len(v) for v in buffers.values()) >= 200:
                flush_and_ship()
                last_flush = time.time()

        # si no hay mensajes, duerme un poco
        if not got_any:
            time.sleep(0.5)

        # flush periódico para que haya evidencia en pocos segundos
        if time.time() - last_flush >= args.flush_every_sec:
            flush_and_ship()
            last_flush = time.time()


if __name__ == "__main__":
    main()

