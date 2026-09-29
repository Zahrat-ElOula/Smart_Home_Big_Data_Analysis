"""Phase 6, étape 2 : producteur Kafka.

Le script lit un échantillon des mesures nettoyées en Parquet et envoie chaque
événement dans le topic Kafka sous forme JSON. Il s'agit d'un rejeu
contrôlé du dataset historique, pas d'un capteur physique temps réel.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from phase1_eda import ensure_java_home
from phase2_cleaning import ensure_hadoop_windows


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "measurements_clean"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "outputs" / "phase6_streaming"


def log(message: str) -> None:
    print(f"[PHASE 6.2] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rejeu d'un échantillon Smart Home vers Kafka."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument(
        "--start",
        default=None,
        help="Début de la tranche rejouée, format 'yyyy-MM-dd HH:mm:ss'.",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="Fin de la tranche rejouée, format 'yyyy-MM-dd HH:mm:ss'.",
    )
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="smart-home-events")
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.01,
        help="Pause entre deux messages pour rendre le flux visible.",
    )
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--scratch-dir", type=Path, default=None)
    parser.add_argument(
        "--hadoop-home",
        type=Path,
        default=None,
        help="Dossier Hadoop contenant bin/winutils.exe et bin/hadoop.dll sous Windows.",
    )
    parser.add_argument("--log-level", default="WARN")
    args = parser.parse_args()

    if args.max_rows < 1:
        parser.error("--max-rows doit être supérieur à 0.")
    if args.delay_seconds < 0:
        parser.error("--delay-seconds doit être positif.")
    if args.start and args.end and args.start > args.end:
        parser.error("--start doit précéder --end.")
    return args


def create_spark(args: argparse.Namespace) -> SparkSession:
    ensure_java_home("PHASE 6.2")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")
    scratch = (
        args.scratch_dir.expanduser().resolve()
        if args.scratch_dir
        else Path(os.environ.get("TEMP", str(PROJECT_ROOT))) / "smart-home-phase6"
    )
    (scratch / "scratch").mkdir(parents=True, exist_ok=True)
    ensure_hadoop_windows(scratch, args.hadoop_home, "PHASE 6.2")
    spark = (
        SparkSession.builder.appName("SmartHome-Phase6-Kafka-Producer")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.dir", str(scratch / "scratch"))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


def event_payload(row: Any) -> dict[str, Any]:
    """Construit le message JSON.

    `timestamp` est déjà une chaîne produite par Spark SQL dans le fuseau de la
    session (UTC) : l'utiliser directement évite le décalage de fuseau qu'un
    passage par un `datetime` Python introduirait.
    """
    return {
        "value_id": int(row["value_id"]),
        "sensor_id": int(row["sensor_id"]),
        "timestamp": row["timestamp_text"],
        "value": float(row["value"]) if row["value"] is not None else None,
        "sensor_name": row["name"],
        "room": row["room"],
        "measurement": row["measurement"],
    }


def write_summary(
    report_dir: Path,
    args: argparse.Namespace,
    sent: int,
    first_timestamp: str | None,
    last_timestamp: str | None,
    started_at: float,
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "phase": "phase6_step2_kafka_producer",
        "started_at_utc": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_dir": str(args.input_dir.resolve()),
        "bootstrap_server": args.bootstrap_server,
        "topic": args.topic,
        "requested_rows": args.max_rows,
        "sent_rows": sent,
        "requested_start": args.start,
        "requested_end": args.end,
        "first_event_timestamp": first_timestamp,
        "last_event_timestamp": last_timestamp,
        "delay_seconds": args.delay_seconds,
        "payload_format": "JSON",
    }
    (report_dir / "producer_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def run(args: argparse.Namespace) -> None:
    started_at = time.time()
    input_dir = args.input_dir.expanduser().resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"Parquet nettoyé introuvable : {input_dir}. Exécutez d'abord phase2_cleaning.py."
        )

    spark = create_spark(args)
    data: DataFrame | None = None
    try:
        log(f"Lecture d'un maximum de {args.max_rows} mesures depuis {input_dir}.")
        measurements = (
            spark.read.parquet(str(input_dir)).filter(~F.col("is_suspect_value"))
        )
        if args.start:
            measurements = measurements.filter(
                F.col("timestamp") >= F.lit(args.start).cast("timestamp")
            )
        if args.end:
            measurements = measurements.filter(
                F.col("timestamp") < F.lit(args.end).cast("timestamp")
            )
        data = (
            measurements.select(
                "value_id",
                "sensor_id",
                "timestamp",
                # Horodatage rendu en UTC par Spark, sans detour par Python.
                F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss.SSSSSS").alias(
                    "timestamp_text"
                ),
                "value",
                "name",
                "room",
                "measurement",
            )
            .orderBy("timestamp")
            .limit(args.max_rows)
        )

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server.split(","),
            acks="all",
            retries=3,
            linger_ms=20,
            value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        )
        sent = 0
        failures: list[Exception] = []
        first_timestamp: str | None = None
        last_timestamp: str | None = None

        def on_error(exception: Exception) -> None:
            failures.append(exception)

        try:
            for row in data.toLocalIterator():
                payload = event_payload(row)
                # Envoi asynchrone : le productivite reste elevee, les
                # echecs d'acquittement sont comptes puis verifies.
                producer.send(args.topic, value=payload).add_errback(on_error)
                sent += 1
                if first_timestamp is None:
                    first_timestamp = payload["timestamp"]
                last_timestamp = payload["timestamp"]
                if sent % 10000 == 0:
                    log(f"{sent} messages envoyés")
                if args.delay_seconds:
                    time.sleep(args.delay_seconds)
            producer.flush(timeout=120)
        finally:
            producer.flush(timeout=60)
            producer.close(timeout=30)

        if failures:
            raise RuntimeError(
                f"{len(failures)} envois ont échoué, premier exemple : {failures[0]}"
            )

        write_summary(
            args.report_dir,
            args,
            sent,
            first_timestamp,
            last_timestamp,
            started_at,
        )
        log(f"Messages envoyés : {sent}")
        log(f"Planche rejouée : {first_timestamp} -> {last_timestamp}")
        log(f"Résumé : {args.report_dir / 'producer_summary.json'}")
    finally:
        if data is not None:
            data.unpersist()
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
