"""Phase 6, étape 3 : lecture du topic Kafka avec Spark Structured Streaming.

Le consumer valide les messages JSON, les écrit dans une table mémoire puis
les sauvegarde en Parquet. L'application du modèle KMeans sera une étape
suivante, après validation de la lecture du flux.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from phase1_eda import ensure_java_home
from phase2_cleaning import ensure_hadoop_windows


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase6_streaming" / "parsed_events"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "outputs" / "phase6_streaming"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("value_id", T.LongType(), True),
        T.StructField("sensor_id", T.LongType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("value", T.DoubleType(), True),
        T.StructField("sensor_name", T.StringType(), True),
        T.StructField("room", T.StringType(), True),
        T.StructField("measurement", T.StringType(), True),
    ]
)


def log(message: str) -> None:
    print(f"[PHASE 6.3] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consommateur Spark Structured Streaming du topic Smart Home."
    )
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="smart-home-events")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=1000)
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

    if args.timeout_seconds < 1:
        parser.error("--timeout-seconds doit être supérieur à 0.")
    if args.max_offsets_per_trigger < 1:
        parser.error("--max-offsets-per-trigger doit être supérieur à 0.")
    return args


def create_spark(args: argparse.Namespace) -> SparkSession:
    ensure_java_home("PHASE 6.3")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")
    scratch = (
        args.scratch_dir.expanduser().resolve()
        if args.scratch_dir
        else Path(os.environ.get("TEMP", str(PROJECT_ROOT))) / "smart-home-phase6-consumer"
    )
    (scratch / "scratch").mkdir(parents=True, exist_ok=True)
    ensure_hadoop_windows(scratch, args.hadoop_home, "PHASE 6.3")

    spark = (
        SparkSession.builder.appName("SmartHome-Phase6-Kafka-Consumer")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.local.dir", str(scratch / "scratch"))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


def parse_messages(raw: DataFrame) -> DataFrame:
    event = F.from_json(F.col("value").cast("string"), EVENT_SCHEMA)
    projected = raw.select(
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_value"),
        event.alias("event"),
    )
    return (
        projected.select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "raw_value",
            F.col("event.value_id").alias("value_id"),
            F.col("event.sensor_id").alias("sensor_id"),
            F.col("event.timestamp").alias("event_timestamp_string"),
            F.col("event.value").alias("value"),
            F.col("event.sensor_name").alias("sensor_name"),
            F.col("event.room").alias("room"),
            F.col("event.measurement").alias("measurement"),
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp("event_timestamp_string", "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        )
        .drop("event_timestamp_string")
        .filter(F.col("value_id").isNotNull() & F.col("event_timestamp").isNotNull())
    )


def write_summary(
    report_dir: Path,
    args: argparse.Namespace,
    count: int,
    started_at: float,
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "phase": "phase6_step3_streaming_consumer",
        "started_at_utc": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "bootstrap_server": args.bootstrap_server,
        "topic": args.topic,
        "timeout_seconds": args.timeout_seconds,
        "max_offsets_per_trigger": args.max_offsets_per_trigger,
        "parsed_event_count": count,
        "output_dir": str(args.output_dir.resolve()),
    }
    (report_dir / "consumer_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def run(args: argparse.Namespace) -> None:
    started_at = time.time()
    output_dir = args.output_dir.expanduser().resolve()
    report_dir = args.report_dir.expanduser().resolve()
    spark = create_spark(args)
    query = None
    try:
        log(f"Lecture du topic Kafka {args.topic} sur {args.bootstrap_server}.")
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_server)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))
            .load()
        )
        parsed = parse_messages(raw)

        query = (
            parsed.writeStream.format("memory")
            .queryName("smart_home_parsed_events")
            .outputMode("append")
            .option("checkpointLocation", str(report_dir / "checkpoint"))
            .start()
        )
        log(f"Query démarrée. Attente de {args.timeout_seconds} secondes.")
        query.awaitTermination(args.timeout_seconds)
        query.stop()

        table = spark.table("smart_home_parsed_events")
        count = table.count()
        log(f"Messages JSON valides : {count}")
        output_dir.mkdir(parents=True, exist_ok=True)
        table.drop("raw_value").write.mode("overwrite").parquet(str(output_dir))
        log(f"Events sauvegardés dans {output_dir}")
        write_summary(report_dir, args, count, started_at)
    finally:
        if query is not None:
            try:
                query.stop()
            except Exception:
                pass
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
