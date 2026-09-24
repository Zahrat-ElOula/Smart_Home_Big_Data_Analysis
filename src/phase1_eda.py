"""Phase 1 du mini-projet Smart Home : ingestion Spark et EDA.

Ce script :
- vérifie les trois fichiers du dataset Mendeley ;
- lit les CSV avec des schémas Spark explicites ;
- produit un inventaire et un résumé par fichier ;
- contrôle les qualité des données ;
- résume chaque capteur ;
- calcule des profils temporels ;
- calcule des quantiles sur un échantillon aléatoire ;
- produit des tableaux CSV, quelques graphiques et un rapport Markdown.

Le dataset complet dépasse 10 Go : il ne doit jamais être chargé dans Pandas.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import platform
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "human_activity_raw_sensor_data"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "eda"
EXPECTED_FILES = (
    "sensor.csv",
    "sensor_sample_int.csv",
    "sensor_sample_float.csv",
)
EXPECTED_HEADERS = {
    "sensor.csv": ["sensor_id", "node_id", "type", "name"],
    "sensor_sample_int.csv": ["value_id", "sensor_id", "timestamp", "value"],
    "sensor_sample_float.csv": ["value_id", "sensor_id", "timestamp", "value"],
}
MAX_REPORT_SENSORS = 10_000

SENSOR_SCHEMA = T.StructType(
    [
        T.StructField("sensor_id", T.LongType(), True),
        T.StructField("node_id", T.LongType(), True),
        T.StructField("type", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
    ]
)

SAMPLE_SCHEMA = T.StructType(
    [
        T.StructField("value_id", T.LongType(), True),
        T.StructField("sensor_id", T.LongType(), True),
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("value", T.DoubleType(), True),
        T.StructField("_corrupt_record", T.StringType(), True),
    ]
)

DAY_NAMES = {
    1: "Dimanche",
    2: "Lundi",
    3: "Mardi",
    4: "Mercredi",
    5: "Jeudi",
    6: "Vendredi",
    7: "Samedi",
}


def log(message: str) -> None:
    print(f"[EDA] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingestion et analyse exploratoire du dataset Smart Home avec PySpark.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--master",
        default="local[8]",
        help="Master Spark, par exemple local[8] ou spark://master:7077",
    )
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    parser.add_argument(
        "--scratch-dir",
        type=Path,
        default=None,
        help="Répertoire local pour le cache, les shuffles et les fichiers temporaires Spark.",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=0.01,
        help="Fraction sans remise utilisée pour les quantiles et profils temporels.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--write-sample-parquet",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Conserve l'échantillonutilisé pour les analyses approchées.",
    )
    parser.add_argument(
        "--charts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Génère les graphiques PNG.",
    )
    parser.add_argument("--log-level", default="WARN")
    args = parser.parse_args()

    if not 0 < args.sample_fraction <= 1:
        parser.error("--sample-fraction doit être compris entre 0 et 1.")
    if args.shuffle_partitions < 1:
        parser.error("--shuffle-partitions doit être supérieur à 0.")
    is_local_master = args.master == "local" or (
        args.master.startswith("local[") and args.master.endswith("]")
    )
    if not is_local_master:
        parser.error(
            "Cette version lit des chemins locaux et accepte uniquement un master "
            "Spark local, par exemple local[8]."
        )
    return args


def check_data_files(data_dir: Path) -> dict[str, Path]:
    data_dir = data_dir.expanduser().resolve()
    missing = [name for name in EXPECTED_FILES if not (data_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(
            f"Dataset incomplet dans {data_dir}. Fichiers manquants : {', '.join(missing)}"
        )
    return {name: data_dir / name for name in EXPECTED_FILES}


def build_inventory(files: dict[str, Path], output_dir: Path) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    for filename, path in files.items():
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            actual_header = next(csv.reader(handle))
        actual_header = [cell.strip() for cell in actual_header]
        expected_header = EXPECTED_HEADERS[filename]
        if [cell.lower() for cell in actual_header] != [
            cell.lower() for cell in expected_header
        ]:
            raise ValueError(
                f"En-tête CSV inattendu pour {path}. "
                f"Attendu={expected_header}, observé={actual_header}. "
                "Le fichier n'est pas lu pour éviter un mauvais alignement des colonnes."
            )
        size_bytes = path.stat().st_size
        inventory.append(
            {
                "filename": filename,
                "path": str(path),
                "size_bytes": size_bytes,
                "size_gb": round(size_bytes / (1024**3), 6),
                "header": ",".join(actual_header),
            }
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "dataset_inventory.json").open("w", encoding="utf-8") as handle:
        json.dump(inventory, handle, ensure_ascii=False, indent=2)
    save_rows_csv(
        inventory,
        ("filename", "path", "size_bytes", "size_gb", "header"),
        output_dir / "tables" / "dataset_inventory.csv",
    )
    return inventory


def ensure_java_home() -> Path:
    """Configure automatiquement un JDK 17+ déjà présent sur le poste Windows."""
    configured = os.environ.get("JAVA_HOME")
    if configured:
        java_home = Path(configured).expanduser()
        if (java_home / "bin" / "java.exe").is_file():
            return java_home

    program_files = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
    candidates: list[Path] = [
        program_files
        / "Neo4j Desktop 2"
        / "resources"
        / "offline"
        / "runtime"
        / "zulu17.60.17-ca-jdk17.0.16-win_x64",
    ]
    search_roots = [
        program_files / "Eclipse Adoptium",
        program_files / "Java",
        program_files / "Microsoft",
        Path.home() / ".jdks",
    ]
    for root in search_roots:
        if not root.is_dir():
            continue
        candidates.extend(sorted(root.glob("jdk-17*"), reverse=True))
        candidates.extend(sorted(root.glob("temurin-17*"), reverse=True))
        candidates.extend(sorted(root.glob("zulu17*"), reverse=True))

    for candidate in candidates:
        if (candidate / "bin" / "java.exe").is_file():
            os.environ["JAVA_HOME"] = str(candidate)
            os.environ["Path"] = (
                str(candidate / "bin")
                + os.pathsep
                + os.environ.get("Path", "")
            )
            log(f"JAVA_HOME détecté automatiquement : {candidate}")
            return candidate

    raise RuntimeError(
        "Java 17 ou supérieur est requis par Spark. Installe un JDK 17+ et "
        "configure JAVA_HOME avant de relancer le script."
    )


def create_spark(args: argparse.Namespace, output_dir: Path) -> SparkSession:
    ensure_java_home()
    # Évite que le nom Windows contenant « _ » soit utilisé comme URL RPC Spark.
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")
    scratch_dir = (
        args.scratch_dir.expanduser().resolve()
        if args.scratch_dir is not None
        else Path(os.environ.get("TEMP", str(PROJECT_ROOT))) / "smart-home-spark"
    )
    local_spark_dir = scratch_dir / "scratch"
    local_spark_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "_spark_warehouse").mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName("SmartHome-Phase1-EDA")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.local.dir", str(local_spark_dir))
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


def read_csv(
    spark: SparkSession,
    path: Path,
    schema: T.StructType,
    timestamp_format: str | None = None,
) -> DataFrame:
    reader = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .option("enforceSchema", "false")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .schema(schema)
    )
    if timestamp_format:
        reader = reader.option("timestampFormat", timestamp_format)
    return reader.csv(str(path))


def finite_value_condition() -> Any:
    value = F.col("value")
    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) != F.lit(float("inf")))


def prepare_sensor_metadata(raw: DataFrame, output_dir: Path) -> tuple[DataFrame, dict[str, int]]:
    cleaned = (
        raw.withColumn("sensor_id", F.col("sensor_id").cast("long"))
        .withColumn("node_id", F.col("node_id").cast("long"))
        .withColumn("type", F.upper(F.trim(F.col("type"))))
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("name_parts", F.split(F.col("name"), "/"))
        .withColumn("room", F.element_at(F.col("name_parts"), 1))
        .withColumn(
            "area_or_device",
            F.when(F.size(F.col("name_parts")) >= 2, F.element_at(F.col("name_parts"), 2)),
        )
        .withColumn(
            "measurement",
            F.element_at(F.col("name_parts"), F.size(F.col("name_parts"))),
        )
        .select("sensor_id", "node_id", "type", "name", "room", "area_or_device", "measurement")
    )

    null_sensor_ids = cleaned.filter(F.col("sensor_id").isNull()).count()
    duplicate_ids = (
        cleaned.filter(F.col("sensor_id").isNotNull())
        .groupBy("sensor_id")
        .count()
        .filter(F.col("count") > 1)
    )
    duplicate_count = duplicate_ids.count()
    duplicate_list = [row["sensor_id"] for row in duplicate_ids.orderBy("sensor_id").collect()]
    if duplicate_list:
        log(
            f"Attention : {duplicate_count} sensor_id dupliqué(s) dans sensor.csv. "
            "La ligne minimale après tri par sensor_id/node_id/name est conservée."
        )
        cleaned = (
            cleaned.orderBy("sensor_id", "node_id", "name")
            .dropDuplicates(["sensor_id"])
            .orderBy("sensor_id")
        )

    cleaned = cleaned.filter(F.col("sensor_id").isNotNull())
    cleaned = cleaned.persist(StorageLevel.MEMORY_ONLY)
    metadata = {
        "null_sensor_id_count": int(null_sensor_ids),
        "duplicate_sensor_id_count": int(duplicate_count),
    }
    save_dataframe_csv(cleaned.orderBy("sensor_id"), output_dir / "tables" / "sensor_metadata.csv")
    return cleaned, metadata


def load_samples(
    spark: SparkSession,
    files: dict[str, Path],
) -> DataFrame:
    timestamp_format = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    int_samples = (
        read_csv(spark, files["sensor_sample_int.csv"], SAMPLE_SCHEMA, timestamp_format)
        .withColumn("expected_type", F.lit("INT"))
        .withColumn("source_file", F.lit("sensor_sample_int.csv"))
    )
    float_samples = (
        read_csv(spark, files["sensor_sample_float.csv"], SAMPLE_SCHEMA, timestamp_format)
        .withColumn("expected_type", F.lit("FLOAT"))
        .withColumn("source_file", F.lit("sensor_sample_float.csv"))
    )
    return int_samples.select(
        "value_id", "sensor_id", "timestamp", "value", "expected_type", "source_file", "_corrupt_record"
    ).unionByName(
        float_samples.select(
            "value_id", "sensor_id", "timestamp", "value", "expected_type", "source_file", "_corrupt_record"
        )
    )


def summarize_by_file(samples: DataFrame, output_dir: Path) -> list[dict[str, Any]]:
    finite = finite_value_condition()
    finite_stat_value = F.when(finite, F.col("value"))
    summary = (
        samples.groupBy("source_file")
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.count("value_id").alias("non_null_value_id_count"),
            F.count("sensor_id").alias("non_null_sensor_id_count"),
            F.count("timestamp").alias("valid_timestamp_count"),
            F.count("value").alias("non_null_value_count"),
            F.count(finite_stat_value).alias("finite_value_count"),
            F.countDistinct("sensor_id").alias("sensor_count"),
            F.approx_count_distinct("value_id", 0.01).alias(
                "approx_distinct_value_id_count"
            ),
            F.min("timestamp").alias("min_timestamp"),
            F.max("timestamp").alias("max_timestamp"),
            F.min(finite_stat_value).alias("min_value"),
            F.max(finite_stat_value).alias("max_value"),
            F.avg(finite_stat_value).alias("mean_value"),
            F.stddev_pop(finite_stat_value).alias("stddev_pop_value"),
            F.sum(F.when(~finite, 1).otherwise(0)).alias("missing_or_non_finite_value_count"),
            F.sum(
                F.when(F.col("timestamp").isNull(), 1).otherwise(0)
            ).alias("missing_timestamp_count"),
            F.sum(
                F.when(F.col("sensor_id").isNull(), 1).otherwise(0)
            ).alias("missing_sensor_id_count"),
            F.sum(
                F.when(F.col("_corrupt_record").isNotNull(), 1).otherwise(0)
            ).alias("corrupt_record_count"),
            F.sum(F.when(finite & (F.col("value") < 0), 1).otherwise(0)).alias(
                "negative_value_count"
            ),
            F.sum(F.when(finite & (F.col("value") == 0), 1).otherwise(0)).alias(
                "zero_value_count"
            ),
            F.sum(F.when(finite & (F.col("value") > 0), 1).otherwise(0)).alias(
                "positive_value_count"
            ),
        )
        .withColumn(
            "duration_hours",
            F.round(
                (F.unix_micros("max_timestamp") - F.unix_micros("min_timestamp"))
                / 3_600_000_000.0,
                3,
            ),
        )
        .withColumn(
            "valid_value_rate",
            F.round(
                1.0 - F.col("missing_or_non_finite_value_count") / F.col("row_count"), 8
            ),
        )
        .withColumn(
            "valid_timestamp_rate",
            F.round(F.col("valid_timestamp_count") / F.col("row_count"), 8),
        )
        .withColumn(
            "hll_estimate_gap_from_non_null_ids",
            F.greatest(
                F.col("non_null_value_id_count") - F.col("approx_distinct_value_id_count"),
                F.lit(0),
            ),
        )
    )

    rows = save_dataframe_csv(
        summary.orderBy("source_file"), output_dir / "tables" / "summary_by_file.csv"
    )
    for row in rows:
        log(
            f"{row['source_file']}: {int(row['row_count']):,} lignes, "
            f"{row['min_timestamp']} -> {row['max_timestamp']}"
        )
    return rows


def build_overall_quality(
    file_rows: Sequence[dict[str, Any]],
    sensor_summary: DataFrame,
    metadata_quality: dict[str, int],
    sensor_metadata: DataFrame,
    output_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    total_rows = sum(int(row["row_count"]) for row in file_rows)
    valid_starts = [row["min_timestamp"] for row in file_rows if row["min_timestamp"] is not None]
    valid_ends = [row["max_timestamp"] for row in file_rows if row["max_timestamp"] is not None]
    if not valid_starts or not valid_ends:
        raise RuntimeError("Aucun timestamp valide n'a été lu dans les fichiers de mesures.")
    min_timestamp = min(valid_starts)
    max_timestamp = max(valid_ends)
    duration_seconds = (max_timestamp - min_timestamp).total_seconds()
    duration_hours = duration_seconds / 3600.0

    sum_columns = (
        "non_null_value_id_count",
        "non_null_sensor_id_count",
        "valid_timestamp_count",
        "non_null_value_count",
        "finite_value_count",
        "approx_distinct_value_id_count",
        "missing_or_non_finite_value_count",
        "missing_timestamp_count",
        "missing_sensor_id_count",
        "corrupt_record_count",
        "negative_value_count",
        "zero_value_count",
        "positive_value_count",
    )
    overall = {column: sum(int(row[column] or 0) for row in file_rows) for column in sum_columns}
    overall.update(
        {
            "row_count": total_rows,
            "min_timestamp": min_timestamp.isoformat(sep=" "),
            "max_timestamp": max_timestamp.isoformat(sep=" "),
            "duration_days": round(duration_seconds / 86400.0, 3),
            "duration_hours": round(duration_hours, 3),
            "approx_rows_per_hour": round(total_rows / duration_hours, 3) if duration_hours else None,
            "valid_value_rate": round(
                1.0 - overall["missing_or_non_finite_value_count"] / total_rows, 8
            ),
            "valid_timestamp_rate": round(overall["valid_timestamp_count"] / total_rows, 8),
            "null_sensor_id_rate": round(overall["missing_sensor_id_count"] / total_rows, 8),
            "corrupt_record_rate": round(overall["corrupt_record_count"] / total_rows, 8),
            "negative_value_rate": round(overall["negative_value_count"] / total_rows, 8),
            "hll_estimate_gap_from_non_null_ids_per_file_sum": sum(
                int(row["hll_estimate_gap_from_non_null_ids"] or 0) for row in file_rows
            ),
            **metadata_quality,
        }
    )

    orphan_events = sensor_summary.filter(
        ~F.col("sensor_id").isin(sensor_metadata.select("sensor_id"))
    )
    orphan_stats = orphan_events.agg(
        F.count(F.lit(1)).alias("sensor_count"),
        F.coalesce(F.sum("event_count"), F.lit(0)).alias("event_count"),
    ).first()
    orphan_rows = save_dataframe_csv(
        orphan_events.orderBy(F.desc("event_count")),
        output_dir / "tables" / "orphan_sensor_events.csv",
        row_limit=MAX_REPORT_SENSORS,
    )
    missing_sensors = sensor_metadata.join(
        sensor_summary.select("sensor_id").distinct(), on="sensor_id", how="left_anti"
    )
    missing_sensor_count = missing_sensors.count()
    missing_sensor_rows = save_dataframe_csv(
        missing_sensors.orderBy("sensor_id"),
        output_dir / "tables" / "sensors_without_measurements.csv",
        row_limit=MAX_REPORT_SENSORS,
    )
    type_quality = sensor_summary.agg(
        F.coalesce(
            F.sum(F.when(F.col("type_mismatch"), F.col("event_count")).otherwise(0)),
            F.lit(0),
        ).alias("mismatched_event_count"),
        F.count(F.when(F.col("type_mismatch"), F.lit(1))).alias("mismatched_sensor_count"),
        F.count(
            F.when(F.col("source_type_count") > 1, F.lit(1))
        ).alias("source_type_conflict_sensor_count"),
    ).first()
    measured_sensor_count = sensor_summary.select("sensor_id").distinct().count()
    overall["measured_sensor_count"] = int(measured_sensor_count)
    overall["orphan_sensor_count"] = int(orphan_stats["sensor_count"])
    overall["orphan_event_count"] = int(orphan_stats["event_count"])
    overall["sensor_without_measurement_count"] = int(missing_sensor_count)
    overall["mismatched_sensor_count"] = int(type_quality["mismatched_sensor_count"])
    overall["mismatched_event_count"] = int(type_quality["mismatched_event_count"])
    overall["source_type_conflict_sensor_count"] = int(
        type_quality["source_type_conflict_sensor_count"]
    )
    overall["orphan_rows_reported"] = len(orphan_rows)
    overall["missing_sensor_rows_reported"] = len(missing_sensor_rows)

    with (output_dir / "tables" / "overall_quality.json").open("w", encoding="utf-8") as handle:
        json.dump(to_jsonable(overall), handle, ensure_ascii=False, indent=2)
    return overall, orphan_rows, missing_sensor_rows


def summarize_sensors(
    samples: DataFrame,
    sensor_metadata: DataFrame,
    global_duration_hours: float,
    output_dir: Path,
) -> tuple[DataFrame, list[dict[str, Any]]]:
    finite = finite_value_condition()
    finite_stat_value = F.when(finite, F.col("value"))
    event_summary = (
        samples.groupBy("sensor_id")
        .agg(
            F.array_join(
                F.sort_array(F.collect_set("expected_type")), ","
            ).alias("observed_source_types"),
            F.count(F.lit(1)).alias("event_count"),
            F.count("value_id").alias("non_null_value_id_count"),
            F.count("timestamp").alias("valid_timestamp_count"),
            F.count("value").alias("non_null_value_count"),
            F.count(finite_stat_value).alias("finite_value_count"),
            F.min("timestamp").alias("first_timestamp"),
            F.max("timestamp").alias("last_timestamp"),
            F.min(finite_stat_value).alias("min_value"),
            F.max(finite_stat_value).alias("max_value"),
            F.avg(finite_stat_value).alias("mean_value"),
            F.stddev_pop(finite_stat_value).alias("stddev_pop_value"),
            F.sum(F.when(~finite, 1).otherwise(0)).alias(
                "missing_or_non_finite_value_count"
            ),
            F.sum(F.when(finite & (F.col("value") < 0), 1).otherwise(0)).alias(
                "negative_value_count"
            ),
            F.sum(F.when(finite & (F.col("value") == 0), 1).otherwise(0)).alias(
                "zero_value_count"
            ),
            F.sum(F.when(finite & (F.col("value") > 0), 1).otherwise(0)).alias(
                "positive_value_count"
            ),
        )
        .withColumn(
            "duration_hours",
            F.round(
                (F.unix_micros("last_timestamp") - F.unix_micros("first_timestamp"))
                / 3_600_000_000.0,
                3,
            ),
        )
        .withColumn(
            "events_per_hour",
            F.when(
                F.col("duration_hours") > 0,
                F.round(F.col("event_count") / F.col("duration_hours"), 6),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "observation_span_ratio",
            F.round(
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        F.col("duration_hours") / F.lit(global_duration_hours),
                    ),
                ),
                8,
            )
            if global_duration_hours > 0
            else F.lit(None).cast("double"),
        )
    )

    enriched = (
        event_summary.join(
            sensor_metadata,
            on="sensor_id",
            how="left_outer",
        )
        .persist(StorageLevel.MEMORY_ONLY)
        .withColumn(
            "source_type_count",
            F.size(F.split(F.col("observed_source_types"), ",")),
        )
        .withColumn(
            "metadata_type_missing",
            F.col("type").isNull() | (F.trim(F.col("type")) == ""),
        )
        .withColumn(
            "type_mismatch",
            F.col("metadata_type_missing")
            | (F.col("source_type_count") != 1)
            | (F.col("observed_source_types") != F.col("type")),
        )
    )
    columns = [
        "sensor_id",
        "node_id",
        "type",
        "observed_source_types",
        "source_type_count",
        "metadata_type_missing",
        "type_mismatch",
        "name",
        "room",
        "area_or_device",
        "measurement",
        "event_count",
        "non_null_value_id_count",
        "valid_timestamp_count",
        "non_null_value_count",
        "finite_value_count",
        "first_timestamp",
        "last_timestamp",
        "duration_hours",
        "observation_span_ratio",
        "events_per_hour",
        "min_value",
        "max_value",
        "mean_value",
        "stddev_pop_value",
        "missing_or_non_finite_value_count",
        "negative_value_count",
        "zero_value_count",
        "positive_value_count",
    ]
    ordered_summary = enriched.select(*columns).orderBy(
        F.desc("event_count"), "sensor_id"
    )
    sensor_cardinality_probe = (
        enriched.select("sensor_id").distinct().limit(MAX_REPORT_SENSORS + 1).count()
    )
    if sensor_cardinality_probe > MAX_REPORT_SENSORS:
        log(
            f"Plus de {MAX_REPORT_SENSORS:,} sensor_id distincts : le rapport CSV sera "
            "limité, tandis que l'audit complet sera écrit avec Spark."
        )
        (
            ordered_summary.write.mode("overwrite")
            .option("header", "true")
            .csv(str(output_dir / "tables" / "sensor_summary_all"))
        )
        rows = save_dataframe_csv(
            ordered_summary,
            output_dir / "tables" / "sensor_summary.csv",
            row_limit=MAX_REPORT_SENSORS,
        )
    else:
        rows = save_dataframe_csv(
            ordered_summary,
            output_dir / "tables" / "sensor_summary.csv",
        )
    return enriched, rows


def make_sample(
    samples: DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> tuple[DataFrame, int]:
    sampled = samples.sample(withReplacement=False, fraction=args.sample_fraction, seed=args.seed)
    sampled = sampled.persist(StorageLevel.DISK_ONLY)
    sample_count = sampled.count()
    log(
        f"Échantillon sans remise : {sample_count:,} lignes "
        f"({args.sample_fraction:.4%}, seed={args.seed})."
    )
    if args.write_sample_parquet and sample_count:
        sample_dir = output_dir / f"sample_{args.sample_fraction:g}"
        (
            sampled.coalesce(16)
            .write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(sample_dir))
        )
        log(f"Échantillon sauvegardé dans {sample_dir}.")
    return sampled, sample_count


def calculate_quantiles(
    sampled: DataFrame,
    sensor_metadata: DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> list[dict[str, Any]]:
    quantiles = {
        "p01": 0.01,
        "p05": 0.05,
        "p25": 0.25,
        "p50": 0.50,
        "p75": 0.75,
        "p95": 0.95,
        "p99": 0.99,
    }
    valid = sampled.filter(finite_value_condition() & F.col("sensor_id").isNotNull())
    expressions = [
        F.expr(f"percentile_approx(value, {percentage}, 10000)").cast("double").alias(name)
        for name, percentage in quantiles.items()
    ]
    quantile_summary = (
        valid.groupBy("sensor_id")
        .agg(
            F.count(F.lit(1)).alias("sampled_valid_value_count"),
            F.min("value").alias("sample_min_value"),
            F.max("value").alias("sample_max_value"),
            *expressions,
        )
        .join(sensor_metadata, on="sensor_id", how="left_outer")
    )
    columns = [
        "sensor_id",
        "type",
        "name",
        "room",
        "sampled_valid_value_count",
        "sample_min_value",
        "sample_max_value",
        *quantiles.keys(),
    ]
    rows = save_dataframe_csv(
        quantile_summary.select(*columns).orderBy("type", "name"),
        output_dir / "tables" / "value_quantiles_sample.csv",
    )
    return rows


def calculate_temporal_profiles(
    sampled: DataFrame,
    sensor_metadata: DataFrame,
    output_dir: Path,
) -> dict[str, Any]:
    valid = (
        sampled.filter(
            F.col("timestamp").isNotNull()
            & finite_value_condition()
            & F.col("sensor_id").isNotNull()
            & F.col("_corrupt_record").isNull()
        )
        .join(
            sensor_metadata.select("sensor_id"),
            on="sensor_id",
            how="inner",
        )
        .withColumn("event_date", F.to_date("timestamp"))
    )
    daily = (
        valid.groupBy("event_date")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum(F.when(F.col("expected_type") == "INT", 1).otherwise(0)).alias("int_count"),
            F.sum(F.when(F.col("expected_type") == "FLOAT", 1).otherwise(0)).alias(
                "float_count"
            ),
        )
        .orderBy("event_date")
    )
    daily_rows = save_dataframe_csv(
        daily, output_dir / "tables" / "daily_activity_sample.csv"
    )
    active_days = len(daily_rows)

    hourly = (
        valid.withColumn("event_hour", F.trunc("timestamp", "hour"))
        .groupBy("event_hour")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum(F.when(F.col("expected_type") == "INT", 1).otherwise(0)).alias("int_count"),
            F.sum(F.when(F.col("expected_type") == "FLOAT", 1).otherwise(0)).alias(
                "float_count"
            ),
        )
        .orderBy("event_hour")
    )
    save_dataframe_csv(hourly, output_dir / "tables" / "hourly_activity_sample.csv")

    hour_of_day = (
        valid.groupBy(F.hour("timestamp").alias("hour_of_day"))
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.countDistinct("event_date").alias("days_with_events"),
        )
        .withColumn(
            "mean_events_per_active_day",
            F.round(F.col("event_count") / F.col("days_with_events"), 3),
        )
        .orderBy("hour_of_day")
    )
    hour_rows = save_dataframe_csv(
        hour_of_day, output_dir / "tables" / "activity_by_hour_of_day.csv"
    )

    day_of_week = (
        valid.groupBy(F.dayofweek("timestamp").alias("day_of_week"))
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.countDistinct("event_date").alias("dates_with_events"),
        )
        .withColumn(
            "mean_events_per_date",
            F.round(F.col("event_count") / F.col("dates_with_events"), 3),
        )
        .orderBy("day_of_week")
    )
    day_rows = save_dataframe_csv(
        day_of_week, output_dir / "tables" / "activity_by_day_of_week.csv"
    )

    top_sensor_examples = (
        sampled.orderBy(F.desc("value_id")).limit(30)
    )
    save_dataframe_csv(
        sampled.orderBy("timestamp").limit(30),
        output_dir / "tables" / "sample_events_first_by_time.csv",
    )
    save_dataframe_csv(
        top_sensor_examples,
        output_dir / "tables" / "sample_events_highest_ids.csv",
    )

    peak_hour = max(hour_rows, key=lambda row: row["event_count"]) if hour_rows else None
    peak_day = max(daily_rows, key=lambda row: row["event_count"]) if daily_rows else None
    peak_weekday = max(day_rows, key=lambda row: row["event_count"]) if day_rows else None
    return {
        "active_days_in_sample": active_days,
        "peak_hour": peak_hour,
        "peak_day": peak_day,
        "peak_day_of_week": peak_weekday,
        "daily_rows": daily_rows,
        "hour_rows": hour_rows,
        "day_of_week_rows": day_rows,
    }


def save_dataframe_csv(
    dataframe: DataFrame,
    path: Path,
    row_limit: int | None = None,
) -> list[dict[str, Any]]:
    if row_limit is not None:
        dataframe = dataframe.limit(row_limit)
    rows = dataframe.collect()
    save_rows_csv(rows, dataframe.columns, path)
    return [row.asDict(recursive=True) for row in rows]


def save_rows_csv(rows: Iterable[Row | dict[str, Any]], columns: Sequence[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            values = row.asDict(recursive=True) if isinstance(row, Row) else dict(row)
            writer.writerow({column: csv_value(values.get(column)) for column in columns})


def csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        return f"{value:.10g}"
    return value


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def create_charts(
    output_dir: Path,
    sensor_rows: Sequence[dict[str, Any]],
    temporal: dict[str, Any],
    global_start: datetime,
    global_end: datetime,
) -> list[str]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
    except ImportError as exc:
        log(f"Graphiques ignorés : matplotlib n'est pas installé ({exc}).")
        return []

    charts_dir = output_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    generated: list[str] = []
    plt.rcParams.update({"figure.figsize": (10, 5), "axes.titlesize": 13})

    daily_rows = temporal["daily_rows"]
    if daily_rows:
        x_values = [datetime.combine(row["event_date"], datetime.min.time()) for row in daily_rows]
        y_values = [row["event_count"] for row in daily_rows]
        fig, ax = plt.subplots()
        ax.plot(x_values, y_values, color="#2563eb", linewidth=1.4)
        ax.set(title="Activité quotidienne de l'échantillon", xlabel="Date", ylabel="Mesures")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
        fig.autofmt_xdate()
        fig.tight_layout()
        path = charts_dir / "daily_activity.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        generated.append(str(path))

    hour_rows = temporal["hour_rows"]
    if hour_rows:
        fig, ax = plt.subplots()
        ax.bar(
            [row["hour_of_day"] for row in hour_rows],
            [row["event_count"] for row in hour_rows],
            color="#0f766e",
        )
        ax.set(
            title="Activité selon l'heure de la journée",
            xlabel="Heure",
            ylabel="Mesures dans l'échantillon",
        )
        ax.set_xticks(range(24))
        fig.tight_layout()
        path = charts_dir / "activity_by_hour.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        generated.append(str(path))

    if sensor_rows:
        top = sensor_rows[:15][::-1]
        fig, ax = plt.subplots(figsize=(11, 7))
        ax.barh(
            [row["name"] for row in top],
            [row["event_count"] for row in top],
            color="#7c3aed",
        )
        ax.set(title="15 capteurs les plus actifs", xlabel="Nombre de mesures")
        fig.tight_layout()
        path = charts_dir / "top_sensors.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        generated.append(str(path))

        valid_sensor_rows = [
            row
            for row in sensor_rows
            if row.get("first_timestamp") is not None
            and row.get("last_timestamp") is not None
        ]
        fig, ax = plt.subplots(figsize=(11, 8))
        ordered = list(reversed(valid_sensor_rows))
        y_positions = list(range(len(ordered)))
        for y, row in zip(y_positions, ordered):
            ax.hlines(
                y,
                row["first_timestamp"],
                row["last_timestamp"],
                color="#64748b",
                linewidth=2,
            )
        ax.set_yticks(y_positions)
        ax.set_yticklabels([row["name"] for row in ordered], fontsize=8)
        ax.set(
            title="Période couverte par chaque capteur",
            xlabel="Date",
            ylabel="Capteur",
            xlim=(global_start, global_end),
        )
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m/%Y"))
        fig.autofmt_xdate()
        fig.tight_layout()
        path = charts_dir / "sensor_coverage.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        generated.append(str(path))

    return generated


def markdown_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    if not rows:
        return "Aucune donnée.\n"
    result = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        result.append("| " + " | ".join(format_markdown(value) for value in row) + " |")
    return "\n".join(result) + "\n"


def format_markdown(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if not math.isfinite(value):
            return "N/A"
        if abs(value) >= 1000:
            return f"{value:,.2f}"
        return f"{value:,.6g}"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value).replace("|", "\\|")


def write_report(
    output_dir: Path,
    args: argparse.Namespace,
    inventory: Sequence[dict[str, Any]],
    file_rows: Sequence[dict[str, Any]],
    overall: dict[str, Any],
    sensor_rows: Sequence[dict[str, Any]],
    temporal: dict[str, Any],
    charts: Sequence[str],
    completed_at: datetime,
) -> Path:
    report_path = output_dir / "eda_report.md"
    peak_hour = temporal.get("peak_hour")
    peak_day = temporal.get("peak_day")
    peak_weekday = temporal.get("peak_day_of_week")
    top_sensors = sensor_rows[:10]
    missing_sensors = overall["sensor_without_measurement_count"]
    orphan_sensors = overall["orphan_sensor_count"]

    inventory_table = markdown_table(
        ["Fichier", "Taille (Go)", "En-tête"],
        [
            [row["filename"], row["size_gb"], f"`{row['header']}`"]
            for row in inventory
        ],
    )
    file_table = markdown_table(
        [
            "Fichier",
            "Lignes",
            "Capteurs",
            "Début",
            "Fin",
            "Valeurs valides",
            "Négatives",
            "Enregistrements corrompus",
        ],
        [
            [
                row["source_file"],
                row["row_count"],
                row["sensor_count"],
                row["min_timestamp"],
                row["max_timestamp"],
                f"{row['valid_value_rate']:.4%}",
                row["negative_value_count"],
                row["corrupt_record_count"],
            ]
            for row in file_rows
        ],
    )
    sensor_table = markdown_table(
        ["Capteur", "Type", "Pièce", "Mesures", "Début", "Fin", "Moyenne", "Min", "Max"],
        [
            [
                row["name"],
                row["type"],
                row["room"],
                row["event_count"],
                row["first_timestamp"],
                row["last_timestamp"],
                row["mean_value"],
                row["min_value"],
                row["max_value"],
            ]
            for row in top_sensors
        ],
    )
    quality_table = markdown_table(
        ["Contrôle", "Résultat", "Interprétation"],
        [
            ["Timestamp valide", f"{overall['valid_timestamp_rate']:.4%}", "Proportion de mesures exploitables dans le temps."],
            ["Valeur numérique valide", f"{overall['valid_value_rate']:.4%}", "Exclut null, NaN et infini."],
            ["sensor_id manquant", overall["missing_sensor_id_count"], "Doit être proche de zéro pour pouvoir joindre au métadonnées."],
            ["Enregistrement CSV corrompu", overall["corrupt_record_count"], "Ligne qui ne respecte pas complètement le schéma."],
            ["Valeurs négatives", overall["negative_value_count"], "Peuvent être normales pour une température ; ne pas les supprimer sans vérifier le capteur."],
            ["Capteurs sans mesures", missing_sensors, "Capteurs déclarés dans sensor.csv mais absents des mesures."],
            ["Mesures sans capteur déclaré", orphan_sensors, "Anomalie d'intégrité : ces lignes ne peuvent pas être nommées."],
            ["Capteurs avec type source incohérent", overall["mismatched_sensor_count"], "Le fichier INT/FLOAT ne correspond pas au type déclaré ou les deux fichiers sont mélangés."],
            ["Mesures avec type incohérent", overall["mismatched_event_count"], "Volume total associé aux incohérences de type."],
            ["Capteurs présents dans les deux fichiers", overall["source_type_conflict_sensor_count"], "Un même sensor_id ne devrait normalement pas être mesuré comme INT et FLOAT."],
            ["sensor_id dupliqués dans métadonnées", overall["duplicate_sensor_id_count"], "Après tri, une ligne déterministe est conservée et le conflit est signalé."],
        ],
    )
    interpretation = "\n".join(
        [
            f"- **Volume** : les deux fichiers de mesures contiennent **{overall['row_count']:,} lignes**. Cette taille justifie Spark et interdit un chargement complet avec Pandas.",
            f"- **Période** : les mesures couvrent **{overall['min_timestamp']}** à **{overall['max_timestamp']}**, soit environ **{overall['duration_days']:.2f} jours**.",
            f"- **Capteurs** : **{overall['measured_sensor_count']}** capteurs produisent des mesures. Les dix plus actifs sont affichés dans le tableau.",
            f"- **Heure la plus active** : {peak_hour['hour_of_day']:02d}:00–{peak_hour['hour_of_day']:02d}:59 avec {peak_hour['event_count']:,} mesures dans l'échantillon." if peak_hour else "- **Heure la plus active** : non calculée.",
            f"- **Jour le plus actif** : {peak_day['event_date']} avec {peak_day['event_count']:,} mesures dans l'échantillon." if peak_day else "- **Jour le plus actif** : non calculé.",
            f"- **Jour de semaine le plus chargé** : {DAY_NAMES.get(peak_weekday['day_of_week'], peak_weekday['day_of_week']) if peak_weekday else 'non calculé'}.",
            "- **Attention à l’interprétation** : les capteurs n'ont pas la même unité. Une valeur de 1024 pour une lumière ou une pression n’est pas directement comparable à une valeur de courant électrique.",
            "- **Objectif ML** : aucune colonne d’activité étiquetée n’est présente. L’étape suivante devra donc créer des fenêtres temporelles puis, soit produire des pseudo-étiquettes validées, soit utiliser une méthode non supervisée.",
        ]
    )

    output_list = [
        "`tables/dataset_inventory.csv`",
        "`tables/sensor_metadata.csv`",
        "`tables/summary_by_file.csv`",
        "`tables/overall_quality.json`",
        "`tables/sensor_summary.csv`",
        "`tables/value_quantiles_sample.csv`",
        "`tables/daily_activity_sample.csv`",
        "`tables/activity_by_hour_of_day.csv`",
        "`tables/activity_by_day_of_week.csv`",
    ]
    if charts:
        output_list.extend(f"`charts/{Path(path).name}`" for path in charts)

    content = f"""# Rapport EDA — Smart Home

Généré le {completed_at.isoformat(sep=" ", timespec="seconds")} UTC.

## 1. Démarche

1. Lire `sensor.csv` pour obtenir les déclarations des capteurs et leur|type de mesure.
2. Lire les deux fichiers de mesures avec un schéma Spark explicite.
3. Uniformiser les types, convertir les timestamps et conserver les enregistrements invalides comme métriques de qualité.
4. Calculer les volumes, périodes,min/max, moyennes et contrôles de qualité.
5. Joindre les mesures aux métadonnées sur `sensor_id`.
6. Construire un échantillon aléatoire sans remise de **{args.sample_fraction:.4%}**, soit **{int(overall.get('sample_count', 0)):,} lignes** (seed `{args.seed}`), puis conserver pour les profils temporelles uniquement les événements à timestamp et valeur finis, dont le capteur est déclaré.
7. Produire des tableaux CSV, des graphiques et une synthèse Markdown.

Les volumes, périodes, min/max, valeurs manquantes et l'intégrité `sensor_id` sont calculés sur **toutes les lignes**. Les quantiles et les profils temporels sont calculés sur l'échantillon afin de limiter les recomputations et la sortie sur le driver.

## 2. Inventaire des fichiers

{inventory_table}
## 3. Résumé des mesures

{file_table}
## 4. Qualité globale

{quality_table}
## 5. Capteurs les plus actifs

{sensor_table}
## 6. Interprétation des résultats

{interpretation}
## 7. Limites à connaître

- Les distributions de `value_quantiles_sample.csv` sont **approchatives** (`percentile_approx`) et reposent sur l'échantillon.
- Les profils d'activité sont des volumes de mesures valides, pas encore des activités humaines validées.
- `observation_span_ratio` compare seulement la période entre la première et la dernière mesure ; il ne mesure pas la couverture réelle des instants intermédiaires.
- Le comptage approximatif `approx_count_distinct` (HLL) fournit un ordre de grandeur, mais son écart avec `count` ne prouve pas la présence de doublons : une vérification exacte serait beaucoup plus coûteuse.
- Le dataset décrit un utilisateur unique. Les résultats ne doivent pas être généralisés à plusieurs foyers.
- Une valeur anormale n'est pas forcément une erreur : une température peut être négative et un pic de courant peut représenter une utilisation réelle.

## 8. Sorties

{chr(10).join(f'- {item}' for item in output_list)}
"""
    report_path.write_text(content, encoding="utf-8")
    return report_path


def write_run_manifest(
    output_dir: Path,
    args: argparse.Namespace,
    spark: SparkSession,
    data_dir: Path,
    inventory: Sequence[dict[str, Any]],
    overall: dict[str, Any],
    started_at: datetime,
    completed_at: datetime,
    report_path: Path,
) -> Path:
    manifest = {
        "phase": "phase1_eda",
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": completed_at.isoformat(),
        "data_dir": str(data_dir),
        "output_dir": str(output_dir),
        "master": args.master,
        "shuffle_partitions": args.shuffle_partitions,
        "sample_fraction": args.sample_fraction,
        "sample_seed": args.seed,
        "python_version": platform.python_version(),
        "pyspark_version": spark.version,
        "java_version": spark._jvm.java.lang.System.getProperty("java.version"),
        "files": inventory,
        "overall_quality": to_jsonable(overall),
        "report": str(report_path),
    }
    path = output_dir / "run_manifest.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
    return path


def run(args: argparse.Namespace) -> None:
    started_at = datetime.now(timezone.utc)
    data_dir = args.data_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    log(f"Vérification du dataset dans {data_dir}")
    files = check_data_files(data_dir)
    inventory = build_inventory(files, output_dir)
    log(f"{len(files)} fichiers trouvés ; total = {sum(row['size_bytes'] for row in inventory):,} octets.")

    spark = create_spark(args, output_dir)
    sensor_metadata: DataFrame | None = None
    samples: DataFrame | None = None
    sensor_summary: DataFrame | None = None
    sampled: DataFrame | None = None
    try:
        log("Lecture de sensor.csv et création des métadonnées normalisées.")
        sensor_metadata, metadata_quality = prepare_sensor_metadata(
            read_csv(spark, files["sensor.csv"], SENSOR_SCHEMA), output_dir
        )

        log("Lecture des mesures avec PySpark (réalisation distributionnelle).")
        samples = load_samples(spark, files).persist(StorageLevel.DISK_ONLY)

        log("Calcul des statistiques par fichier et des contrôles de qualité.")
        file_rows = summarize_by_file(samples, output_dir)

        log("Calcul du résumé de chaque capteur.")
        global_starts = [
            row["min_timestamp"] for row in file_rows if row["min_timestamp"] is not None
        ]
        global_ends = [
            row["max_timestamp"] for row in file_rows if row["max_timestamp"] is not None
        ]
        if not global_starts or not global_ends:
            raise RuntimeError("Impossible de définir la période globale sans timestamp valide.")
        global_start = min(global_starts)
        global_end = max(global_ends)
        global_duration_hours = (global_end - global_start).total_seconds() / 3600.0
        sensor_summary, sensor_rows = summarize_sensors(
            samples, sensor_metadata, global_duration_hours, output_dir
        )

        log("Construction de l'échantillon analytique.")
        sampled, sample_count = make_sample(samples, args, output_dir)
        if not sample_count:
            raise RuntimeError(
                "L'échantillon analytique est vide. Augmentez --sample-fraction."
            )
        samples.unpersist(blocking=True)
        samples = None
        log("Cache des mesures complètes libéré ; la suite utilise uniquement l'échantillon.")

        log("Calcul des quantiles approximatifs sur l'échantillon.")
        quantile_rows = calculate_quantiles(sampled, sensor_metadata, args, output_dir)
        log(
            "Calcul des profils temporels quotidiens, horaires et hebdomadaires sur l'échantillon."
        )
        temporal = calculate_temporal_profiles(sampled, sensor_metadata, output_dir)

        log("Contrôle final des relations entre mesures et métadonnées.")
        overall, _, _ = build_overall_quality(
            file_rows, sensor_summary, metadata_quality, sensor_metadata, output_dir
        )
        overall["sample_count"] = sample_count
        with (output_dir / "tables" / "overall_quality.json").open(
            "w", encoding="utf-8"
        ) as handle:
            json.dump(to_jsonable(overall), handle, ensure_ascii=False, indent=2)

        charts = (
            create_charts(output_dir, sensor_rows, temporal, global_start, global_end)
            if args.charts
            else []
        )
        completed_at = datetime.now(timezone.utc)
        report_path = write_report(
            output_dir,
            args,
            inventory,
            file_rows,
            overall,
            sensor_rows,
            temporal,
            charts,
            completed_at,
        )
        manifest_path = write_run_manifest(
            output_dir,
            args,
            spark,
            data_dir,
            inventory,
            overall,
            started_at,
            completed_at,
            report_path,
        )

        log(f"EDA terminé. Lignes lues : {overall['row_count']:,}")
        log(f"Rapport : {report_path}")
        log(f"Manifeste : {manifest_path}")
        log(
            f"Capteurs avec mesures : {overall['measured_sensor_count']} ; "
            f"quantiles calculés : {len(quantile_rows)}"
        )
    finally:
        if sampled is not None:
            sampled.unpersist()
        if sensor_summary is not None:
            sensor_summary.unpersist()
        if samples is not None:
            samples.unpersist()
        if sensor_metadata is not None:
            sensor_metadata.unpersist()
        spark.stop()


def main() -> int:
    try:
        run(parse_args())
        return 0
    except KeyboardInterrupt:
        log("Interruption par l'utilisateur.")
        return 130
    except Exception as exc:
        log(f"ÉCHEC : {exc}")
        raise


if __name__ == "__main__":
    sys.exit(main())
