"""Phase 3 : construction des caractéristiques temporelles.

Le script lit les mesures nettoyées, les regroupe par fenêtre de cinq minutes,
calcule des statistiques par capteur, puis produit :
- une table longue, utile pour l'analyse et le contrôle ;
- une table large, prête pour les modèles Spark ML.

Les données ne sont pas encore standardisées ici : cette opération appartient
au pipeline ML et doit être ajustée sur les données d'entraînement uniquement.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from phase1_eda import ensure_java_home
from phase2_cleaning import ensure_hadoop_windows


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "measurements_clean"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "features_5min"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "outputs" / "phase3_features"

FEATURE_NAMES = (
    "sample_count",
    "mean_value",
    "min_value",
    "max_value",
    "stddev_value",
    "last_value",
    "positive_value_count",
    "zero_value_count",
    "suspect_value_count",
)


def log(message: str) -> None:
    print(f"[PHASE 3] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Construction de features temporelles Smart Home avec PySpark."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--window", default="5 minutes")
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--scratch-dir", type=Path, default=None)
    parser.add_argument(
        "--hadoop-home",
        type=Path,
        default=None,
        help="Dossier Hadoop contenant bin/winutils.exe et bin/hadoop.dll sous Windows.",
    )
    parser.add_argument("--log-level", default="WARN")
    args = parser.parse_args()

    is_local = args.master == "local" or (
        args.master.startswith("local[") and args.master.endswith("]")
    )
    if not is_local:
        parser.error("Cette phase lit des chemins locaux : utilisez --master local[N].")
    if args.shuffle_partitions < 1:
        parser.error("--shuffle-partitions doit être supérieur à 0.")
    return args


def create_spark(args: argparse.Namespace) -> SparkSession:
    ensure_java_home("PHASE 3")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")

    scratch = (
        args.scratch_dir.expanduser().resolve()
        if args.scratch_dir
        else Path(os.environ.get("TEMP", str(PROJECT_ROOT))) / "smart-home-phase3"
    )
    (scratch / "scratch").mkdir(parents=True, exist_ok=True)
    ensure_hadoop_windows(scratch, args.hadoop_home, "PHASE 3")

    spark = (
        SparkSession.builder.appName("SmartHome-Phase3-Features")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.local.dir", str(scratch / "scratch"))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


def usable_value() -> Any:
    """Exclut seulement les valeurs explicitement marquées suspectes."""
    return F.when(F.col("is_suspect_value"), F.lit(None).cast("double")).otherwise(
        F.col("value")
    )


def add_window(data: DataFrame, window: str) -> DataFrame:
    with_window = data.withColumn("sensor_window", F.window("timestamp", window))
    return (
        with_window.withColumn("window_start", F.col("sensor_window.start"))
        .withColumn("window_end", F.col("sensor_window.end"))
        .withColumn("feature_key", F.regexp_replace(F.lower("name"), "[^a-z0-9]+", "_"))
        .withColumn("usable_value", usable_value())
    )


def build_long_features(data: DataFrame, window: str) -> DataFrame:
    windowed = add_window(data, window)
    return (
        windowed.groupBy(
            "window_start",
            "window_end",
            "sensor_id",
            "feature_key",
        )
        .agg(
            F.first("name").alias("sensor_name"),
            F.first("room").alias("room"),
            F.first("device_or_area").alias("device_or_area"),
            F.first("measurement").alias("measurement"),
            F.first("sensor_type").alias("sensor_type"),
            F.first("source_type").alias("source_type"),
            F.count(F.lit(1)).alias("raw_event_count"),
            F.count("usable_value").alias("sample_count"),
            F.avg("usable_value").alias("mean_value"),
            F.min("usable_value").alias("min_value"),
            F.max("usable_value").alias("max_value"),
            F.stddev_pop("usable_value").alias("stddev_value"),
            F.expr("max_by(usable_value, timestamp)").cast("double").alias(
                "last_value"
            ),
            F.sum(
                F.when(F.col("usable_value") > 0, 1).otherwise(0)
            ).alias("positive_value_count"),
            F.sum(
                F.when(F.col("usable_value") == 0, 1).otherwise(0)
            ).alias("zero_value_count"),
            F.sum(
                F.when(F.col("is_suspect_value"), 1).otherwise(0)
            ).alias("suspect_value_count"),
        )
        .select(
            "window_start",
            "window_end",
            "sensor_id",
            "feature_key",
            "sensor_name",
            "room",
            "device_or_area",
            "measurement",
            "sensor_type",
            "source_type",
            *FEATURE_NAMES,
            "raw_event_count",
        )
    )


def build_wide_features(long_features: DataFrame) -> DataFrame:
    aggregations = []
    for feature_name in FEATURE_NAMES:
        if feature_name == "sample_count":
            expression = F.coalesce(
                F.first(feature_name, ignorenulls=True), F.lit(0)
            )
        else:
            expression = F.first(feature_name, ignorenulls=True)
        aggregations.append(expression.alias(feature_name))

    wide = long_features.groupBy("window_start", "window_end").pivot(
        "feature_key"
    ).agg(*aggregations)

    window_stats = long_features.groupBy("window_start", "window_end").agg(
        F.countDistinct("sensor_id").alias("active_sensor_count"),
        F.sum("sample_count").alias("total_sample_count"),
        F.sum("suspect_value_count").alias("total_suspect_count"),
    )
    wide = wide.join(window_stats, on=["window_start", "window_end"], how="left")
    return (
        wide.withColumn("hour", F.hour("window_start"))
        .withColumn("day_of_week", F.dayofweek("window_start"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("window_start").isin(1, 7), True).otherwise(False),
        )
        .orderBy("window_start")
    )


def write_feature_dictionary(
    long_features: DataFrame, wide: DataFrame, report_dir: Path
) -> None:
    sensor_rows = long_features.select(
        "feature_key", "sensor_name", "room", "measurement", "sensor_type"
    ).dropDuplicates().orderBy("feature_key").collect()
    wide_columns = set(wide.columns)
    rows: list[dict[str, Any]] = []
    for sensor in sensor_rows:
        for feature_name in FEATURE_NAMES:
            column_name = f"{sensor['feature_key']}_{feature_name}"
            if column_name in wide_columns:
                rows.append(
                    {
                        "column": column_name,
                        "feature_key": sensor["feature_key"],
                        "sensor_name": sensor["sensor_name"],
                        "room": sensor["room"],
                        "measurement": sensor["measurement"],
                        "sensor_type": sensor["sensor_type"],
                        "feature": feature_name,
                    }
                )

    report_dir.mkdir(parents=True, exist_ok=True)
    with (report_dir / "feature_dictionary.csv").open(
        "w", encoding="utf-8-sig", newline=""
    ) as handle:
        fields = [
            "column",
            "feature_key",
            "sensor_name",
            "room",
            "measurement",
            "sensor_type",
            "feature",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    report_dir: Path,
    args: argparse.Namespace,
    long_features: DataFrame,
    wide: DataFrame,
    started_at: float,
) -> None:
    long_count = long_features.count()
    long_summary = long_features.agg(
        F.countDistinct("window_start").alias("window_count"),
        F.countDistinct("sensor_id").alias("sensor_count"),
        F.min("window_start").alias("first_window"),
        F.max("window_start").alias("last_window"),
        F.sum("sample_count").alias("usable_value_count"),
        F.sum("suspect_value_count").alias("suspect_value_count"),
    ).first()
    wide_count = wide.count()
    wide_feature_count = len(wide.columns)

    summary = {
        "phase": "phase3_features",
        "started_at_utc": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_dir": str(args.input_dir.resolve()),
        "output_dir": str(args.output_dir.resolve()),
        "window": args.window,
        "long_row_count": long_count,
        "window_count": int(long_summary["window_count"]),
        "sensor_count": int(long_summary["sensor_count"]),
        "first_window": str(long_summary["first_window"]),
        "last_window": str(long_summary["last_window"]),
        "usable_value_count": int(long_summary["usable_value_count"]),
        "suspect_value_count": int(long_summary["suspect_value_count"]),
        "wide_row_count": wide_count,
        "wide_column_count": wide_feature_count,
    }
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "feature_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    report = f"""# Phase 3 — Construction des caractéristiques temporelles

## Démarche

1. Lecture des mesures nettoyées en Parquet.
2. Regroupement des événements par fenêtre de **{args.window}** et par capteur.
3. Calcul des statistiques par capteur.
4. Exclusion des six valeurs marquées `is_suspect_value` des statistiques de valeur.
5. Conservation des zéros et des informations de fréquence.
6. Passage d'une table longue à une table large pour les modèles Spark ML.

## Statistiques calculées par capteur et par fenêtre

- `sample_count` : nombre de valeurs utilisables ;
- `mean_value`, `min_value`, `max_value`, `stddev_value` ;
- `last_value` : dernière valeur de la fenêtre ;
- `positive_value_count` et `zero_value_count` ;
- `suspect_value_count` : valeurs signalées comme suspectes.

## Résultat

- Lignes dans la table longue : **{long_count:,}**
- Fenelles temporelles : **{long_summary['window_count']:,}**
- Capteurs utilisés : **{long_summary['sensor_count']}**
- Lignes dans la table large : **{wide_count:,}**
- Colonnes de la table large : **{wide_feature_count}**
- Valeurs suspectes exclues des statistiques : **{long_summary['suspect_value_count']:,}**

La table large contient une ligne par fenêtre observée (fenêtre contenant au moins un événement), des variables temporelles (`hour`, `day_of_week`, `is_weekend`) et une colonne par statistique de capteur. Les valeurs manquantes d'un capteur dans une fenêtre restent à `null` afin de distinguer l'absence de mesures d'une valeur réellement nulle.

## Sorties

```text
{(args.output_dir / 'sensor_features_long').resolve()}
{(args.output_dir / 'window_features_wide').resolve()}
{report_dir / 'feature_dictionary.csv'}
```

## Limites

Cette phase construit les variables mais ne standardise pas encore les données. La standardisation devra être fitted uniquement sur la période d'entraînement du futur modèle pour éviter la fuite de données.
"""
    (report_dir / "feature_report.md").write_text(report, encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    started_at = time.time()
    input_dir = args.input_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    report_dir = args.report_dir.expanduser().resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"Dataset nettoyé introuvable : {input_dir}. Exécutez d'abord phase2_cleaning.py."
        )

    spark = create_spark(args)
    data: DataFrame | None = None
    long_features: DataFrame | None = None
    wide: DataFrame | None = None
    try:
        log("Lecture des mesures nettoyées en Parquet.")
        data = spark.read.parquet(str(input_dir))

        log(f"Regroupement par fenêtre de {args.window} et par capteur.")
        long_features = build_long_features(data, args.window).persist(
            StorageLevel.DISK_ONLY
        )
        long_count = long_features.count()
        log(f"Lignes dans la table longue : {long_count:,}")

        log("Construction de la table large pour Spark ML.")
        wide = build_wide_features(long_features)
        wide_output = output_dir / "window_features_wide"
        long_output = output_dir / "sensor_features_long"
        wide_output.parent.mkdir(parents=True, exist_ok=True)
        long_output.parent.mkdir(parents=True, exist_ok=True)

        (
            long_features.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(long_output))
        )
        log(f"Table longue écrite dans {long_output}")

        (
            wide.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(wide_output))
        )
        log(f"Table large écrite dans {wide_output}")

        log("Création du dictionnaire et du rapport des features.")
        write_feature_dictionary(long_features, wide, report_dir)
        write_report(report_dir, args, long_features, wide, started_at)
        log(f"Rapport : {report_dir / 'feature_report.md'}")
    finally:
        if wide is not None:
            wide.unpersist()
        if long_features is not None:
            long_features.unpersist()
        if data is not None:
            data.unpersist()
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
