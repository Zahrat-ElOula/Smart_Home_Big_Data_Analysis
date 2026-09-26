"""Phase 2 : nettoyage léger et préparation des mesures Smart Home.

Objectifs :
- vérifier les en-têtes et les types des fichiers ;
- associer les mesures aux 24 capteurs de sensor.csv ;
- supprimer uniquement les lignes structurellement invalides ;
- conserver les valeurs suspectes mais les signaler ;
- enregistrer un dataset propre au format Parquet.

Le dataset brut n'est jamais modifié.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Réutilise la détection Java validée dans la phase 1.
from phase1_eda import ensure_java_home


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "human_activity_raw_sensor_data"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "measurements_clean"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "outputs" / "phase2_cleaning"

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
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"

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


def log(message: str) -> None:
    print(f"[PHASE 2] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Nettoyage léger et préparation des mesures Smart Home avec PySpark."
    )
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--scratch-dir", type=Path, default=None)
    parser.add_argument(
        "--hadoop-home",
        type=Path,
        default=None,
        help="Dossier contenant bin/winutils.exe et bin/hadoop.dll pour l'écriture Parquet sous Windows.",
    )
    parser.add_argument(
        "--deduplicate",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Supprime les doublons exacts ; opération coûteuse et désactivée par défaut.",
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


def check_headers(data_dir: Path) -> dict[str, Path]:
    data_dir = data_dir.expanduser().resolve()
    files: dict[str, Path] = {}

    for filename, expected in EXPECTED_HEADERS.items():
        path = data_dir / filename
        if not path.is_file():
            raise FileNotFoundError(f"Fichier manquant : {path}")

        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            header = next(csv.reader(handle))
        header = [value.strip() for value in header]
        if [value.lower() for value in header] != [value.lower() for value in expected]:
            raise ValueError(
                f"En-tête invalide pour {path}. "
                f"Attendu={expected}, observé={header}"
            )
        files[filename] = path
    return files


def finite_value() -> Any:
    value = F.col("value")
    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) != F.lit(float("inf")))


def ensure_hadoop_windows(
    scratch_dir: Path, requested_home: Path | None, log_prefix: str = "PHASE 2"
) -> None:
    """Configure les deux binaires nécessaires au Parquet local sous Windows."""
    if os.name != "nt":
        return

    program_files = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
    temp_root = Path(os.environ.get("TEMP", str(PROJECT_ROOT))) / "opencode"
    candidate_homes = [
        requested_home,
        Path(os.environ["HADOOP_HOME"]) if os.environ.get("HADOOP_HOME") else None,
        temp_root / "phase2_hadoop",
        program_files / "Hadoop",
    ]

    for candidate in candidate_homes:
        if candidate is None:
            continue
        home = candidate.expanduser().resolve()
        if (home / "bin" / "winutils.exe").is_file() and (
            home / "bin" / "hadoop.dll"
        ).is_file():
            os.environ["HADOOP_HOME"] = str(home)
            os.environ["Path"] = str(home / "bin") + os.pathsep + os.environ.get("Path", "")
            print(f"[{log_prefix}] HADOOP_HOME utilisé : {home}", flush=True)
            return

    if requested_home is not None:
        raise RuntimeError(
            f"--hadoop-home doit contenir bin/winutils.exe et bin/hadoop.dll : {requested_home}"
        )

    winutils_candidates = [
        program_files / "RStudio" / "resources" / "app" / "bin" / "winutils" / "x64" / "winutils.exe",
        program_files / "RStudio" / "resources" / "app" / "bin" / "winutils" / "winutils.exe",
        program_files / "Hadoop" / "bin" / "winutils.exe",
    ]
    dll_candidates = [
        temp_root / "phase2_hadoop" / "bin" / "hadoop.dll",
        program_files / "Hadoop" / "bin" / "hadoop.dll",
    ]
    winutils = next((path for path in winutils_candidates if path.is_file()), None)
    hadoop_dll = next((path for path in dll_candidates if path.is_file()), None)
    if winutils is None or hadoop_dll is None:
        raise RuntimeError(
            "Pour écrire un Parquet sous Windows, il faut winutils.exe et hadoop.dll. "
            "Installe Hadoop ou passe --hadoop-home avec un dossier contenant "
            "bin/winutils.exe et bin/hadoop.dll."
        )

    hadoop_home = scratch_dir / "hadoop"
    bin_dir = hadoop_home / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(winutils, bin_dir / "winutils.exe")
    shutil.copy2(hadoop_dll, bin_dir / "hadoop.dll")
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["Path"] = str(bin_dir) + os.pathsep + os.environ.get("Path", "")
    print(
        f"[{log_prefix}] HADOOP_HOME configuré automatiquement : {hadoop_home}",
        flush=True,
    )


def create_spark(args: argparse.Namespace) -> SparkSession:
    ensure_java_home("PHASE 2")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")

    scratch = (
        args.scratch_dir.expanduser().resolve()
        if args.scratch_dir
        else Path(os.environ.get("TEMP", str(PROJECT_ROOT))) / "smart-home-phase2"
    )
    (scratch / "scratch").mkdir(parents=True, exist_ok=True)
    ensure_hadoop_windows(scratch, args.hadoop_home)

    spark = (
        SparkSession.builder.appName("SmartHome-Phase2-Cleaning")
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


def read_csv(spark: SparkSession, path: Path, schema: T.StructType) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .option("enforceSchema", "false")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .option("timestampFormat", TIMESTAMP_FORMAT)
        .schema(schema)
        .csv(str(path))
    )


def load_sensor_metadata(spark: SparkSession, path: Path) -> DataFrame:
    metadata = (
        read_csv(spark, path, SENSOR_SCHEMA)
        .withColumn("sensor_id", F.col("sensor_id").cast("long"))
        .withColumn("node_id", F.col("node_id").cast("long"))
        .withColumn("sensor_type", F.upper(F.trim(F.col("type"))))
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("name_parts", F.split(F.col("name"), "/"))
        .withColumn("room", F.element_at(F.col("name_parts"), 1))
        .withColumn(
            "device_or_area",
            F.when(F.size(F.col("name_parts")) >= 2, F.element_at(F.col("name_parts"), 2)),
        )
        .withColumn(
            "measurement",
            F.element_at(F.col("name_parts"), F.size(F.col("name_parts"))),
        )
        .select("sensor_id", "node_id", "sensor_type", "name", "room", "device_or_area", "measurement")
        .filter(F.col("sensor_id").isNotNull())
        .dropDuplicates(["sensor_id"])
    )
    log(f"Capteurs déclarés après dédoublonnage : {metadata.count()}")
    return metadata


def load_measurements(spark: SparkSession, files: dict[str, Path]) -> DataFrame:
    measurements: list[DataFrame] = []
    for filename, source_type in (
        ("sensor_sample_int.csv", "INT"),
        ("sensor_sample_float.csv", "FLOAT"),
    ):
        measurements.append(
            read_csv(spark, files[filename], SAMPLE_SCHEMA)
            .withColumn("source_type", F.lit(source_type))
            .withColumn("source_file", F.lit(filename))
        )

    return measurements[0].select(
        "value_id",
        "sensor_id",
        "timestamp",
        "value",
        "source_type",
        "source_file",
        "_corrupt_record",
    ).unionByName(
        measurements[1].select(
            "value_id",
            "sensor_id",
            "timestamp",
            "value",
            "source_type",
            "source_file",
            "_corrupt_record",
        )
    )


def quality_metrics(dataframe: DataFrame) -> dict[str, Any]:
    finite = finite_value()
    corrupt = (
        F.col("_corrupt_record")
        if "_corrupt_record" in dataframe.columns
        else F.lit(None).cast("string")
    )
    row = dataframe.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.count("value_id").alias("value_id_count"),
        F.count("sensor_id").alias("sensor_id_count"),
        F.count("timestamp").alias("timestamp_count"),
        F.count(finite_value()).alias("finite_value_count"),
        F.sum(
            F.when(finite & (F.col("value") < 0), 1).otherwise(0)
        ).alias("negative_value_count"),
        F.sum(F.when(corrupt.isNotNull(), 1).otherwise(0)).alias(
            "corrupt_record_count"
        ),
    ).first()
    return {
        key: int(value or 0) for key, value in row.asDict().items()
    }


def build_clean_data(
    measurements: DataFrame,
    metadata: DataFrame,
) -> tuple[DataFrame, dict[str, Any]]:
    joined = measurements.join(
        F.broadcast(metadata),
        on="sensor_id",
        how="left",
    )
    joined = joined.withColumn("event_date", F.to_date("timestamp"))

    # Les valeurs négatives de courant sont conservées et signalées, pas supprimées.
    joined = joined.withColumn(
        "is_suspect_value",
        (F.col("measurement") == "current") & (F.col("value") < 0),
    )

    valid = (
        F.col("value_id").isNotNull()
        & F.col("sensor_id").isNotNull()
        & F.col("timestamp").isNotNull()
        & finite_value()
        & F.col("_corrupt_record").isNull()
        & F.col("name").isNotNull()
    )
    cleaned = joined.filter(valid).select(
        "value_id",
        "sensor_id",
        "node_id",
        "timestamp",
        "event_date",
        "value",
        "sensor_type",
        "source_type",
        "source_file",
        "name",
        "room",
        "device_or_area",
        "measurement",
        "is_suspect_value",
    )
    return cleaned, {
        "rule": "Conserver les valeurs et signaler les courants négatifs comme suspects.",
        "suspect_rule": "is_suspect_value = (measurement == 'current' and value < 0)",
    }


def write_clean_data(cleaned: DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (
        cleaned.write.mode("overwrite")
        .partitionBy("source_type")
        .option("compression", "snappy")
        .parquet(str(output_dir))
    )
    log(f"Dataset nettoyé écrit dans {output_dir}")


def write_summary(
    report_dir: Path,
    args: argparse.Namespace,
    before: dict[str, Any],
    after: dict[str, Any],
    rules: dict[str, Any],
    spark: SparkSession,
    started_at: float,
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    completed_at = time.time()
    summary = {
        "phase": "phase2_cleaning",
        "started_at_utc": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "completed_at_utc": datetime.fromtimestamp(completed_at, timezone.utc).isoformat(),
        "data_dir": str(args.data_dir.resolve()),
        "output_dir": str(args.output_dir.resolve()),
        "spark_version": spark.version,
        "master": args.master,
        "deduplicate": args.deduplicate,
        "before": before,
        "after": after,
        "removed_row_count": before["row_count"] - after["row_count"],
        "suspect_row_count": after["suspect_row_count"],
        "rules": rules,
    }
    (report_dir / "cleaning_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    report = f"""# Phase 2 — Nettoyage et préparation

## Résultat

- Lignes lues : **{before['row_count']:,}**
- Lignes conservées : **{after['row_count']:,}**
- Lignes supprimées : **{before['row_count'] - after['row_count']:,}**
- Valeurs suspectes signalées : **{after['suspect_row_count']:,}**
- Capteurs déclarés : **{after['metadata_sensor_count']}**
- Mesures sans capteur déclaré conservées : **{after['missing_sensor_count']:,}**

## Règles appliquées

1. Vérification des en-têtes CSV.
2. Conversion des types avec un schéma Spark explicite.
3. Jointure des mesures avec `sensor.csv` sur `sensor_id`.
4. Suppression des lignes sans identifiant, timestamp, valeur finie ou capteur déclaré.
5. Conservation des valeurs négatives de courant avec l'indicateur `is_suspect_value`.
6. Les zéros sont conservés : ils représentent un état inactif normal.
7. Les données brutes ne sont jamais modifiées.

## Résultat qualité après nettoyage

- Valeurs finies : **{after['finite_value_count']:,}**
- Enregistrements corrompus : **{after['corrupt_record_count']}**
- Valeurs négatives : **{after['negative_value_count']}**

## Format de sortie

Les données nettoyées sont enregistrées en Parquet, partitionnées par `source_type` :

```text
{args.output_dir.resolve()}
```

## Étape suivante

Ces données peuvent être regroupées en fenêtres de cinq minutes pour construire les variables utilisées par Spark ML.
"""
    (report_dir / "cleaning_report.md").write_text(report, encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    started_at = time.time()
    data_dir = args.data_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    report_dir = args.report_dir.expanduser().resolve()

    log("Vérification des fichiers et des en-têtes.")
    files = check_headers(data_dir)
    spark = create_spark(args)

    metadata: DataFrame | None = None
    measurements: DataFrame | None = None
    cleaned: DataFrame | None = None
    try:
        log("Lecture et normalisation de sensor.csv.")
        metadata = load_sensor_metadata(spark, files["sensor.csv"])

        log("Lecture des deux fichiers de mesures.")
        measurements = load_measurements(spark, files).persist(StorageLevel.DISK_ONLY)

        log("Calcul des métriques avant nettoyage.")
        before = quality_metrics(measurements)
        log(f"Lignes brutes : {before['row_count']:,}")

        log("Association aux capteurs et application des règles de nettoyage.")
        cleaned, rules = build_clean_data(measurements, metadata)

        if args.deduplicate:
            log("Suppression des doublons exacts.")
            cleaned = cleaned.dropDuplicates(
                ["value_id", "sensor_id", "timestamp", "value"]
            )
        else:
            log("Dédoublonnage exact désactivé : contrôle futur coûteux sur 247 millions de lignes.")

        cleaned = cleaned.persist(StorageLevel.DISK_ONLY)
        after = quality_metrics(cleaned)
        after["suspect_row_count"] = cleaned.filter(
            F.col("is_suspect_value")
        ).count()
        after["metadata_sensor_count"] = int(metadata.count())
        # Toutes les lignes conservées ont un capteur déclaré grâce au filtre valid.
        after["missing_sensor_count"] = 0
        log(
            f"Lignes conservées : {after['row_count']:,} ; "
            f"valeurs suspectes : {after['suspect_row_count']:,}"
        )

        write_clean_data(cleaned, output_dir)
        write_summary(report_dir, args, before, after, rules, spark, started_at)
        log(f"Rapport : {report_dir / 'cleaning_report.md'}")
    finally:
        if cleaned is not None:
            cleaned.unpersist()
        if measurements is not None:
            measurements.unpersist()
        if metadata is not None:
            metadata.unpersist()
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
