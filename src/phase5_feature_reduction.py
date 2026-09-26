"""Phase 5, étape 2 : réduction du nombre de variables.

La table large complète est réduite aux 8 capteurs stables sélectionnés en
étape 1 et à 5 statistiques par capteur. Les variables calendaires utiles au
modèle sont conservées.

Cette étape prépare une table réduite ; elle ne réentraîne pas encore KMeans.
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

from phase1_eda import ensure_java_home
from phase2_cleaning import ensure_hadoop_windows


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "features_5min" / "window_features_wide"
DEFAULT_SELECTION_FILE = PROJECT_ROOT / "outputs" / "phase5_sensor_selection" / "stable_sensor_selection.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "features_5min" / "window_features_reduced"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "outputs" / "phase5_feature_reduction"

RETAINED_SUFFIXES = (
    "sample_count",
    "mean_value",
    "max_value",
    "positive_value_count",
    "zero_value_count",
)
DROPPED_SUFFIXES = ("min_value", "last_value")


def log(message: str) -> None:
    print(f"[PHASE 5.2] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Réduction des variables du clustering Smart Home."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--selection-file", type=Path, default=DEFAULT_SELECTION_FILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
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
    return args


def create_spark(args: argparse.Namespace) -> SparkSession:
    ensure_java_home("PHASE 5.2")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")
    scratch = (
        args.scratch_dir.expanduser().resolve()
        if args.scratch_dir
        else Path(os.environ.get("TEMP", str(PROJECT_ROOT))) / "smart-home-phase5-2"
    )
    (scratch / "scratch").mkdir(parents=True, exist_ok=True)
    ensure_hadoop_windows(scratch, args.hadoop_home, "PHASE 5.2")

    spark = (
        SparkSession.builder.appName("SmartHome-Phase5-Feature-Reduction")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.local.dir", str(scratch / "scratch"))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


def read_selected_sensors(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise FileNotFoundError(
            f"Fichier de sélection introuvable : {path}. Exécutez d'abord phase5_sensor_selection.py."
        )
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    selected = [
        row
        for row in rows
        if row.get("selected", "").strip().lower() == "true"
    ]
    if not selected:
        raise ValueError(f"Aucun capteur sélectionné dans {path}.")
    required = {"feature_key", "sensor_name", "room", "sensor_type"}
    if not required.issubset(selected[0]):
        raise ValueError(f"Colonnes manquantes dans {path} : {required}")
    return selected


def add_calendar_features(data: DataFrame) -> DataFrame:
    return (
        data.withColumn("is_weekend", F.col("is_weekend").cast("double"))
        .withColumn(
            "hour_sin",
            F.sin(F.lit(2.0 * 3.141592653589793) * F.col("hour") / F.lit(24.0)),
        )
        .withColumn(
            "hour_cos",
            F.cos(F.lit(2.0 * 3.141592653589793) * F.col("hour") / F.lit(24.0)),
        )
        .withColumn(
            "day_of_week_sin",
            F.sin(F.lit(2.0 * 3.141592653589793) * F.col("day_of_week") / F.lit(7.0)),
        )
        .withColumn(
            "day_of_week_cos",
            F.cos(F.lit(2.0 * 3.141592653589793) * F.col("day_of_week") / F.lit(7.0)),
        )
    )


def build_reduced_table(
    data: DataFrame,
    selected_sensors: list[dict[str, str]],
) -> tuple[DataFrame, list[str]]:
    sensor_columns: list[str] = []
    dictionary_rows: list[dict[str, Any]] = []
    for sensor in selected_sensors:
        feature_key = sensor["feature_key"]
        for suffix in RETAINED_SUFFIXES:
            column = f"{feature_key}_{suffix}"
            if column not in data.columns:
                raise ValueError(f"Colonne absente de la table large : {column}")
            sensor_columns.append(column)
            dictionary_rows.append(
                {
                    "column": column,
                    "feature_key": feature_key,
                    "sensor_name": sensor["sensor_name"],
                    "room": sensor["room"],
                    "sensor_type": sensor["sensor_type"],
                    "feature": suffix,
                    "decision": "conservée",
                    "reason": "statistique utile et disponible pour le profil de cinq minutes",
                }
            )
        for suffix in DROPPED_SUFFIXES:
            column = f"{feature_key}_{suffix}"
            if column in data.columns:
                dictionary_rows.append(
                    {
                        "column": column,
                        "feature_key": feature_key,
                        "sensor_name": sensor["sensor_name"],
                        "room": sensor["room"],
                        "sensor_type": sensor["sensor_type"],
                        "feature": suffix,
                        "decision": "supprimée",
                        "reason": "redondante avec mean_value/max_value ou last_value",
                    }
                )

    calendar_columns = [
        "window_start",
        "window_end",
        "hour",
        "day_of_week",
        "is_weekend",
        "active_sensor_count",
        "total_sample_count",
        "hour_sin",
        "hour_cos",
        "day_of_week_sin",
        "day_of_week_cos",
    ]
    required_columns = calendar_columns + sensor_columns
    missing = [column for column in required_columns if column not in data.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans la table large : {missing}")

    reduced = data.select(*required_columns)
    model_features = sensor_columns + [
        "active_sensor_count",
        "total_sample_count",
        "hour_sin",
        "hour_cos",
        "day_of_week_sin",
        "day_of_week_cos",
        "is_weekend",
    ]
    for column in model_features:
        if column not in dictionary_rows:
            dictionary_rows.append(
                {
                    "column": column,
                    "feature_key": "context",
                    "sensor_name": "contexte global",
                    "room": "global",
                    "sensor_type": "CONTEXT",
                    "feature": column,
                    "decision": "conservée",
                    "reason": "variable de contexte temporel ou de disponibilité",
                }
            )
    return reduced, model_features


def write_dictionary(rows: list[dict[str, Any]], path: Path) -> None:
    fields = [
        "column",
        "feature_key",
        "sensor_name",
        "room",
        "sensor_type",
        "feature",
        "decision",
        "reason",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    report_dir: Path,
    args: argparse.Namespace,
    selected_sensors: list[dict[str, str]],
    input_columns: int,
    input_model_features: int,
    reduced_rows: int,
    reduced_columns: int,
    model_features: list[str],
    started_at: float,
) -> None:
    reduced_model_features = len(model_features)
    removed_model_features = input_model_features - reduced_model_features
    reduction = removed_model_features / input_model_features if input_model_features else 0
    report_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "phase": "phase5_step2_feature_reduction",
        "started_at_utc": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_dir": str(args.input_dir.resolve()),
        "selection_file": str(args.selection_file.resolve()),
        "output_dir": str(args.output_dir.resolve()),
        "input_column_count": input_columns,
        "input_model_feature_count": input_model_features,
        "selected_sensor_count": len(selected_sensors),
        "retained_suffixes": list(RETAINED_SUFFIXES),
        "dropped_suffixes": list(DROPPED_SUFFIXES),
        "reduced_model_feature_count": reduced_model_features,
        "removed_model_feature_count": removed_model_features,
        "reduction_ratio": round(reduction, 6),
        "reduced_row_count": reduced_rows,
        "reduced_column_count": reduced_columns,
    }
    (report_dir / "feature_reduction_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    selected_list = "\n".join(f"- `{row['sensor_name']}`" for row in selected_sensors)
    report = f"""# Phase 5 — Étape 2 : réduction des variables

## Démarche

1. Utiliser les 8 capteurs stables sélectionnés en étape 1.
2. Conserver 5 statistiques par capteur :
   - `sample_count` ;
   - `mean_value` ;
   - `max_value` ;
   - `positive_value_count` ;
   - `zero_value_count`.
3. Supprimer `min_value` et `last_value`, considérés redondants pour une fenêtre de cinq minutes.
4. Conserver les variables de contexte : nombre de capteurs, nombre de mesures, heure cyclique et jour cyclique.

## Capteurs conservés

{selected_list}

## Résultat

- Colonnes de la table large originale : **{input_columns}**
- Variables du modèle originales : **{input_model_features}**
- Capteurs utilisés : **{len(selected_sensors)}**
- Statistiques conservées par capteur : **{len(RETAINED_SUFFIXES)}**
- Variables du modèle réduites : **{reduced_model_features}**
- Variables supprimées : **{removed_model_features}**
- Réduction : **{reduction:.2%}**
- Lignes conservées : **{reduced_rows:,}**
- Colonnes de la table réduite : **{reduced_columns}**

## Interprétation

La réduction diminue le nombre de variables de **{input_model_features}** à **{reduced_model_features}**. Elle supprime principalement les capteurs disponibles seulement sur une partie de la période et les statistiques redondantes.

Cette opération devrait réduire la variance du modèle et améliorer sa stabilité temporelle. Elle ne garantit cependant pas à elle seule une silhouette positive : la prochaine étape devra réentraîner et comparer les modèles.

## Sorties

- Table Parquet : `{args.output_dir.resolve()}`
- Dictionnaire : `{report_dir / 'reduced_feature_dictionary.csv'}`
- Résumé : `{report_dir / 'feature_reduction_summary.json'}`
"""
    (report_dir / "feature_reduction_report.md").write_text(report, encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    started_at = time.time()
    input_dir = args.input_dir.expanduser().resolve()
    selection_file = args.selection_file.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    report_dir = args.report_dir.expanduser().resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"Table large introuvable : {input_dir}. Exécutez d'abord phase3_features.py."
        )
    selected_sensors = read_selected_sensors(selection_file)
    log(
        f"Capteurs stables reçus : {len(selected_sensors)} "
        f"(seuil défini dans {selection_file})."
    )

    spark = create_spark(args)
    data: DataFrame | None = None
    reduced: DataFrame | None = None
    try:
        log("Lecture de la table large.")
        data = spark.read.parquet(str(input_dir))
        input_columns = len(data.columns)
        data = add_calendar_features(data)
        original_sensor_columns = [
            column
            for column in data.columns
            if column not in {
                "window_start",
                "window_end",
                "hour",
                "day_of_week",
                "is_weekend",
                "active_sensor_count",
                "total_sample_count",
                "total_suspect_count",
            }
            and any(
                column.endswith(f"_{suffix}")
                for suffix in RETAINED_SUFFIXES + DROPPED_SUFFIXES
            )
        ]
        input_model_features = len(original_sensor_columns) + 7

        reduced, model_features = build_reduced_table(data, selected_sensors)
        reduced = reduced.persist(StorageLevel.DISK_ONLY)
        reduced_rows = reduced.count()
        reduced_columns = len(reduced.columns)
        log(
            f"Table réduite : {reduced_rows:,} lignes, "
            f"{len(model_features)} variables de modèle."
        )

        output_dir.mkdir(parents=True, exist_ok=True)
        reduced.write.mode("overwrite").option("compression", "snappy").parquet(
            str(output_dir)
        )
        log(f"Table réduite écrite dans {output_dir}")

        report_dir.mkdir(parents=True, exist_ok=True)
        dictionary_rows: list[dict[str, Any]] = []
        for sensor in selected_sensors:
            for suffix in RETAINED_SUFFIXES:
                dictionary_rows.append(
                    {
                        "column": f"{sensor['feature_key']}_{suffix}",
                        "feature_key": sensor["feature_key"],
                        "sensor_name": sensor["sensor_name"],
                        "room": sensor["room"],
                        "sensor_type": sensor["sensor_type"],
                        "feature": suffix,
                        "decision": "conservée",
                        "reason": "statistique utile et disponible pour le profil de cinq minutes",
                    }
                )
            for suffix in DROPPED_SUFFIXES:
                dictionary_rows.append(
                    {
                        "column": f"{sensor['feature_key']}_{suffix}",
                        "feature_key": sensor["feature_key"],
                        "sensor_name": sensor["sensor_name"],
                        "room": sensor["room"],
                        "sensor_type": sensor["sensor_type"],
                        "feature": suffix,
                        "decision": "supprimée",
                        "reason": "redondante avec mean_value/max_value ou last_value",
                    }
                )
        for column in model_features:
            if not column.startswith(next(sensor["feature_key"] for sensor in selected_sensors)):
                dictionary_rows.append(
                    {
                        "column": column,
                        "feature_key": "context",
                        "sensor_name": "contexte global",
                        "room": "global",
                        "sensor_type": "CONTEXT",
                        "feature": column,
                        "decision": "conservée",
                        "reason": "variable de contexte temporel ou de disponibilité",
                    }
                )
        write_dictionary(dictionary_rows, report_dir / "reduced_feature_dictionary.csv")
        write_report(
            report_dir,
            args,
            selected_sensors,
            input_columns,
            input_model_features,
            reduced_rows,
            reduced_columns,
            model_features,
            started_at,
        )
        log(f"Rapport : {report_dir / 'feature_reduction_report.md'}")
    finally:
        if reduced is not None:
            reduced.unpersist()
        if data is not None:
            data.unpersist()
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
