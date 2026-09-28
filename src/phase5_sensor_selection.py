"""Phase 5, étape 1 : sélection des capteurs stables.

La sélection repose sur la couverture réelle des fenêtres de cinq minutes :
un capteur est stable s'il apparaît dans au moins 95 % des fenêtres
observées dans la table longue. Cette étape ne modifie pas le clustering.
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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from phase1_eda import ensure_java_home
from phase2_cleaning import ensure_hadoop_windows


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "features_5min" / "sensor_features_long"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase5_sensor_selection"


def log(message: str) -> None:
    print(f"[PHASE 5.1] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sélection des capteurs stables pour la phase 5."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument(
        "--min-window-coverage",
        type=float,
        default=0.95,
        help="Part minimale des fenêtres observées contenant le capteur.",
    )
    parser.add_argument("--scratch-dir", type=Path, default=None)
    parser.add_argument(
        "--hadoop-home",
        type=Path,
        default=None,
        help="Dossier Hadoop contenant bin/winutils.exe et bin/hadoop.dll sous Windows.",
    )
    parser.add_argument("--log-level", default="WARN")
    args = parser.parse_args()

    if not 0 < args.min_window_coverage <= 1:
        parser.error("--min-window-coverage doit être compris entre 0 et 1.")
    is_local = args.master == "local" or (
        args.master.startswith("local[") and args.master.endswith("]")
    )
    if not is_local:
        parser.error("Cette phase lit des chemins locaux : utilisez --master local[N].")
    return args


def create_spark(args: argparse.Namespace) -> SparkSession:
    ensure_java_home("PHASE 5.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")
    scratch = (
        args.scratch_dir.expanduser().resolve()
        if args.scratch_dir
        else Path(os.environ.get("TEMP", str(PROJECT_ROOT))) / "smart-home-phase5"
    )
    (scratch / "scratch").mkdir(parents=True, exist_ok=True)
    ensure_hadoop_windows(scratch, args.hadoop_home, "PHASE 5.1")

    spark = (
        SparkSession.builder.appName("SmartHome-Phase5-Sensor-Selection")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.maxResultSize", "128m")
        .config("spark.local.dir", str(scratch / "scratch"))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


def sensor_coverage(long_features: DataFrame, threshold: float) -> tuple[DataFrame, dict[str, Any]]:
    global_row = long_features.agg(
        F.countDistinct("window_start").alias("global_window_count"),
        F.min("window_start").alias("first_window"),
        F.max("window_start").alias("last_window"),
    ).first()
    global_window_count = int(global_row["global_window_count"])
    if global_window_count == 0:
        raise RuntimeError("La table longue ne contient aucune fenêtre.")

    global_start = global_row["first_window"]
    global_end = global_row["last_window"]
    global_span_micros = int(
        (global_end - global_start).total_seconds() * 1_000_000
    )

    per_sensor = long_features.groupBy(
        "sensor_id", "feature_key", "sensor_name", "room", "sensor_type"
    ).agg(
        F.countDistinct("window_start").alias("observed_window_count"),
        F.min("window_start").alias("first_observed_window"),
        F.max("window_start").alias("last_observed_window"),
    )
    coverage = per_sensor.withColumn(
        "window_coverage",
        F.round(F.col("observed_window_count") / F.lit(global_window_count), 8),
    ).withColumn(
        "observation_span_ratio",
        F.round(
            F.when(
                F.col("first_observed_window").isNotNull()
                & F.col("last_observed_window").isNotNull(),
                (
                    F.unix_micros(F.col("last_observed_window"))
                    - F.unix_micros(F.col("first_observed_window"))
                )
                / F.lit(max(global_span_micros, 1)),
            ).otherwise(F.lit(None).cast("double")),
            8,
        ),
    ).withColumn(
        "selected", F.col("window_coverage") >= F.lit(threshold)
    ).orderBy(
        F.desc("selected"), F.desc("window_coverage"), "sensor_id"
    )

    global_info = {
        "global_window_count": global_window_count,
        "first_window": global_start.isoformat(sep=" "),
        "last_window": global_end.isoformat(sep=" "),
        "global_span_days": round(global_span_micros / 86_400_000_000, 4),
        "min_window_coverage": threshold,
    }
    return coverage, global_info


def write_csv(rows: list[dict[str, Any]], path: Path, fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_outputs(
    output_dir: Path,
    coverage: DataFrame,
    global_info: dict[str, Any],
    args: argparse.Namespace,
    started_at: float,
) -> None:
    rows = [row.asDict(recursive=True) for row in coverage.collect()]
    fields = [
        "sensor_id",
        "feature_key",
        "sensor_name",
        "room",
        "sensor_type",
        "observed_window_count",
        "global_window_count",
        "window_coverage",
        "observation_span_ratio",
        "first_observed_window",
        "last_observed_window",
        "selected",
    ]
    for row in rows:
        row["global_window_count"] = global_info["global_window_count"]
        for key in ("first_observed_window", "last_observed_window"):
            value = row[key]
            row[key] = value.isoformat(sep=" ") if value is not None else ""
    write_csv(rows, output_dir / "stable_sensor_selection.csv", fields)

    selected = [row for row in rows if row["selected"]]
    selected_names = [str(row["sensor_name"]) for row in selected]
    (output_dir / "stable_sensor_names.txt").write_text(
        "\n".join(selected_names) + ("\n" if selected_names else ""),
        encoding="utf-8",
    )
    summary = {
        "phase": "phase5_step1_sensor_selection",
        "started_at_utc": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_dir": str(args.input_dir.resolve()),
        "output_dir": str(output_dir.resolve()),
        **global_info,
        "sensor_count": len(rows),
        "selected_sensor_count": len(selected),
        "selected_sensor_names": selected_names,
    }
    (output_dir / "sensor_selection_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    selection_table = "\n".join(
        f"| {row['sensor_name']} | {row['room']} | {row['sensor_type']} | "
        f"{row['observed_window_count']:,} | {row['window_coverage']:.2%} | "
        f"{'Oui' if row['selected'] else 'Non'} |"
        for row in rows
    )
    report = f"""# Phase 5 — Étape 1 : sélection des capteurs stables

## Démarche

1. Lecture de la table longue produite par la phase 3.
2. Calcul du nombre total de fenêtres observées : **{global_info['global_window_count']:,}**.
3. Calcul, pour chaque capteur, du nombre de fenêtres dans lesquelles il apparaît.
4. Calcul de la couverture : `observed_window_count / global_window_count`.
5. Sélection des capteurs dont la couverture est au moins **{args.min_window_coverage:.0%}**.

## Résultat

- Capteurs analysés : **{len(rows)}**
- Capteurs stables sélectionnés : **{len(selected)}**
- Seuil de couverture : **{args.min_window_coverage:.0%}**
- Première fenêtre : `{global_info['first_window']}`
- Dernière fenêtre : `{global_info['last_window']}`

| Capteur | Pièce | Type | Fenêtres observées | Couverture | Sélection |
| --- | --- | --- | ---: | ---: | --- |
{selection_table}

## Interprétation

La couverture mesure la présence réelle d'un capteur dans les fenêtres observées. Elle est plus informative qu'un simple ratio entre la première et la dernière mesure, car un capteur peut avoir une longue période sans données.

Les capteurs non sélectionnés restent disponibles pour une analyse descriptive, mais ne seront pas utilisés dans la première version du modèle de clustering. Cette sélection réduit le biais lié à l'activation progressive des capteurs et devrait améliorer la stabilité temporelle du modèle.

## Sorties

- `{output_dir / 'stable_sensor_selection.csv'}`
- `{output_dir / 'stable_sensor_names.txt'}`
- `{output_dir / 'sensor_selection_summary.json'}`
"""
    (output_dir / "sensor_selection_report.md").write_text(report, encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    started_at = time.time()
    input_dir = args.input_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"Table longue introuvable : {input_dir}. Exécutez d'abord phase3_features.py."
        )

    spark = create_spark(args)
    data: DataFrame | None = None
    try:
        log("Lecture de la table longue des caractéristiques.")
        data = spark.read.parquet(str(input_dir))
        log(f"Calcul de la couverture des capteurs avec un seuil de {args.min_window_coverage:.0%}.")
        coverage, global_info = sensor_coverage(data, args.min_window_coverage)
        write_outputs(output_dir, coverage, global_info, args, started_at)
        summary = json.loads(
            (output_dir / "sensor_selection_summary.json").read_text(encoding="utf-8")
        )
        log(
            f"Capteurs stables : {summary['selected_sensor_count']} "
            f"/ {summary['sensor_count']}"
        )
        log(f"Rapport : {output_dir / 'sensor_selection_report.md'}")
    finally:
        if data is not None:
            data.unpersist()
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
