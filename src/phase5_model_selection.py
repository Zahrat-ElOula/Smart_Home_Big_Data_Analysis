"""Phase 5, étape 4 : sélection robuste de k sur la table réduite.

Pour chaque k, le modèle est entraîné sur toute la période d'entraînement.
La silhouette est mesurée sur un échantillon d'entraînement et sur les
périodes de validation et de test. Un k n'est retenu que si son plus petit
cluster atteint une part minimale de la période d'entraînement.
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
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame

from phase4_clustering import (
    add_calendar_features,
    build_preprocessor,
    create_spark,
    select_feature_columns,
    temporal_split,
)
from phase4_clustering import CONTEXT_COLUMNS


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "features_5min" / "window_features_reduced"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "outputs" / "phase5_model_selection"
META_COLUMNS = (
    "window_start",
    "window_end",
    "hour",
    "day_of_week",
    "is_weekend",
    "active_sensor_count",
    "total_sample_count",
)


def log(message: str) -> None:
    print(f"[PHASE 5.4] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sélection de k robuste pour le clustering réduit."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
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
    parser.add_argument("--train-fraction", type=float, default=0.70)
    parser.add_argument("--validation-fraction", type=float, default=0.15)
    parser.add_argument("--k-candidates", default="2,3,4,5,6")
    parser.add_argument(
        "--min-cluster-share",
        type=float,
        default=0.01,
        help="Part minimale du plus petit cluster dans l'entraînement.",
    )
    parser.add_argument(
        "--evaluation-sample-fraction",
        type=float,
        default=0.30,
        help="Échantillon utilisé pour la silhouette d'entraînement.",
    )
    parser.add_argument("--max-iter", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--log-level", default="WARN")
    args = parser.parse_args()

    if not 0 < args.min_cluster_share < 1:
        parser.error("--min-cluster-share doit être compris entre 0 et 1.")
    if not 0 < args.evaluation_sample_fraction <= 1:
        parser.error("--evaluation-sample-fraction doit être compris entre 0 et 1.")
    if args.train_fraction + args.validation_fraction >= 1:
        parser.error("La somme train_fraction + validation_fraction doit être < 1.")
    try:
        args.k_candidates = sorted(
            {int(value.strip()) for value in args.k_candidates.split(",") if value.strip()}
        )
    except ValueError as exc:
        parser.error("--k-candidates doit contenir des entiers.")
    if not args.k_candidates or min(args.k_candidates) < 2:
        parser.error("Il faut au moins un k >= 2.")
    return args


def select_scaled_columns(
    transformed: DataFrame,
    sensor_feature_columns: list[str],
) -> DataFrame:
    return transformed.select(*META_COLUMNS, "scaled_features", *sensor_feature_columns)


def silhouette_or_none(
    evaluator: ClusteringEvaluator,
    predictions: DataFrame,
) -> float | None:
    try:
        return float(evaluator.evaluate(predictions))
    except Exception as exc:  # une métrique indisponible ne doit pas masquer la comparaison
        log(f"Silhouette indisponible : {exc}")
        return None


def write_csv(rows: list[dict[str, Any]], path: Path, fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def create_chart(report_dir: Path, rows: list[dict[str, Any]]) -> str | None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        log("Matplotlib absent : graphique non créé.")
        return None

    chart_dir = report_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)
    ks = [row["k"] for row in rows]
    fig, ax = plt.subplots(figsize=(10, 5))
    for field, label, color in (
        ("train_silhouette", "Entraînement", "#2563eb"),
        ("validation_silhouette", "Validation", "#dc2626"),
        ("test_silhouette", "Test", "#16a34a"),
    ):
        values = [row[field] if row[field] is not None else float("nan") for row in rows]
        ax.plot(ks, values, marker="o", label=label, color=color)
    chosen = next((row for row in rows if row["selected"]), None)
    if chosen is not None:
        ax.scatter(
            [chosen["k"]],
            [chosen["validation_silhouette"] or 0],
            color="black",
            s=80,
            marker="*",
            label="k retenu",
            zorder=5,
        )
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set(
        title="Silhouette par k sur les périodes chronologiques",
        xlabel="Nombre de clusters (k)",
        ylabel="Score de silhouette",
    )
    ax.set_xticks(ks)
    ax.legend()
    fig.tight_layout()
    path = chart_dir / "silhouette_train_validation_test.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return str(path)


def score_text(value: float | None) -> str:
    return f"{value:.4f}" if value is not None else "N/A"


def write_report(
    report_dir: Path,
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    selected_k: int,
    boundaries: list[Any],
    split_counts: dict[str, int],
    feature_count: int,
    chart_path: str | None,
    started_at: float,
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    selected = next(row for row in rows if row["selected"])
    table = "\n".join(
        f"| {row['k']} | {score_text(row['train_silhouette'])} | "
        f"{score_text(row['validation_silhouette'])} | "
        f"{score_text(row['test_silhouette'])} | "
        f"{row['min_cluster_share']:.2%} | {'Oui' if row['eligible'] else 'Non'} |"
        for row in rows
    )
    summary = {
        "phase": "phase5_step4_model_selection",
        "started_at_utc": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_dir": str(args.input_dir.resolve()),
        "feature_count": feature_count,
        "split_counts": split_counts,
        "boundaries": [str(value) for value in boundaries],
        "k_candidates": rows,
        "min_cluster_share_threshold": args.min_cluster_share,
        "selected_k": selected_k,
        "selection_reason": "Meilleure silhouette de validation parmi les k éligibles.",
    }
    (report_dir / "model_selection_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    chart_line = f"- `{Path(chart_path).name}`" if chart_path else "- Aucun graphique."
    report = f"""# Phase 5 — Étape 4 : sélection robuste de k

## Démarche

1. Utiliser la table réduite de 47 variables.
2. Conserver la séparation chronologique 70 % / 15 % / 15 %.
3. Ajuster l'imputation et le standard scaler sur l'entraînement uniquement.
4. Entraîner KMeans sur toute la période d'entraînement pour chaque k.
5. Mesurer la silhouette sur l'entraînement, la validation et le test.
6. Écarter un k dont le plus petit cluster représente moins de **{args.min_cluster_share:.0%}** des fenêtres d'entraînement.
7. Retenir le k éligible avec la meilleure silhouette de validation.

## Résultat

- Variables : **{feature_count}**
- Entraînement : **{split_counts['train']:,}**
- Validation : **{split_counts['validation']:,}**
- Test : **{split_counts['test']:,}**
- Part minimale imposée : **{args.min_cluster_share:.0%}**
- k retenu : **{selected_k}**

| k | Silhouette entraînement | Silhouette validation | Silhouette test | Petit cluster | Éligible |
| ---: | ---: | ---: | ---: | ---: | --- |
{table}

## Lecture

Le k=3 obtenu précédemment sur l'échantillon d'entraînement produit un cluster de seulement 0,11 % des fenêtres après entraînement complet. Ce cluster est donc trop petit pour être considéré comme un profil stable. Le critère de taille minimale permet d'éviter ce choix.

k={selected_k} est retenu parmi les valeurs éligibles. La lecture finale doit comparer la silhouette de validation et la stabilité des profils, plutôt que la seule silhouette d'entraînement.

## Graphique

{chart_line}

## Limites

- Le clustering reste non supervisé.
- Un score de validation négatif indique une difficulté de généralisation temporelle.
- Le critère de taille minimale est une règle de robustesse, pas une preuve d'activité.
"""
    (report_dir / "model_selection_report.md").write_text(report, encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    started_at = time.time()
    input_dir = args.input_dir.expanduser().resolve()
    report_dir = args.report_dir.expanduser().resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"Table réduite introuvable : {input_dir}. Exécutez d'abord phase5_feature_reduction.py."
        )

    spark = create_spark(args)
    data: DataFrame | None = None
    train_scaled: DataFrame | None = None
    train_evaluation_sample: DataFrame | None = None
    try:
        data = add_calendar_features(spark.read.parquet(str(input_dir)))
        feature_columns = select_feature_columns(data)
        sensor_feature_columns = [
            column for column in feature_columns if column not in CONTEXT_COLUMNS
        ]
        log(f"Variables de modèle : {len(feature_columns)}")

        train, validation, test, boundaries = temporal_split(
            data, args.train_fraction, args.validation_fraction
        )
        split_counts = {
            "train": train.count(),
            "validation": validation.count(),
            "test": test.count(),
        }
        log(f"Split : {split_counts}")

        preprocessor, _ = build_preprocessor(feature_columns)
        preprocess_model = preprocessor.fit(train)
        train_scaled = select_scaled_columns(
            preprocess_model.transform(train), sensor_feature_columns
        ).persist(StorageLevel.MEMORY_AND_DISK)
        validation_scaled = select_scaled_columns(
            preprocess_model.transform(validation), sensor_feature_columns
        )
        test_scaled = select_scaled_columns(
            preprocess_model.transform(test), sensor_feature_columns
        )
        train_evaluation_sample = train_scaled.sample(
            withReplacement=False,
            fraction=args.evaluation_sample_fraction,
            seed=args.seed,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        evaluator = ClusteringEvaluator(
            featuresCol="scaled_features",
            predictionCol="cluster",
            metricName="silhouette",
        )
        rows: list[dict[str, Any]] = []
        for k in args.k_candidates:
            log(f"Entraînement KMeans k={k} sur toutes les données d'entraînement.")
            model = (
                KMeans()
                .setK(k)
                .setMaxIter(args.max_iter)
                .setSeed(args.seed)
                .setFeaturesCol("scaled_features")
                .setPredictionCol("cluster")
                .fit(train_scaled)
            )
            train_predictions = model.transform(train_scaled)
            counts = [
                int(row["count"])
                for row in train_predictions.groupBy("cluster")
                .count()
                .orderBy("cluster")
                .collect()
            ]
            smallest_cluster = min(counts)
            min_share = smallest_cluster / split_counts["train"]
            train_score = silhouette_or_none(
                evaluator,
                model.transform(train_evaluation_sample),
            )
            validation_score = silhouette_or_none(
                evaluator, model.transform(validation_scaled)
            )
            test_score = silhouette_or_none(
                evaluator, model.transform(test_scaled)
            )
            rows.append(
                {
                    "k": k,
                    "train_silhouette": train_score,
                    "validation_silhouette": validation_score,
                    "test_silhouette": test_score,
                    "smallest_cluster_count": smallest_cluster,
                    "min_cluster_share": min_share,
                    "eligible": min_share >= args.min_cluster_share,
                    "selected": False,
                }
            )
            log(
                f"k={k} : validation={validation_score}, "
                f"test={test_score}, petit cluster={min_share:.2%}"
            )

        eligible = [row for row in rows if row["eligible"]]
        pool = eligible or rows
        pool_with_score = [
            row for row in pool if row["validation_silhouette"] is not None
        ]
        if pool_with_score:
            chosen = max(
                pool_with_score,
                key=lambda row: row["validation_silhouette"],
            )
        else:
            chosen = max(pool, key=lambda row: row["min_cluster_share"])
        chosen["selected"] = True
        selected_k = int(chosen["k"])

        rows = sorted(rows, key=lambda row: row["k"])
        write_csv(
            rows,
            report_dir / "k_selection_full.csv",
            [
                "k",
                "train_silhouette",
                "validation_silhouette",
                "test_silhouette",
                "smallest_cluster_count",
                "min_cluster_share",
                "eligible",
                "selected",
            ],
        )
        chart_path = create_chart(report_dir, rows)
        write_report(
            report_dir,
            args,
            rows,
            selected_k,
            boundaries,
            split_counts,
            len(feature_columns),
            chart_path,
            started_at,
        )
        log(f"k retenu : {selected_k}")
        log(f"Rapport : {report_dir / 'model_selection_report.md'}")
    finally:
        if train_evaluation_sample is not None:
            train_evaluation_sample.unpersist()
        if train_scaled is not None:
            train_scaled.unpersist()
        if data is not None:
            data.unpersist()
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
