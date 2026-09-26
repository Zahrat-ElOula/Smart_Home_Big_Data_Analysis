"""Phase 4 : standardisation, séparation temporelle et clustering Spark ML.

Le script :
- lit la table large de la phase 3 ;
- sépare les fenêtres par ordre chronologique ;
- impute les valeurs manquantes sur l'entraînement uniquement ;
- standardise les variables ;
- choisit k par silhouette ;
- entraîne un modèle KMeans ;
- sauvegarde le modèle, les affectations et un rapport.

Le clustering est non supervisé : k est choisi comme une description statistique
des profils observés, pas comme une vérité terrain.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from phase1_eda import ensure_java_home
from phase2_cleaning import ensure_hadoop_windows


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "features_5min" / "window_features_wide"
DEFAULT_MODEL_DIR = PROJECT_ROOT / "models" / "phase4_kmeans"
DEFAULT_ASSIGNMENT_DIR = PROJECT_ROOT / "data" / "processed" / "clustering_5min"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "outputs" / "phase4_clustering"

SENSOR_SUFFIXES = (
    "sample_count",
    "mean_value",
    "min_value",
    "max_value",
    "last_value",
    "positive_value_count",
    "zero_value_count",
)
META_COLUMNS = {
    "window_start",
    "window_end",
    "hour",
    "day_of_week",
    "is_weekend",
    "active_sensor_count",
    "total_sample_count",
    "total_suspect_count",
}
CONTEXT_COLUMNS = {
    "active_sensor_count",
    "total_sample_count",
    "hour_sin",
    "hour_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "is_weekend",
}


def log(message: str) -> None:
    print(f"[PHASE 4] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Standardisation et clustering KMeans des fenêtres Smart Home."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--assignment-dir", type=Path, default=DEFAULT_ASSIGNMENT_DIR)
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
    parser.add_argument(
        "--k-candidates",
        default="2,3,4,5,6",
        help="Valeurs de k testées pour sélectionner le nombre de clusters.",
    )
    parser.add_argument("--selection-sample-fraction", type=float, default=0.30)
    parser.add_argument("--max-iter", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--log-level", default="WARN")
    args = parser.parse_args()

    is_local = args.master == "local" or (
        args.master.startswith("local[") and args.master.endswith("]")
    )
    if not is_local:
        parser.error("Cette phase lit des chemins locaux : utilisez --master local[N].")
    if not 0 < args.train_fraction < 1:
        parser.error("--train-fraction doit être compris entre 0 et 1.")
    if not 0 < args.validation_fraction < 1:
        parser.error("--validation-fraction doit être compris entre 0 et 1.")
    if args.train_fraction + args.validation_fraction >= 1:
        parser.error("La somme train_fraction + validation_fraction doit être < 1.")
    if not 0 < args.selection_sample_fraction <= 1:
        parser.error("--selection-sample-fraction doit être compris entre 0 et 1.")
    if args.max_iter < 1:
        parser.error("--max-iter doit être supérieur à 0.")
    try:
        args.k_candidates = sorted(
            {int(value.strip()) for value in args.k_candidates.split(",") if value.strip()}
        )
    except ValueError as exc:
        parser.error("--k-candidates doit contenir des entiers séparés par des virgules.")
    if not args.k_candidates or min(args.k_candidates) < 2:
        parser.error("Il faut au moins une valeur de k supérieure ou égale à 2.")
    return args


def create_spark(args: argparse.Namespace) -> SparkSession:
    ensure_java_home("PHASE 4")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")

    scratch = (
        args.scratch_dir.expanduser().resolve()
        if args.scratch_dir
        else Path(os.environ.get("TEMP", str(PROJECT_ROOT))) / "smart-home-phase4"
    )
    (scratch / "scratch").mkdir(parents=True, exist_ok=True)
    ensure_hadoop_windows(scratch, args.hadoop_home, "PHASE 4")

    spark = (
        SparkSession.builder.appName("SmartHome-Phase4-Clustering")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.local.dir", str(scratch / "scratch"))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


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


def select_feature_columns(data: DataFrame) -> list[str]:
    sensor_columns = sorted(
        column
        for column in data.columns
        if column not in META_COLUMNS
        and any(column.endswith(f"_{suffix}") for suffix in SENSOR_SUFFIXES)
    )
    if not sensor_columns:
        raise ValueError("Aucune colonne de capteur reconnue dans la table large.")
    return sensor_columns + [
        "active_sensor_count",
        "total_sample_count",
        "hour_sin",
        "hour_cos",
        "day_of_week_sin",
        "day_of_week_cos",
        "is_weekend",
    ]


def build_preprocessor(feature_columns: list[str]) -> tuple[Pipeline, list[str]]:
    imputed_columns = [f"imputed_{index}" for index in range(len(feature_columns))]
    imputer = Imputer(
        strategy="median",
        inputCols=feature_columns,
        outputCols=imputed_columns,
        missingValue=float("nan"),
    )
    assembler = VectorAssembler(
        inputCols=imputed_columns,
        outputCol="assembled_features",
        handleInvalid="skip",
    )
    scaler = StandardScaler(
        inputCol="assembled_features",
        outputCol="scaled_features",
        withMean=True,
        withStd=True,
    )
    return Pipeline(stages=[imputer, assembler, scaler]), imputed_columns


def temporal_split(
    data: DataFrame, train_fraction: float, validation_fraction: float
) -> tuple[DataFrame, DataFrame, DataFrame, list[Any]]:
    first_boundary = train_fraction
    second_boundary = train_fraction + validation_fraction
    # approxQuantile ne gère pas TimestampType ; on quantile les secondes Unix.
    epoch_data = data.withColumn("_window_epoch", F.unix_timestamp("window_start"))
    quantiles = epoch_data.approxQuantile(
        "_window_epoch", [first_boundary, second_boundary], 0.0
    )
    if len(quantiles) < 2 or quantiles[0] is None or quantiles[1] is None:
        raise RuntimeError("Impossible de calculer les frontières temporelles.")
    train_end_epoch, validation_end_epoch = quantiles
    train = epoch_data.filter(F.col("_window_epoch") <= train_end_epoch).drop(
        "_window_epoch"
    )
    validation = epoch_data.filter(
        (F.col("_window_epoch") > train_end_epoch)
        & (F.col("_window_epoch") <= validation_end_epoch)
    ).drop("_window_epoch")
    test = epoch_data.filter(F.col("_window_epoch") > validation_end_epoch).drop(
        "_window_epoch"
    )
    train_end = datetime.fromtimestamp(train_end_epoch, timezone.utc)
    validation_end = datetime.fromtimestamp(validation_end_epoch, timezone.utc)
    return train, validation, test, [train_end, validation_end]


def select_k(
    train_scaled: DataFrame,
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], int]:
    selection_sample = train_scaled.sample(
        withReplacement=False, fraction=args.selection_sample_fraction, seed=args.seed
    ).persist(StorageLevel.MEMORY_AND_DISK)
    sample_count = selection_sample.count()
    evaluator = ClusteringEvaluator(
        featuresCol="scaled_features",
        predictionCol="candidate_prediction",
        metricName="silhouette",
    )
    metrics: list[dict[str, Any]] = []
    try:
        for k in args.k_candidates:
            if k > sample_count:
                metrics.append({"k": k, "silhouette": None, "sample_count": sample_count})
                continue
            candidate = (
                KMeans()
                .setK(k)
                .setMaxIter(args.max_iter)
                .setSeed(args.seed)
                .setFeaturesCol("scaled_features")
                .setPredictionCol("candidate_prediction")
                .fit(selection_sample)
            )
            predicted = candidate.transform(selection_sample)
            score = float(evaluator.evaluate(predicted))
            metrics.append({"k": k, "silhouette": score, "sample_count": sample_count})
            log(f"k={k} : silhouette={score:.4f}")
    finally:
        selection_sample.unpersist()

    valid = [row for row in metrics if row["silhouette"] is not None]
    if not valid:
        return metrics, args.k_candidates[0]
    best = max(valid, key=lambda row: row["silhouette"])
    return metrics, int(best["k"])


def cluster_profiles(
    predictions: DataFrame,
    feature_columns: list[str],
    scaler_model: Any,
) -> list[dict[str, Any]]:
    profile_expressions = [F.avg(column).alias(column) for column in feature_columns]
    profiles = predictions.groupBy("cluster").agg(
        F.count(F.lit(1)).alias("window_count"),
        F.avg("active_sensor_count").alias("active_sensor_count_mean"),
        F.avg("total_sample_count").alias("total_sample_count_mean"),
        F.avg("hour").alias("hour_mean"),
        F.avg("is_weekend").alias("is_weekend_rate"),
        *profile_expressions,
    ).orderBy("cluster").collect()

    means = list(scaler_model.mean)
    scales = list(scaler_model.std)
    total_windows = sum(row["window_count"] for row in profiles) or 1
    result: list[dict[str, Any]] = []
    for row in profiles:
        differences: list[tuple[float, str]] = []
        for index, column in enumerate(feature_columns):
            value = row[column]
            if value is None:
                continue
            scale = scales[index] if scales[index] else 1.0
            difference = (float(value) - float(means[index])) / float(scale)
            differences.append((abs(difference), column))
        top_features = sorted(differences, reverse=True)[:8]
        top_text = "; ".join(
            f"{column} ({value:+.2f})" for value, column in top_features
        )
        result.append(
            {
                "cluster": int(row["cluster"]),
                "window_count": int(row["window_count"]),
                "window_share": round(row["window_count"] / total_windows, 6),
                "active_sensor_count_mean": round(
                    float(row["active_sensor_count_mean"]), 4
                ),
                "total_sample_count_mean": round(
                    float(row["total_sample_count_mean"]), 4
                ),
                "hour_mean": round(float(row["hour_mean"]), 4),
                "is_weekend_rate": round(float(row["is_weekend_rate"]), 6),
                "top_differentiating_features": top_text,
            }
        )
    return result


def write_csv(rows: list[dict[str, Any]], path: Path, fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def create_charts(
    report_dir: Path,
    metrics: list[dict[str, Any]],
    profiles: list[dict[str, Any]],
    predictions: DataFrame,
) -> list[str]:
    """Crée les graphiques de sélection et d'interprétation du clustering."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        log(f"Graphiques ignorés : matplotlib absent ({exc}).")
        return []

    chart_dir = report_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)
    charts: list[str] = []
    plt.rcParams.update({"figure.figsize": (10, 5), "axes.titlesize": 13})

    valid_metrics = [row for row in metrics if row["silhouette"] is not None]
    if valid_metrics:
        ks = [row["k"] for row in valid_metrics]
        scores = [row["silhouette"] for row in valid_metrics]
        best_index = max(range(len(scores)), key=lambda index: scores[index])
        fig, ax = plt.subplots()
        ax.plot(ks, scores, marker="o", color="#2563eb", linewidth=2)
        ax.scatter([ks[best_index]], [scores[best_index]], color="#dc2626", s=70, zorder=3)
        ax.set(
            title="Silhouette selon le nombre de clusters",
            xlabel="Nombre de clusters (k)",
            ylabel="Score de silhouette",
        )
        ax.set_xticks(ks)
        fig.tight_layout()
        path = chart_dir / "silhouette_by_k.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        charts.append(str(path))

    if profiles:
        clusters = [row["cluster"] for row in profiles]
        counts = [row["window_count"] for row in profiles]
        fig, ax = plt.subplots()
        bars = ax.bar([f"Cluster {cluster}" for cluster in clusters], counts, color="#7c3aed")
        ax.set(title="Nombre de fenêtres par cluster", ylabel="Nombre de fenêtres")
        for bar, count in zip(bars, counts):
            ax.text(bar.get_x() + bar.get_width() / 2, count, f"{count:,}", ha="center", va="bottom")
        fig.tight_layout()
        path = chart_dir / "cluster_sizes.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        charts.append(str(path))

        fig, axes = plt.subplots(1, 3, figsize=(15, 4))
        for ax, field, title in zip(
            axes,
            (
                "active_sensor_count_mean",
                "total_sample_count_mean",
                "hour_mean",
            ),
            ("Capteurs actifs moyens", "Mesures moyennes", "Heure moyenne"),
        ):
            values = [row[field] for row in profiles]
            ax.bar([f"C{cluster}" for cluster in clusters], values, color="#0f766e")
            ax.set(title=title)
            ax.tick_params(axis="x", rotation=0)
        fig.suptitle("Caractéristiques moyennes des clusters")
        fig.tight_layout()
        path = chart_dir / "cluster_context.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        charts.append(str(path))

        parsed_features: list[tuple[int, list[tuple[str, float]]]] = []
        pattern = re.compile(r"^(.*)\s+\(([+-]?[0-9.]+)\)$")
        for profile in profiles:
            entries: list[tuple[str, float]] = []
            for item in str(profile["top_differentiating_features"]).split(";"):
                match = pattern.match(item.strip())
                if match:
                    entries.append((match.group(1), float(match.group(2))))
            parsed_features.append((int(profile["cluster"]), entries[:8]))
        if any(entries for _, entries in parsed_features):
            fig, ax = plt.subplots(figsize=(12, max(5, 1.4 * len(parsed_features) * 8)))
            y_position = 0
            labels: list[str] = []
            colors = plt.cm.tab10.colors
            for index, (cluster, entries) in enumerate(parsed_features):
                for name, value in entries:
                    ax.barh(y_position, value, color=colors[index % len(colors)])
                    labels.append(f"C{cluster} | {name}")
                    y_position += 1
            ax.set_yticks(range(len(labels)))
            ax.set_yticklabels(labels, fontsize=8)
            ax.axvline(0, color="black", linewidth=0.8)
            ax.set(
                title="Features les plus différenciantes par cluster",
                xlabel="Écart standardisé moyen par rapport au centre global",
            )
            fig.tight_layout()
            path = chart_dir / "cluster_differentiating_features.png"
            fig.savefig(path, dpi=160)
            plt.close(fig)
            charts.append(str(path))

    hour_rows = predictions.groupBy("cluster", "hour").count().collect()
    if hour_rows:
        clusters = sorted({int(row["cluster"]) for row in hour_rows})
        hours = list(range(24))
        matrix = [
            [
                next(
                    (int(row["count"]) for row in hour_rows if int(row["cluster"]) == cluster and int(row["hour"]) == hour),
                    0,
                )
                for hour in hours
            ]
            for cluster in clusters
        ]
        fig, ax = plt.subplots()
        bottom = [0] * 24
        for index, cluster in enumerate(clusters):
            values = matrix[index]
            ax.bar(hours, values, bottom=bottom, label=f"Cluster {cluster}")
            bottom = [left + right for left, right in zip(bottom, values)]
        ax.set(
            title="Répartition des clusters selon l'heure",
            xlabel="Heure",
            ylabel="Nombre de fenêtres",
        )
        ax.set_xticks(hours)
        ax.legend()
        fig.tight_layout()
        path = chart_dir / "cluster_distribution_by_hour.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        charts.append(str(path))

    day_rows = predictions.groupBy("cluster", "day_of_week").count().collect()
    if day_rows:
        clusters = sorted({int(row["cluster"]) for row in day_rows})
        days = list(range(1, 8))
        day_names = ["Dimanche", "Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi"]
        matrix = [
            [
                next(
                    (int(row["count"]) for row in day_rows if int(row["cluster"]) == cluster and int(row["day_of_week"]) == day),
                    0,
                )
                for day in days
            ]
            for cluster in clusters
        ]
        fig, ax = plt.subplots()
        bottom = [0] * 7
        for index, cluster in enumerate(clusters):
            values = matrix[index]
            ax.bar(days, values, bottom=bottom, label=f"Cluster {cluster}")
            bottom = [left + right for left, right in zip(bottom, values)]
        ax.set(
            title="Répartition des clusters selon le jour de la semaine",
            xlabel="Jour",
            ylabel="Nombre de fenêtres",
        )
        ax.set_xticks(days)
        ax.set_xticklabels(day_names, rotation=30)
        ax.legend()
        fig.tight_layout()
        path = chart_dir / "cluster_distribution_by_day.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        charts.append(str(path))

    return charts


def write_report(
    report_dir: Path,
    args: argparse.Namespace,
    feature_columns: list[str],
    boundaries: list[Any],
    split_counts: dict[str, int],
    metrics: list[dict[str, Any]],
    best_k: int,
    holdout_silhouette: float | None,
    profiles: list[dict[str, Any]],
    model_dir: Path,
    assignment_dir: Path,
    started_at: float,
    charts: list[str],
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    completed_at = datetime.now(timezone.utc)
    selection_rows = "\n".join(
        f"| {row['k']} | "
        f"{row['silhouette']:.4f} |"
        if row["silhouette"] is not None
        else f"| {row['k']} | N/A |"
        for row in metrics
    )
    profile_rows = "\n".join(
        f"| {row['cluster']} | {row['window_count']:,} | "
        f"{row['window_share']:.2%} | {row['active_sensor_count_mean']:.2f} | "
        f"{row['total_sample_count_mean']:.1f} | {row['hour_mean']:.2f} |"
        for row in profiles
    )
    holdout_text = (
        f"{holdout_silhouette:.4f}" if holdout_silhouette is not None else "N/A"
    )
    holdout_interpretation = (
        "La silhouette de validation est négative : les centres appris sur la période "
        "d'entraînement ne séparent pas correctement les périodes futures. "
        "Le clustering est donc techniquement valide mais sa généralisation temporelle "
        "doit être considérée avec prudence."
        if holdout_silhouette is not None and holdout_silhouette < 0
        else "La silhouette de validation reste à interpréter avec prudence, car le "
        "clustering est non supervisé et les périodes futures peuvent avoir une "
        "distribution différente."
    )
    chart_lines = (
        "\n".join(f"- `{Path(path).name}`" for path in charts)
        or "- Aucun graphique généré."
    )
    report = f"""# Phase 4 — Standardisation et clustering KMeans

## Démarche

1. Lecture de la table large de la phase 3.
2. Ajout de variables cycliques pour l'heure et le jour de la semaine.
3. Séparation chronologique : 70 % entraînement, 15 % validation, 15 % test.
4. Imputation des valeurs manquantes sur l'entraînement uniquement.
5. Standardisation des variables.
6. Test de plusieurs valeurs de k avec le score de silhouette.
7. Entraînement du modèle KMeans final.
8. Affectation de chaque fenêtre à un cluster.

## Prévention de la fuite de données

Le modèle de preprocessing est ajusté uniquement sur la période d'entraînement :

- fin entraînement : {boundaries[0]}
- fin validation : {boundaries[1]}

Les périodes de validation et de test ne servent pas à ajuster l'imputation, le scaler ou les centres KMeans.

## Résultat de la séparation

- Fenêtres d'entraînement : **{split_counts['train']:,}**
- Fenêtres de validation : **{split_counts['validation']:,}**
- Fenêtres de test : **{split_counts['test']:,}**
- Nombre de variables utilisées : **{len(feature_columns)}**
- Nombre de clusters retenu : **{best_k}**
- Silhouette sur la période de validation : **{holdout_text}**

## Sélection de k

| k | silhouette |
| --- | --- |
{selection_rows}

Le k avec le meilleur score de silhouette est retenu. Ce score mesure la séparation entre clusters ; il ne constitue pas une preuve d'activité réelle.

## Profils des clusters

| cluster | fenêtres | part | capteurs actifs moyens | échantillons moyens | heure moyenne |
| --- | ---: | ---: | ---: | ---: | ---: |
{profile_rows}

Les features les plus différenciantes de chaque cluster sont détaillées dans `cluster_profiles.csv`.

## Lecture des résultats

- Le nombre de clusters a été sélectionné sur un échantillon de la période d'entraînement, puis le modèle final a été réajusté sur toutes les données d'entraînement.
- {holdout_interpretation}
- Les deux clusters sont assez équilibrés, mais leur différence principale semble aussi liée au nombre de capteurs actifs et au volume de mesures.
- Le cluster doit donc être décrit comme un profil de mesures, et non comme une activité humaine certaine.

## Graphiques

Les graphiques d'interprétation se trouvent dans `{report_dir / 'charts'}` :

{chart_lines}

## Modèle et sorties

- Modèle de preprocessing : `{model_dir / 'preprocessing'}`
- Modèle KMeans : `{model_dir / 'kmeans'}`
- Affectations : `{assignment_dir}`
- Dictionnaire des variables : `{report_dir / 'model_features.csv'}`

## Limites

- Le clustering est non supervisé et ne donne pas automatiquement le nom d'une activité.
- Les valeurs nulles représentent une absence de mesure, tandis que zéro représente une mesure inactive.
- Les résultats doivent être interprétés avec le dictionnaire des capteurs et les profils de cluster.
"""
    (report_dir / "clustering_report.md").write_text(report, encoding="utf-8")

    summary = {
        "phase": "phase4_clustering",
        "started_at_utc": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "completed_at_utc": completed_at.isoformat(),
        "input_dir": str(args.input_dir.resolve()),
        "model_dir": str(model_dir.resolve()),
        "assignment_dir": str(assignment_dir.resolve()),
        "feature_count": len(feature_columns),
        "split_counts": split_counts,
        "boundaries": [str(value) for value in boundaries],
        "k_candidates": metrics,
        "selected_k": best_k,
        "holdout_silhouette": holdout_silhouette,
        "profiles": profiles,
    }
    (report_dir / "clustering_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def run(args: argparse.Namespace) -> None:
    started_at = time.time()
    input_dir = args.input_dir.expanduser().resolve()
    model_dir = args.model_dir.expanduser().resolve()
    assignment_dir = args.assignment_dir.expanduser().resolve()
    report_dir = args.report_dir.expanduser().resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"Table large introuvable : {input_dir}. Exécutez d'abord phase3_features.py."
        )

    spark = create_spark(args)
    data: DataFrame | None = None
    train_scaled: DataFrame | None = None
    try:
        log("Lecture de la table large des fenêtres.")
        data = add_calendar_features(spark.read.parquet(str(input_dir)))
        feature_columns = select_feature_columns(data)
        sensor_feature_columns = [
            column for column in feature_columns if column not in CONTEXT_COLUMNS
        ]
        log(f"Nombre de variables numériques retenues : {len(feature_columns)}")

        log("Séparation chronologique des fenêtres.")
        train, validation, test, boundaries = temporal_split(
            data, args.train_fraction, args.validation_fraction
        )
        split_counts = {
            "train": train.count(),
            "validation": validation.count(),
            "test": test.count(),
        }
        log(
            f"Entraînement={split_counts['train']:,}, "
            f"validation={split_counts['validation']:,}, "
            f"test={split_counts['test']:,}"
        )

        log("Ajustement de l'imputation, de l'assemblage et du standard scaler.")
        preprocessor, imputed_columns = build_preprocessor(feature_columns)
        preprocess_model = preprocessor.fit(train)
        train_scaled = preprocess_model.transform(train).select(
            "window_start",
            "window_end",
            "hour",
            "day_of_week",
            "hour_sin",
            "hour_cos",
            "day_of_week_sin",
            "day_of_week_cos",
            "is_weekend",
            "active_sensor_count",
            "total_sample_count",
            "scaled_features",
            *sensor_feature_columns,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        log("Standardisation terminée sur la période d'entraînement.")

        log("Sélection du nombre de clusters avec le score de silhouette.")
        metrics, best_k = select_k(train_scaled, args)
        log(f"k retenu : {best_k}")

        log("Entraînement du modèle KMeans final.")
        kmeans = (
            KMeans()
            .setK(best_k)
            .setMaxIter(args.max_iter)
            .setSeed(args.seed)
            .setFeaturesCol("scaled_features")
            .setPredictionCol("cluster")
        )
        kmeans_model = kmeans.fit(train_scaled)

        log("Évaluation du modèle sur la période de validation.")
        validation_scaled = preprocess_model.transform(validation)
        validation_pred = kmeans_model.transform(validation_scaled)
        evaluator = ClusteringEvaluator(
            featuresCol="scaled_features",
            predictionCol="cluster",
            metricName="silhouette",
        )
        try:
            holdout_silhouette: float | None = float(
                evaluator.evaluate(validation_pred)
            )
        except Exception as exc:  # le rapport doit rester produit même si la métrique échoue
            log(f"Silhouette de validation indisponible : {exc}")
            holdout_silhouette = None

        log("Affectation de toutes les fenêtres et création des profils.")
        all_scaled = preprocess_model.transform(data).select(
            "window_start",
            "window_end",
            "hour",
            "day_of_week",
            "hour_sin",
            "hour_cos",
            "day_of_week_sin",
            "day_of_week_cos",
            "is_weekend",
            "active_sensor_count",
            "total_sample_count",
            "scaled_features",
            *sensor_feature_columns,
        )
        predictions = kmeans_model.transform(all_scaled)
        assignment_dir.mkdir(parents=True, exist_ok=True)
        predictions.write.mode("overwrite").option("compression", "snappy").parquet(
            str(assignment_dir)
        )
        log(f"Affectations enregistrées dans {assignment_dir}")

        scaler_model = preprocess_model.stages[-1]
        profiles = cluster_profiles(predictions, feature_columns, scaler_model)
        charts = create_charts(report_dir, metrics, profiles, predictions)
        write_csv(
            metrics,
            report_dir / "k_selection.csv",
            ["k", "silhouette", "sample_count"],
        )
        write_csv(
            profiles,
            report_dir / "cluster_profiles.csv",
            [
                "cluster",
                "window_count",
                "window_share",
                "active_sensor_count_mean",
                "total_sample_count_mean",
                "hour_mean",
                "is_weekend_rate",
                "top_differentiating_features",
            ],
        )
        write_csv(
            [{"column": column} for column in feature_columns],
            report_dir / "model_features.csv",
            ["column"],
        )

        model_dir.mkdir(parents=True, exist_ok=True)
        preprocess_model.write().overwrite().save(str(model_dir / "preprocessing"))
        kmeans_model.write().overwrite().save(str(model_dir / "kmeans"))
        write_report(
            report_dir,
            args,
            feature_columns,
            boundaries,
            split_counts,
            metrics,
            best_k,
            holdout_silhouette,
            profiles,
            model_dir,
            assignment_dir,
            started_at,
            charts,
        )
        log(f"Rapport : {report_dir / 'clustering_report.md'}")
    finally:
        if train_scaled is not None:
            train_scaled.unpersist()
        if data is not None:
            data.unpersist()
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
