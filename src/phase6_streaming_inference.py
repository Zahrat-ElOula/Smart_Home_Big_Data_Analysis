"""Phase 6, étape 4 : prédiction KMeans sur les fenêtres Kafka.

Chaque micro-batch Kafka est agrégé par fenêtre de cinq minutes. Les 47
variables attendues par le modèle final sont reconstruites, puis le modèle
KMeans k=2 produit le cluster de chaque fenêtre.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml import PipelineModel
from pyspark.ml.clustering import KMeansModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from phase6_streaming_consumer import create_spark, parse_messages, wait_for_drain


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL_DIR = PROJECT_ROOT / "models" / "phase5_final_kmeans"
DEFAULT_FEATURES_FILE = PROJECT_ROOT / "outputs" / "phase5_final_model" / "model_features.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase6_streaming" / "cluster_predictions"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "outputs" / "phase6_streaming"

FEATURE_NAMES = (
    "sample_count",
    "mean_value",
    "max_value",
    "positive_value_count",
    "zero_value_count",
)


def log(message: str) -> None:
    print(f"[PHASE 6.4] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Application du modèle KMeans aux fenêtres Kafka."
    )
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="smart-home-events")
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--features-file", type=Path, default=DEFAULT_FEATURES_FILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=DEFAULT_REPORT_DIR / "inference_checkpoint",
    )
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--poll-seconds", type=float, default=3.0)
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

    if args.timeout_seconds < 1 or args.max_offsets_per_trigger < 1:
        parser.error("Les paramètres de streaming doivent être positifs.")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds doit être supérieur à 0.")
    return args


def load_feature_columns(path: Path) -> list[str]:
    if not path.is_file():
        raise FileNotFoundError(f"Liste des variables introuvable : {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        columns = [row["column"].strip() for row in csv.DictReader(handle)]
    if not columns:
        raise ValueError(f"Aucune variable dans {path}.")
    return columns


def add_missing_model_features(
    features: DataFrame,
    expected_features: list[str],
) -> DataFrame:
    """Ajoute les colonnes absentes du micro-batch avec la valeur null imputable."""
    for column in expected_features:
        if column not in features.columns:
            default = F.lit(0) if column.endswith("_sample_count") else F.lit(None).cast("double")
            features = features.withColumn(column, default)
    return features.select("window_start", "window_end", *expected_features)


def build_window_features(
    batch: DataFrame,
    expected_features: list[str],
) -> DataFrame:
    """Reconstruit les variables du modèle pour chaque fenêtre de cinq minutes.

    Les statistiques par capteur sont produites pour tous les capteurs présents
    dans le flux ; seules les 47 variables attendues par le modèle sont
    conservées, les autres restent nulles et sont imputées par le modèle de
    preprocessing, exactement comme lors de l'entraînement.
    """
    events = (
        batch.withColumn(
            "feature_key",
            F.regexp_replace(F.lower("sensor_name"), "[^a-z0-9]+", "_"),
        )
        .withColumn(
            "sensor_window",
            F.window(F.col("event_timestamp"), "5 minutes"),
        )
        .withColumn("window_start", F.col("sensor_window.start"))
        .withColumn("window_end", F.col("sensor_window.end"))
    )
    long_features = events.groupBy(
        "window_start", "window_end", "sensor_id", "feature_key"
    ).agg(
        F.count(F.lit(1)).alias("raw_event_count"),
        F.count("value").alias("sample_count"),
        F.avg("value").alias("mean_value"),
        F.max("value").alias("max_value"),
        F.sum(F.when(F.col("value") > 0, 1).otherwise(0)).alias(
            "positive_value_count"
        ),
        F.sum(F.when(F.col("value") == 0, 1).otherwise(0)).alias(
            "zero_value_count"
        ),
    )

    aggregations = []
    for feature_name in FEATURE_NAMES:
        if feature_name == "sample_count":
            expression = F.coalesce(F.first(feature_name, True), F.lit(0))
        else:
            expression = F.first(feature_name, True)
        aggregations.append(expression.alias(feature_name))

    wide = long_features.groupBy("window_start", "window_end").pivot(
        "feature_key"
    ).agg(*aggregations)
    window_stats = long_features.groupBy("window_start", "window_end").agg(
        F.countDistinct("sensor_id").alias("active_sensor_count"),
        F.sum("sample_count").alias("total_sample_count"),
    )
    features = (
        wide.join(window_stats, on=["window_start", "window_end"], how="left")
        .withColumn("hour", F.hour("window_start"))
        .withColumn("day_of_week", F.dayofweek("window_start"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("window_start").isin(1, 7), 1.0).otherwise(0.0),
        )
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
    return add_missing_model_features(features, expected_features)


def write_summary(
    report_dir: Path,
    args: argparse.Namespace,
    batch_count: int,
    consumed_events: int,
    prediction_count: int,
    distinct_windows: int,
    cluster_counts: dict[str, int],
    window_range: tuple[Any, Any],
    started_at: float,
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    first_window, last_window = window_range
    summary = {
        "phase": "phase6_step4_streaming_inference",
        "started_at_utc": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "bootstrap_server": args.bootstrap_server,
        "topic": args.topic,
        "model_dir": str(args.model_dir.resolve()),
        "model_features": len(load_feature_columns(args.features_file.expanduser().resolve())),
        "processed_batches": batch_count,
        "consumed_events": consumed_events,
        "predicted_windows": prediction_count,
        "distinct_windows": distinct_windows,
        "cluster_counts": cluster_counts,
        "first_window": str(first_window),
        "last_window": str(last_window),
        "output_dir": str(args.output_dir.resolve()),
    }
    (report_dir / "inference_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (report_dir / "inference_report.md").write_text(
        build_report(summary), encoding="utf-8"
    )


def build_report(summary: dict[str, Any]) -> str:
    total = summary["predicted_windows"]
    cluster_rows = "\n".join(
        f"| {cluster} | {count} | {count / total:.2%} |"
        for cluster, count in sorted(summary["cluster_counts"].items())
    )
    return f"""# Phase 6, étape 4 — Inférence KMeans sur le flux Kafka

## Démarche

1. Lecture du topic `smart-home-events` avec la source Kafka de Spark Structured Streaming.
2. Décodage de chaque message JSON en événement de mesure.
3. Regroupement des événements par fenêtre de cinq minutes, comme en phase 3.
4. Reconstruction des {summary['model_features']} variables du modèle final.
5. Application du modèle de preprocessing puis du modèle KMeans k=2 enregistré en phase 5.
6. Écriture du cluster prédit pour chaque fenêtre.

Chaque micro-batch est traité de façon autonome (`foreachBatch`) : le topic est
relu depuis le début à chaque exécution, le checkpoint étant supprimé au démarrage.

## Résultat

- Micro-batchs traités : **{summary['processed_batches']}**
- Messages Kafka consommés : **{summary['consumed_events']:,}**
- Fenêtres prédites : **{summary['predicted_windows']:,}**
- Fenêtres distinctes : **{summary['distinct_windows']:,}**
- Première fenêtre : `{summary['first_window']}`
- Dernière fenêtre : `{summary['last_window']}`

| cluster | fenêtres | part |
| --- | ---: | ---: |
{cluster_rows}

## Lecture des résultats

- Le nombre de fenêtres prédites est supérieur au nombre de fenêtres distinctes :
  une fenêtre à cheval sur deux micro-batchs est prédite deux fois, une fois par
  partie. Ce comportement est propre à l'agrégation par micro-batch ; un déploiement
  realiste utiliserait une agrégation avec état et marque temporelle.
- Les horodatages sont rendus par Spark SQL dans le fuseau de la session (UTC) :
  le rejeu et le modèle restent ainsi alignés sur les fenêtres de la phase 3.
- Le rejeu reconstruit les mêmes variables que le traitement hors-ligne : les
  fenêtres du flux et celles de la phase 3 partagent les mêmes bornes de cinq
  minutes, et les capteurs absents d'une fenêtre sont imputés par le modèle.
- Les journées denses (environ 23 capteurs actifs par fenêtre) et les journées creuses
  (environ 10 capteurs actifs) ne se rattachent pas au même cluster, ce qui confirme que
  la séparation apprise porte surtout sur la densité des mesures.

## Limites

- Il s'agit d'un rejeu historique, pas d'un flux de capteurs physiques.
- Les modèles sont rechargés dans chaque micro-batch, choix lié à la sérialisation des
  objets Java dans la closure `foreachBatch`.
- Le clustering reste exploratoire : la silhouette de validation de la phase 5 est négative.
"""


def sum_offsets(value: Any) -> int:
    """Somme des offsets Kafka, publiés sous forme de JSON sérialisé.

    La source Kafka publie `{"topic": {"partition": offset}}` : les niveaux
    imbriqués sont additionnés jusqu'aux valeurs numériques.
    """
    if value is None:
        return 0
    if isinstance(value, str):
        if not value or value == "null":
            return 0
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return 0
    if isinstance(value, dict):
        return sum(sum_offsets(item) for item in value.values())
    if isinstance(value, (int, float)) or str(value).isdigit():
        return int(value)
    return 0


def count_consumed_offsets(progress: list[dict[str, Any]]) -> int:
    """Nombre de messages Kafka réellement consommés, d'après les offsets.

    `numInputRows` n'est pas utilisé ici : il peut cumuler les lignes relues lors
    des ré-exécutions de tâches, alors que les offsets Kafka comptent chaque
    message une seule fois.
    """
    total = 0
    for batch in progress:
        for source in batch.get("sources", []):
            total += sum_offsets(source.get("endOffset")) - sum_offsets(
                source.get("startOffset")
            )
    return total


def run(args: argparse.Namespace) -> None:
    started_at = time.time()
    model_dir = args.model_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    report_dir = args.report_dir.expanduser().resolve()
    checkpoint_dir = args.checkpoint_dir.expanduser().resolve()
    feature_columns = load_feature_columns(args.features_file.expanduser().resolve())
    log(f"Variables du modèle : {len(feature_columns)}")
    if output_dir.exists():
        shutil.rmtree(output_dir)
    if checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir)

    spark = create_spark(args)
    query = None
    try:
        model_dir_text = str(model_dir)
        output_dir_text = str(output_dir)

        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_server)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))
            .load()
        )
        # Chaque micro-batch est traité de façon indépendante : l'agrégation par
        # fenêtre de cinq minutes est faite dans process_batch, donc aucune
        # marque temporelle ni deduplication d'état n'est necessaire.
        events = parse_messages(raw)

        def process_batch(batch_df: DataFrame, batch_id: int) -> None:
            features = build_window_features(batch_df, feature_columns).cache()
            try:
                window_count = features.count()
                if window_count == 0:
                    return
                # Les modèles sont chargés dans le worker pour éviter de
                # sérialiser des objets Java dans la closure foreachBatch.
                local_preprocess = PipelineModel.load(
                    str(Path(model_dir_text) / "preprocessing")
                )
                local_kmeans = KMeansModel.load(str(Path(model_dir_text) / "kmeans"))
                predictions = local_kmeans.transform(
                    local_preprocess.transform(features)
                ).select(
                    "window_start",
                    "window_end",
                    "cluster",
                    "active_sensor_count",
                    "total_sample_count",
                ).withColumn("batch_id", F.lit(batch_id))
                predictions.write.mode("append").parquet(output_dir_text)
                log(f"Micro-batch {batch_id} : {window_count} fenêtres prédites")
            finally:
                features.unpersist()

        query = events.writeStream.foreachBatch(process_batch).option(
            "checkpointLocation", str(checkpoint_dir)
        ).start()
        log(f"Query démarrée, arrêt après {args.timeout_seconds} s maximum.")
        wait_for_drain(query, args.timeout_seconds, args.poll_seconds)
        progress = list(query.recentProgress)
        batch_count = sum(1 for item in progress if item.get("numInputRows", 0) > 0)
        consumed_events = count_consumed_offsets(progress)
        if not query.isActive:
            raise RuntimeError(f"Query interrompue : {query.exception()}")
        query.stop()

        predictions = spark.read.parquet(str(output_dir)).cache()
        prediction_count = predictions.count()
        if prediction_count == 0:
            raise RuntimeError(
                "Aucune fenêtre n'a été prédite. Vérifiez le topic et le checkpoint."
            )
        distinct_windows = predictions.select("window_start").distinct().count()
        cluster_counts = {
            str(row["cluster"]): row["count"]
            for row in predictions.groupBy("cluster").count().collect()
        }
        # Les bornes sont rendues par Spark : un datetime Python serait décalé
        # par le fuseau local de la machine.
        bounds = predictions.agg(
            F.min("window_start").cast("string").alias("premiere"),
            F.max("window_start").cast("string").alias("derniere"),
        ).first()
        write_summary(
            report_dir,
            args,
            batch_count,
            consumed_events,
            prediction_count,
            distinct_windows,
            cluster_counts,
            (bounds["premiere"], bounds["derniere"]),
            started_at,
        )
        log(f"Micro-batchs traités : {batch_count}")
        log(f"Messages consommés : {consumed_events:,}")
        log(f"Fenêtres prédites : {prediction_count:,} ({distinct_windows:,} distinctes)")
        log(f"Répartition des clusters : {cluster_counts}")
        log(f"Prédictions : {output_dir}")
        log(f"Rapport : {report_dir / 'inference_report.md'}")
        predictions.unpersist()
    finally:
        if query is not None:
            try:
                query.stop()
            except Exception:
                pass
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
