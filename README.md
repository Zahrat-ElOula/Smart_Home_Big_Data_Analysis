# Mini-projet Big Data 2 — Smart Home

Analyse du dataset Mendeley **Multi-sensor dataset of human activities in a smart home environment** avec PySpark.

## Structure

```text
BigData/
├── human_activity_raw_sensor_data/
│   ├── sensor.csv
│   ├── sensor_sample_int.csv
│   └── sensor_sample_float.csv
├── src/
│   ├── phase1_eda.py
│   ├── phase2_cleaning.py
│   └── phase3_features.py
├── outputs/
│   └── eda/                 # créé par le script
├── requirements.txt
└── README.md
```

## Phase 1 — Ingestion et EDA

Le script `src/phase1_eda.py` :

1. vérifie la présence et la taille des trois fichiers ;
2. lit les métadonnées des capteurs ;
3. lit les mesures avec des schémas Spark explicites ;
4. vérifie les types, timestamps, valeurs manquantes et relations entre fichiers ;
5. calcule les statistiques globales et par capteur ;
6. crée un échantillon aléatoire de 1 % pour les quantiles et profils temporels ;
7. produit des tableaux CSV, des graphiques et `eda_report.md`.

Les volumes et contrôles de qualité sont calculés sur **toutes les mesures**. Les quantiles et profils d'activité sont calculés sur l'échantillon afin de limiter les recomputations.

## Prérequis

- Python 3.10 ou plus récent ;
- Java 17 pour Spark ;
- au moins 8 Go de RAM disponibles ;
- **35 à 40 Go libres** pour le dataset, le cache Spark, les fichiers temporaires et les résultats.

## Installation

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Le script recherche automatiquement une installation Java 17+ sur Windows. Si aucune installation n'est détectée, définir `JAVA_HOME`.

## Exécution complète

Après avoir créé `.venv` et installé les dépendances :

```powershell
.\.venv\Scripts\python.exe src\phase1_eda.py `
  --master "local[8]" `
  --shuffle-partitions 32 `
  --sample-fraction 0.01 `
  --seed 42
```

Un second passage avec un échantillon plus petit peut limiter le travail sur les quantiles, mais il reste une **ingestion complète** :

```powershell
.\.venv\Scripts\python.exe src\phase1_eda.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --sample-fraction 0.002 `
  --no-write-sample-parquet `
  --output-dir outputs\eda_test
```

Un répertoire de travail Spark peut être choisi explicitement, par exemple sur un autre disque :

```powershell
.\.venv\Scripts\python.exe src\phase1_eda.py `
  --master "local[8]" `
  --scratch-dir "D:\spark-temp\smart-home"
```

Cette première version accepte uniquement un master `local[...]`, car elle lit des fichiers présents sur le poste.

## Phase 2 — Nettoyage léger

Après l'EDA, le script suivant normalise les mesures, les associe aux capteurs et les écrit en Parquet :

```powershell
.\.venv\Scripts\python.exe src\phase2_cleaning.py --master "local[4]" --shuffle-partitions 16
```

Sorties :

```text
data/processed/measurements_clean/
outputs/phase2_cleaning/cleaning_report.md
outputs/phase2_cleaning/cleaning_summary.json
```

Les zéros sont conservés car ils représentent un état normal. Les courants négatifs sont conservés mais signalés par `is_suspect_value`. Le dédoublonnage exact est désactivé par défaut, car il serait coûteux sur 247 millions de lignes.

Sous Windows, l'écriture Parquet nécessite `winutils.exe` et `hadoop.dll`. Le script détecte une installation locale ; sinon, utilisez `--hadoop-home` avec un dossier contenant `bin/winutils.exe` et `bin/hadoop.dll`.

## Phase 3 — Construction des fenêtres temporelles

La phase 3 lit le Parquet nettoyé et regroupe les événements par fenêtre de cinq minutes :

```powershell
.\.venv\Scripts\python.exe src\phase3_features.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --window "5 minutes" `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase3_spark"
```

Sorties :

```text
data/processed/features_5min/sensor_features_long/
data/processed/features_5min/window_features_wide/
outputs/phase3_features/feature_report.md
outputs/phase3_features/feature_dictionary.csv
```

La table longue contient les statistiques par capteur et par fenêtre. La table large contient une ligne par fenêtre et des colonnes prêtes pour une étape Spark ML. Les valeurs marquées `is_suspect_value` sont exclues des statistiques, mais le nombre de valeurs suspectes est conservé.

## Principaux résultats

Les résultats sont créés dans `outputs/eda/` :

```text
outputs/eda/
├── eda_report.md
├── run_manifest.json
├── dataset_inventory.json
├── tables/
│   ├── sensor_metadata.csv
│   ├── summary_by_file.csv
│   ├── overall_quality.json
│   ├── sensor_summary.csv
│   ├── orphan_sensor_events.csv
│   ├── sensors_without_measurements.csv
│   ├── value_quantiles_sample.csv
│   ├── daily_activity_sample.csv
│   ├── hourly_activity_sample.csv
│   ├── activity_by_hour_of_day.csv
│   └── activity_by_day_of_week.csv
├── charts/
│   ├── daily_activity.png
│   ├── activity_by_hour.png
│   ├── top_sensors.png
│   └── sensor_coverage.png
└── sample_0.01/
    └── données Parquet de l'échantillon
```

## Comment lire les résultats

- `row_count` : nombre total de mesures lues par Spark ;
- `valid_timestamp_rate` : proportion de timestamps exploitables ;
- `missing_or_non_finite_value_count` : valeurs nulles, `NaN` ou infinies ;
- `sensor_summary.csv` : volume, période, moyenne, minimum et maximum par capteur ; `observation_span_ratio` est un rapport de période observée, pas une véritable taux de couverture temporelle ;
- `orphan_sensor_events.csv` : mesures dont le `sensor_id` n'existe pas dans `sensor.csv` ;
- `sensors_without_measurements.csv` : capteurs déclarés mais sans aucune mesure ;
- `value_quantiles_sample.csv` : distribution approximative des valeurs, séparée par capteur ;
- `activity_by_hour_of_day.csv` : nombre de mesures selon l'heure ;
- `activity_by_day_of_week.csv` : nombre de mesures selon le jour de semaine.

## Limites importantes

- Une forte activité de capteur ne prouve pas à elle seule une activité humaine : certains capteurs changent d'état très souvent.
- Les capteurs utilisent des unités différentes ; il ne faut pas les comparer directement avant standardisation.
- Le dataset ne contient pas de colonne `activity`. Une classification supervisée nécessite des annotations ou des pseudo-étiquettes validées.
- Les quantiles sont des estimations calculées sur un échantillon de 1 %, alors que les volumes et contrôles d'intégrité portent sur toutes les lignes.
- `approx_count_distinct` fournit seulement une estimation HLL du nombre de `value_id` distincts ; son écart avec le nombre de lignes ne constitue pas une preuve de doublons.
