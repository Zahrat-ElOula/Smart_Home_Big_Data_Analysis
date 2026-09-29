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
│   ├── phase3_features.py
│   ├── phase4_clustering.py
│   ├── phase5_sensor_selection.py
│   ├── phase5_feature_reduction.py
│   ├── phase5_model_selection.py
│   ├── phase6_kafka_producer.py
│   ├── phase6_streaming_consumer.py
│   └── phase6_streaming_inference.py
├── docker-compose.yml
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

## Phase 4 — Standardisation et clustering

La phase 4 sépare les fenêtres par ordre chronologique, ajuste l'imputation et le `StandardScaler` uniquement sur la période d'entraînement, puis recherche le meilleur nombre de clusters :

```powershell
.\.venv\Scripts\python.exe src\phase4_clustering.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --k-candidates "2,3,4,5,6" `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase4_spark"
```

Sorties :

```text
models/phase4_kmeans/preprocessing/
models/phase4_kmeans/kmeans/
data/processed/clustering_5min/
outputs/phase4_clustering/clustering_report.md
outputs/phase4_clustering/cluster_profiles.csv
outputs/phase4_clustering/k_selection.csv
```

Le clustering est non supervisé : les clusters doivent être interprétés avec `feature_dictionary.csv` et ne constituent pas automatiquement des noms d'activités.

## Phase 5 — Étape 1 : sélection des capteurs stables

Cette étape lit la table longue de la phase 3 et conserve les capteurs présents dans au moins 95 % des fenêtres observées :

```powershell
.\.venv\Scripts\python.exe src\phase5_sensor_selection.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --min-window-coverage 0.95 `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase5_sensor"
```

Sorties :

```text
outputs/phase5_sensor_selection/stable_sensor_selection.csv
outputs/phase5_sensor_selection/stable_sensor_names.txt
outputs/phase5_sensor_selection/sensor_selection_summary.json
outputs/phase5_sensor_selection/sensor_selection_report.md
```

Cette étape ne modifie pas encore le modèle de clustering. Elle prépare uniquement la liste des capteurs stables.

## Phase 5 — Étape 2 : réduction des variables

Cette étape utilise les 8 capteurs stables et conserve 5 statistiques par capteur :

```powershell
.\.venv\Scripts\python.exe src\phase5_feature_reduction.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase5_reduction"
```

Sorties :

```text
data/processed/features_5min/window_features_reduced/
outputs/phase5_feature_reduction/feature_reduction_report.md
outputs/phase5_feature_reduction/reduced_feature_dictionary.csv
outputs/phase5_feature_reduction/feature_reduction_summary.json
```

Le clustering n'est pas encore réentraîné à cette étape.

## Phase 5 — Étape 3 : sélection robuste de k

Après la réduction des variables, cette étape entraîne KMeans sur toute la période d'entraînement pour plusieurs valeurs de `k`, mesure les silhouettes entraînement/validation/test et impose une taille minimale de cluster :

```powershell
.\.venv\Scripts\python.exe src\phase5_model_selection.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --k-candidates "2,3,4,5,6" `
  --min-cluster-share 0.01 `
  --evaluation-sample-fraction 0.30 `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase5_model_selection"
```

Sorties :

```text
outputs/phase5_model_selection/k_selection_full.csv
outputs/phase5_model_selection/model_selection_summary.json
outputs/phase5_model_selection/model_selection_report.md
outputs/phase5_model_selection/charts/
```

## Phase 6 — Kafka : infrastructure

Docker Desktop doit être démarré. Le broker Kafka local est défini dans `docker-compose.yml` :

```powershell
docker compose up -d
```

Après le démarrage du broker, créer le topic :

```powershell
docker exec smart-home-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create --if-not-exists `
  --topic smart-home-events `
  --partitions 1 `
  --replication-factor 1
```

Vérification du topic :

```powershell
docker exec smart-home-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

## Phase 6 — Kafka : producteur

Le producteur relit les mesures nettoyées sur une tranche de temps donnée et envoie des messages JSON dans `smart-home-events` par ordre chronologique :

```powershell
.\.venv\Scripts\python.exe src\phase6_kafka_producer.py `
  --bootstrap-server "localhost:9092" `
  --topic "smart-home-events" `
  --start "2020-06-18 09:00:00" `
  --end "2020-06-18 12:00:00" `
  --max-rows 250000 `
  --delay-seconds 0
```

Il s'agit d'un rejeu contrôlé du dataset historique, et non de capteurs physiques temps réel. Les envois sont asynchrones (`acks=all`) et les acquittements en erreur sont comptés puis signalés.

## Phase 6 — Kafka : consommateur Spark Structured Streaming

Le consommateur lit le topic, valide les messages JSON et les sauvegarde en Parquet :

```powershell
.\.venv\Scripts\python.exe src\phase6_streaming_consumer.py `
  --bootstrap-server "localhost:9092" `
  --topic "smart-home-events" `
  --timeout-seconds 20 `
  --max-offsets-per-trigger 1000
```

La première exécution peut télécharger le connecteur Spark Kafka `spark-sql-kafka-0-10_2.13:4.2.0` via Maven/Ivy.

Sorties :

```text
outputs/phase6_streaming/parsed_events/
outputs/phase6_streaming/consumer_summary.json
```

## Phase 6 — Kafka : inférence KMeans

Le consommateur d'inférence regroupe chaque micro-batch Kafka par fenêtre de cinq minutes, reconstruit les 47 variables du modèle final (8 capteurs × 5 statistiques + 7 variables de contexte) et prédit le cluster :

```powershell
.\.venv\Scripts\python.exe src\phase6_streaming_inference.py `
  --bootstrap-server "localhost:9092" `
  --topic "smart-home-events" `
  --timeout-seconds 900 `
  --max-offsets-per-trigger 70000
```

La query s'arrête dès qu'un micro-batch vide confirme que le topic est épuisé. Le checkpoint et le répertoire de prédictions sont supprimés à chaque exécution pour rejouer le flux depuis le début.

Les capteurs absents d'une fenêtre restent nuls et sont imputés par le modèle de preprocessing, exactement comme lors de l'entraînement. C'est indispensable : les variables de contexte `active_sensor_count` et `total_sample_count` n'ont de sens que si le flux contient tous les capteurs.

## Phase 6 — Kafka : résultats

Le topic `smart-home-events` a été alimenté par trois rejeux du 18 et du 29 juin 2020, deux journées de profils opposés :

```text
2020-06-17 21:00 → 2020-06-17 23:59   209 736 messages (nuit)
2020-06-18 09:00 → 2020-06-18 11:59   211 084 messages (journée dense, 23 capteurs)
2020-06-29 07:00 → 2020-06-29 09:29    63 272 messages (journée creuse, 9 capteurs)
```

L'inférence donne :

- **484 092** messages consommés en **7** micro-batchs
- **108** fenêtres prédites, dont **102** distinctes
- cluster 0 : **78** fenêtres (72,2 %) ; cluster 1 : **30** fenêtres (27,8 %)

Les journées denses et les journées creuses ne se rattachent pas au même cluster, ce qui confirme que la séparation apprise porte d'abord sur la densité des mesures et non sur une activité humaine identifiée.

Deux points de vigilance documentés dans le rapport :

- une fenêtre à cheval sur deux micro-batchs est prédite deux fois (108 prédictions pour 102 fenêtres) ; un déploiement réel utiliserait une agrégation avec état et marque temporelle ;
- les horodatages sont rendus par Spark SQL en UTC : passer par un `datetime` Python introduirait un décalage de fuseau et décalerait toutes les fenêtres.

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
