# Smart Home Big Data Project

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white" alt="Python 3.10+" />
  <img src="https://img.shields.io/badge/Apache-Spark-3.x-E25A1C?logo=apache-spark&logoColor=white" alt="Apache Spark" />
  <img src="https://img.shields.io/badge/Kafka-3.x-231F20?logo=apache-kafka&logoColor=white" alt="Kafka" />
  <img src="https://img.shields.io/badge/Status-Research%20Project-4CAF50" alt="Research Project" />
</p>

A data engineering and analytics project that analyzes the Mendeley dataset: "Multi-sensor dataset of human activities in a smart home environment" using PySpark, clustering, feature reduction, and a Kafka streaming pipeline.

The detailed synthesis and interpretation of each phase are documented in [RAPPORT.md](RAPPORT.md). This README focuses on repository structure, setup, execution flow, and expected outputs.

## Overview

This project studies smart-home sensor behavior through a complete pipeline:

- data ingestion and EDA
- light cleaning and validation
- temporal feature construction
- clustering and model selection
- sensor stability analysis and dimensionality reduction
- Kafka-based streaming ingestion and inference

The objective is to understand patterns in sensor activity, identify dominant operating regimes, and evaluate whether clusters correspond to meaningful activity or simply to measurement density profiles.

## Repository structure

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
│   ├── eda/
│   ├── phase2_cleaning/
│   ├── phase3_features/
│   ├── phase4_clustering/
│   ├── phase5_sensor_selection/
│   ├── phase5_feature_reduction/
│   ├── phase5_reduced_clustering/
│   ├── phase5_model_selection/
│   ├── phase5_final_model/
│   └── phase6_streaming/
├── requirements.txt
├── RAPPORT.md
├── README.md
└── models/
```

## Project phases

### Phase 1 — Ingestion and EDA

The script `src/phase1_eda.py`:

1. checks the presence and size of the three input files;
2. reads sensor metadata;
3. loads measurements with explicit Spark schemas;
4. validates types, timestamps, missing values, and cross-file relationships;
5. computes global and per-sensor quality statistics;
6. creates a random 1% sample for quantiles and time-based profiling;
7. writes CSV tables, plots, and `eda_report.md`.

Important: integrity checks and volume metrics are computed on the full dataset, while quantile and temporal profiling are computed on the sample to reduce repeated work.

### Phase 2 — Light cleaning

The script `src/phase2_cleaning.py` normalizes measurements, joins them with sensor metadata, and writes clean Parquet data.

It retains zero values because they represent a valid system state, and it keeps negative current values but flags them as `is_suspect_value` for downstream review.

### Phase 3 — Time-window feature construction

The script `src/phase3_features.py` groups events into five-minute windows and produces:

- a long-format table with per-sensor statistics per window
- a wide-format table with one row per window and one column per feature

This is the base dataset used for clustering and modeling.

### Phase 4 — Standardization and clustering

The script `src/phase4_clustering.py`:

- orders windows chronologically;
- adjusts missing-value imputation and scaling using only the training period;
- searches for the best cluster count (`k`) using candidate values such as 2, 3, 4, 5, 6.

Outputs include preprocessing artifacts, KMeans models, cluster profiles, and clustering diagnostics.

### Phase 5 — Sensor stability, feature reduction, and model selection

This phase progressively refines the modeling pipeline:

1. sensor selection based on stability across windows
2. variable reduction using the most relevant statistics
3. robust evaluation of multiple `k` values

The final model reduces the original feature space from 175 variables to 47 while keeping the clustering objective focused on meaningful structure.

### Phase 6 — Kafka infrastructure and streaming inference

The project includes a Kafka-based pipeline:

- a producer replays historical measurements as JSON events
- a Spark Structured Streaming consumer reads and validates messages
- an inference pipeline reconstructs final feature vectors and predicts clusters in real time

This provides an end-to-end streaming demonstration without requiring live physical sensors.

## Prerequisites

- Python 3.10+
- Java 17+ for Spark
- at least 8 GB of available RAM
- 35–40 GB of free disk space for the dataset, Spark cache, temp files, and generated outputs

## Installation

On Windows:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

The scripts automatically look for a Java 17+ installation. If none is found, set `JAVA_HOME` manually.

## Execution

### Phase 1 — EDA

```powershell
.\.venv\Scripts\python.exe src\phase1_eda.py `
  --master "local[8]" `
  --shuffle-partitions 32 `
  --sample-fraction 0.01 `
  --seed 42
```

Optional lighter run with a smaller sample:

```powershell
.\.venv\Scripts\python.exe src\phase1_eda.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --sample-fraction 0.002 `
  --no-write-sample-parquet `
  --output-dir outputs\eda_light
```

### Phase 2 — Cleaning

```powershell
.\.venv\Scripts\python.exe src\phase2_cleaning.py --master "local[4]" --shuffle-partitions 16
```

### Phase 3 — Feature engineering

```powershell
.\.venv\Scripts\python.exe src\phase3_features.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --window "5 minutes" `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase3_spark"
```

### Phase 4 — Clustering

```powershell
.\.venv\Scripts\python.exe src\phase4_clustering.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --k-candidates "2,3,4,5,6" `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase4_spark"
```

### Phase 5 — Sensor selection, reduction, and model selection

```powershell
.\.venv\Scripts\python.exe src\phase5_sensor_selection.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --min-window-coverage 0.95 `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase5_sensor"
```

```powershell
.\.venv\Scripts\python.exe src\phase5_feature_reduction.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase5_reduction"
```

```powershell
.\.venv\Scripts\python.exe src\phase5_model_selection.py `
  --master "local[4]" `
  --shuffle-partitions 16 `
  --k-candidates "2,3,4,5,6" `
  --min-cluster-share 0.01 `
  --evaluation-sample-fraction 0.30 `
  --scratch-dir "C:\Users\zahra\AppData\Local\Temp\opencode\phase5_model_selection"
```

## Kafka setup

Docker Desktop must be running. Start the local Kafka broker:

```powershell
docker compose up -d
```

Create the topic:

```powershell
docker exec smart-home-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create --if-not-exists `
  --topic smart-home-events `
  --partitions 1 `
  --replication-factor 1
```

List the topic:

```powershell
docker exec smart-home-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

### Kafka producer

```powershell
.\.venv\Scripts\python.exe src\phase6_kafka_producer.py `
  --bootstrap-server "localhost:9092" `
  --topic "smart-home-events" `
  --start "2020-06-18 09:00:00" `
  --end "2020-06-18 12:00:00" `
  --max-rows 250000 `
  --delay-seconds 0
```

### Spark Structured Streaming consumer

```powershell
.\.venv\Scripts\python.exe src\phase6_streaming_consumer.py `
  --bootstrap-server "localhost:9092" `
  --topic "smart-home-events" `
  --timeout-seconds 1500 `
  --max-offsets-per-trigger 80000
```

### Streaming inference

```powershell
.\.venv\Scripts\python.exe src\phase6_streaming_inference.py `
  --bootstrap-server "localhost:9092" `
  --topic "smart-home-events" `
  --timeout-seconds 900 `
  --max-offsets-per-trigger 70000
```

## Expected outputs

### Phase outputs

```text
outputs/eda/
outputs/phase2_cleaning/
outputs/phase3_features/
outputs/phase4_clustering/
outputs/phase5_sensor_selection/
outputs/phase5_feature_reduction/
outputs/phase5_model_selection/
outputs/phase6_streaming/
```

### Key artifacts

- `eda_report.md` — data quality and exploratory analysis report
- `cleaning_report.md` — cleaning diagnostics and summary
- `feature_report.md` — windowed feature statistics
- `cluster_profiles.csv` — cluster-level summaries
- `k_selection.csv` — KMeans model selection results
- `sensor_selection_report.md` — stable sensors analysis
- `reduced_feature_dictionary.csv` — reduced feature names and meanings
- `consumer_summary.json` — Kafka consumer run summary
- final predictions and outputs for the streaming inference stage

## Main results summary

| Phase | Key result |
| --- | --- |
| 1 — EDA | 247,304,708 measurements, 24 sensors, 182.5 days, 0 missing values, 0 corrupted records |
| 2 — Cleaning | 0 rows removed, 6 suspect values flagged |
| 3 — Windows | 50,220 five-minute windows, wide table with 224 columns |
| 4 — Clustering | 175 variables, validation silhouette = -0.2574 |
| 5 — Final model | 175 → 47 variables, k = 2, validation silhouette = -0.1450 |
| 6 — Streaming | 484,092 messages consumed across 7 micro-batches |

### Cluster interpretation

The project identifies two distinct dense/low-density operating profiles, not a direct label for human activity. The main difference is driven by sensor activity and measurement volume rather than a semantic activity class.

## Interpretation notes

- A high sensor activity rate does not automatically imply a human activity.
- Sensors use different units; they must be standardized before comparison.
- The dataset contains no explicit `activity` label, so supervised classification requires annotations or validated pseudo-labels.
- Quantile estimates are based on a sample, while quality checks cover the full dataset.
- `approx_count_distinct` gives only a high-level estimate of unique IDs and should not be used as proof of duplication.

## Related documentation

- [RAPPORT.md](RAPPORT.md) — complete project synthesis and phase-by-phase interpretation

## License and usage

This project is intended for academic and research use. Please credit the original Mendeley dataset and cite the project report when reusing the analysis or findings.

## Contact

If you want to extend the project, add new clustering methods, improve the streaming pipeline, or integrate other sensor datasets, you can build directly on the existing structure in `src/` and `outputs/`.

---

This README was intentionally rewritten to improve clarity, structure, visual readability, and English phrasing while preserving the technical content of the original project.
