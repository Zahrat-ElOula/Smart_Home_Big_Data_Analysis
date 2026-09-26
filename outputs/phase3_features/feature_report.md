# Phase 3 — Construction des caractéristiques temporelles

## Démarche

1. Lecture des mesures nettoyées en Parquet.
2. Regroupement des événements par fenêtre de **5 minutes** et par capteur.
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

- Lignes dans la table longue : **942,862**
- Fenelles temporelles : **50,220**
- Capteurs utilisés : **24**
- Lignes dans la table large : **50,220**
- Colonnes de la table large : **224**
- Valeurs suspectes exclues des statistiques : **6**

La table large contient une ligne par fenêtre observée (fenêtre contenant au moins un événement), des variables temporelles (`hour`, `day_of_week`, `is_weekend`) et une colonne par statistique de capteur. Les valeurs manquantes d'un capteur dans une fenêtre restent à `null` afin de distinguer l'absence de mesures d'une valeur réellement nulle.

## Sorties

```text
C:\Users\zahra\OneDrive\Desktop\BigData\data\processed\features_5min\sensor_features_long
C:\Users\zahra\OneDrive\Desktop\BigData\data\processed\features_5min\window_features_wide
C:\Users\zahra\OneDrive\Desktop\BigData\outputs\phase3_features\feature_dictionary.csv
```

## Limites

Cette phase construit les variables mais ne standardise pas encore les données. La standardisation devra être fitted uniquement sur la période d'entraînement du futur modèle pour éviter la fuite de données.
