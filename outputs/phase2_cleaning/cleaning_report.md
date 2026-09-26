# Phase 2 — Nettoyage et préparation

## Résultat

- Lignes lues : **247,304,708**
- Lignes conservées : **247,304,708**
- Lignes supprimées : **0**
- Valeurs suspectes signalées : **6**
- Capteurs déclarés : **24**
- Mesures sans capteur déclaré conservées : **0**

## Règles appliquées

1. Vérification des en-têtes CSV.
2. Conversion des types avec un schéma Spark explicite.
3. Jointure des mesures avec `sensor.csv` sur `sensor_id`.
4. Suppression des lignes sans identifiant, timestamp, valeur finie ou capteur déclaré.
5. Conservation des valeurs négatives de courant avec l'indicateur `is_suspect_value`.
6. Les zéros sont conservés : ils représentent un état inactif normal.
7. Les données brutes ne sont jamais modifiées.

## Résultat qualité après nettoyage

- Valeurs finies : **247,304,708**
- Enregistrements corrompus : **0**
- Valeurs négatives : **6**

## Format de sortie

Les données nettoyées sont enregistrées en Parquet, partitionnées par `source_type` :

```text
C:\Users\zahra\OneDrive\Desktop\BigData\data\processed\measurements_clean
```

## Étape suivante

Ces données peuvent être regroupées en fenêtres de cinq minutes pour construire les variables utilisées par Spark ML.
