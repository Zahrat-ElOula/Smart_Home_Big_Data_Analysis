# Phase 5 — Étape 2 : réduction des variables

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

- `livingroom/tv/light`
- `kitchen/ambience/motion`
- `bedroom/ambience/motion`
- `balcon/door/contact`
- `livingroom/ambience/motion`
- `corridor/ambience/motion`
- `kitchen/fridge/contact`
- `bedroom/bed/pressure`

## Résultat

- Colonnes de la table large originale : **224**
- Variables du modèle originales : **175**
- Capteurs utilisés : **8**
- Statistiques conservées par capteur : **5**
- Variables du modèle réduites : **47**
- Variables supprimées : **128**
- Réduction : **73.14%**
- Lignes conservées : **50,220**
- Colonnes de la table réduite : **51**

## Interprétation

La réduction diminue le nombre de variables de **175** à **47**. Elle supprime principalement les capteurs disponibles seulement sur une partie de la période et les statistiques redondantes.

Cette opération devrait réduire la variance du modèle et améliorer sa stabilité temporelle. Elle ne garantit cependant pas à elle seule une silhouette positive : la prochaine étape devra réentraîner et comparer les modèles.

## Sorties

- Table Parquet : `C:\Users\zahra\OneDrive\Desktop\BigData\data\processed\features_5min\window_features_reduced`
- Dictionnaire : `C:\Users\zahra\OneDrive\Desktop\BigData\outputs\phase5_feature_reduction\reduced_feature_dictionary.csv`
- Résumé : `C:\Users\zahra\OneDrive\Desktop\BigData\outputs\phase5_feature_reduction\feature_reduction_summary.json`
