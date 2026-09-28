# Phase 4 — Standardisation et clustering KMeans

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

- fin entraînement : 2020-07-04 07:35:00+00:00
- fin validation : 2020-07-30 11:25:00+00:00

Les périodes de validation et de test ne servent pas à ajuster l'imputation, le scaler ou les centres KMeans.

## Résultat de la séparation

- Fenêtres d'entraînement : **35,154**
- Fenêtres de validation : **7,533**
- Fenêtres de test : **7,533**
- Nombre de variables utilisées : **47**
- Nombre de clusters retenu : **2**
- Silhouette sur la période de validation : **-0.1450**

## Sélection de k

| k | silhouette |
| --- | --- |
| 2 | 0.1628 |

Le k avec le meilleur score de silhouette est retenu. Ce score mesure la séparation entre clusters ; il ne constitue pas une preuve d'activité réelle.

## Profils des clusters

| cluster | fenêtres | part | capteurs actifs moyens | échantillons moyens | heure moyenne |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 26,133 | 52.04% | 20.54 | 5322.1 | 13.26 |
| 1 | 24,087 | 47.96% | 16.86 | 4492.9 | 9.62 |

Les features les plus différenciantes de chaque cluster sont détaillées dans `cluster_profiles.csv`.

## Lecture des résultats

- Le nombre de clusters a été sélectionné sur un échantillon de la période d'entraînement, puis le modèle final a été réajusté sur toutes les données d'entraînement.
- La silhouette de validation est négative : les centres appris sur la période d'entraînement ne séparent pas correctement les périodes futures. Le clustering est donc techniquement valide mais sa généralisation temporelle doit être considérée avec prudence.
- Les deux clusters sont assez équilibrés, mais leur différence principale semble aussi liée au nombre de capteurs actifs et au volume de mesures.
- Le cluster doit donc être décrit comme un profil de mesures, et non comme une activité humaine certaine.

## Graphiques

Les graphiques d'interprétation se trouvent dans `C:\Users\zahra\OneDrive\Desktop\BigData\outputs\phase5_final_model\charts` :

- `silhouette_by_k.png`
- `cluster_sizes.png`
- `cluster_context.png`
- `cluster_differentiating_features.png`
- `cluster_distribution_by_hour.png`
- `cluster_distribution_by_day.png`

## Modèle et sorties

- Modèle de preprocessing : `C:\Users\zahra\OneDrive\Desktop\BigData\models\phase5_final_kmeans\preprocessing`
- Modèle KMeans : `C:\Users\zahra\OneDrive\Desktop\BigData\models\phase5_final_kmeans\kmeans`
- Affectations : `C:\Users\zahra\OneDrive\Desktop\BigData\data\processed\clustering_5min_final`
- Dictionnaire des variables : `C:\Users\zahra\OneDrive\Desktop\BigData\outputs\phase5_final_model\model_features.csv`

## Limites

- Le clustering est non supervisé et ne donne pas automatiquement le nom d'une activité.
- Les valeurs nulles représentent une absence de mesure, tandis que zéro représente une mesure inactive.
- Les résultats doivent être interprétés avec le dictionnaire des capteurs et les profils de cluster.
