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
- Nombre de variables utilisées : **175**
- Nombre de clusters retenu : **2**
- Silhouette sur la période de validation : **-0.2574**

## Sélection de k

| k | silhouette |
| --- | --- |
| 2 | 0.2039 |
| 3 | 0.1461 |
| 4 | 0.1428 |
| 5 | 0.1925 |
| 6 | 0.1210 |

Le k avec le meilleur score de silhouette est retenu. Ce score mesure la séparation entre clusters ; il ne constitue pas une preuve d'activité réelle.

## Profils des clusters

| cluster | fenêtres | part | capteurs actifs moyens | échantillons moyens | heure moyenne |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 25,636 | 51.05% | 21.00 | 5379.9 | 12.29 |
| 1 | 24,584 | 48.95% | 16.45 | 4449.5 | 10.70 |

Les features les plus différenciantes de chaque cluster sont détaillées dans `cluster_profiles.csv`.

## Lecture des résultats

- Le nombre de clusters a été sélectionné sur un échantillon de la période d'entraînement, puis le modèle final a été réajusté sur toutes les données d'entraînement.
- La silhouette de validation est négative : les centres appris sur la période d'entraînement ne séparent pas correctement les périodes futures. Le clustering est donc techniquement valide mais sa généralisation temporelle doit être considérée avec prudence.
- Les deux clusters sont assez équilibrés, mais leur différence principale semble aussi liée au nombre de capteurs actifs et au volume de mesures.
- Le cluster doit donc être décrit comme un profil de mesures, et non comme une activité humaine certaine.

## Modèle et sorties

- Modèle de preprocessing : `C:\Users\zahra\OneDrive\Desktop\BigData\models\phase4_kmeans\preprocessing`
- Modèle KMeans : `C:\Users\zahra\OneDrive\Desktop\BigData\models\phase4_kmeans\kmeans`
- Affectations : `C:\Users\zahra\OneDrive\Desktop\BigData\data\processed\clustering_5min`
- Dictionnaire des variables : `C:\Users\zahra\OneDrive\Desktop\BigData\outputs\phase4_clustering\model_features.csv`

## Limites

- Le clustering est non supervisé et ne donne pas automatiquement le nom d'une activité.
- Les valeurs nulles représentent une absence de mesure, tandis que zéro représente une mesure inactive.
- Les résultats doivent être interprétés avec le dictionnaire des capteurs et les profils de cluster.
