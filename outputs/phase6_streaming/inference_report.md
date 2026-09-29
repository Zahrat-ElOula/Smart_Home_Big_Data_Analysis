# Phase 6, étape 4 — Inférence KMeans sur le flux Kafka

## Démarche

1. Lecture du topic `smart-home-events` avec la source Kafka de Spark Structured Streaming.
2. Décodage de chaque message JSON en événement de mesure.
3. Regroupement des événements par fenêtre de cinq minutes, comme en phase 3.
4. Reconstruction des 47 variables du modèle final.
5. Application du modèle de preprocessing puis du modèle KMeans k=2 enregistré en phase 5.
6. Écriture du cluster prédit pour chaque fenêtre.

Chaque micro-batch est traité de façon autonome (`foreachBatch`) : le topic est
relu depuis le début à chaque exécution, le checkpoint étant supprimé au démarrage.

## Résultat

- Micro-batchs traités : **7**
- Messages Kafka consommés : **484,092**
- Fenêtres prédites : **108**
- Fenêtres distinctes : **102**
- Première fenêtre : `2020-06-17 21:00:00`
- Dernière fenêtre : `2020-06-29 09:25:00`

| cluster | fenêtres | part |
| --- | ---: | ---: |
| 0 | 78 | 72.22% |
| 1 | 30 | 27.78% |

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
