# Mini-projet Big Data 2 — Smart Home multi-capteurs

Rapport de synthèse des six phases.

---

## 1. Contexte et problématique

Le jeu de données *Multi-sensor Smart Home* (Mendeley) contient les mesures de 24
capteurs domotiques relevés dans une maison pendant 182 jours. La problématique
retenue est la suivante : **peut-on décrire le fonctionnement de la maison par
des profils de mesures, et ces profils peuvent-ils être calculés en continu sur
un flux de données ?**

Deux contraintes ont orienté les choix méthodologiques :

- le dataset ne fournit **aucune étiquette d'activité**, ce qui oriente vers une
  approche non supervisée ;
- l'objectif Big Data impose de travailler sur le volume complet avec PySpark,
  et de démontrer que le modèle entraîné fonctionne ensuite en flux.

---

## 2. Données

| Indicateur | Valeur |
| --- | ---: |
| Mesures lues | 247 304 708 |
| Capteurs déclarés et mesurés | 24 |
| Période couverte | 2020-02-26 → 2020-08-26 (182,5 jours) |
| Mesures par heure (moyenne) | 56 469 |
| Valeurs manquantes ou non finies | 0 |
| Enregistrements corrompus | 0 |
| Mesures orphelines (capteur inconnu) | 0 |
| Valeurs négatives | 6 |
| Zéros | 156 478 448 |
| Valeurs positives | 90 826 254 |

Le dataset est **d'une qualité très élevée** : aucune valeur manquante, aucun
enregistrement illisible, aucune rupture d'intégrité entre `sensor.csv` et les
mesures. La seule anomalie est constituée des 6 valeurs négatives, concentrées
sur un capteur de courant de machine à laver : elles sont physiquement
impossibles et ont été signalées plutôt que supprimées.

---

## 3. Démarche par phase

### Phase 1 — Ingestion et analyse exploratoire

Lecture des 12,4 Go de CSV avec un schéma Spark explicite. Les volumes, périodes
et contrôles d'intégrité sont calculés sur **toutes** les lignes ; les quantiles
et les profils temporels sur un **échantillon aléatoire de 1 %** (2 471 863
lignes, graine 42) pour éviter de saturer le driver.

**Interprétation.** Le volume de zéros (63 %) est le résultat le plus important
de cette phase : une valeur nulle signifie « capteur inactif », pas « mesure
manquante ». Confondre les deux aurait conduit à un nettoyage destructif et
faux. La distinction est conservée dans toute la suite du projet.

### Phase 2 — Nettoyage

**Aucune ligne n'a été supprimée** : le dataset étant intégralement valide, un
nettoyage par filtrage aurait été arbitraire. Les 6 valeurs impossibles sont
signalées par un indicateur `is_suspect_value`, afin qu'elles soient exclues des
statistiques sans perdre la trace de leur existence.

**Interprétation.** Cette phase illustre une règle utile : nettoyer, ce n'est pas
toujours effacer. Ici le bon geste était l'annotation.

### Phase 3 — Construction des caractéristiques temporelles

Agrégation par fenêtres de 5 minutes et par capteur, puis passage d'une table
longue (942 862 lignes) à une table large (50 220 fenêtres × 224 colonnes).

Par capteur et par fenêtre : nombre d'échantillons, moyenne, minimum, maximum,
écart-type, dernière valeur, nombre de valeurs positives et nulles. Plus des
variables de contexte : nombre de capteurs actifs, volume total, heure, jour de
la semaine.

**Interprétation.** La fenêtre de 5 minutes est le compromis retenu : assez fine
pour distinguer une occupation d'une absence, assez large pour que chaque
capteur produise une poignée de mesures exploitables. La conservation des
`null` pour un capteur absent d'une fenêtre est essentielle : elle distingue
l'absence de mesure de la mesure inactive.

### Phase 4 — Standardisation et clustering

KMeans sur les 175 variables de la table complète, avec séparation chronologique
70 % / 15 % / 15 % et ajustement de l'imputation et du standard scaler sur la
seule période d'entraînement, pour éviter toute fuite de données.

**Résultat : silhouette de validation de −0,2574.** Négative.

**Interprétation.** C'est le premier signal que le modèle ne sépare pas des
périodes futures. Un KMeans appliqué à des mesures brutes retrouve surtout la
densité du trafic, et celle-ci évolue avec l'usure et l'évolution du foyer. La
séquence de réduction des variables commence ici.

### Phase 5 — Réduction et sélection du modèle

Quatre étapes successives :

1. **Sélection des capteurs stables** : 8 des 24 capteurs couvrent au moins
   95 % des fenêtres, les 16 autres n'apparaissent que sur une partie de la
   période (certains démarrent mi-mars, d'autres n'existent qu'en été).
2. **Réduction des variables** : 175 → 47 variables, soit **73,14 % de
   réduction**, en conservant 5 statistiques par capteur et les 7 variables de
   contexte.
3. **Entraînement du modèle réduit** : la silhouette de validation s'améliore de
   −0,2574 à **−0,1450**. L'amélioration est réelle mais insuffisante.
4. **Sélection robuste de k** : un critère de taille minimale (1 % des fenêtres)
   écarte les k = 3 à 6, qui produisent tous un micro-cluster de 0,15 %.

Modèle final : **k = 2**, 47 variables.

| Cluster | Fenêtres | Part | Capteurs actifs | Échantillons | Heure moyenne |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 26 133 | 52,04 % | 20,54 | 5 322,1 | 13,26 |
| 1 | 24 087 | 47,96 % | 16,86 | 4 492,9 | 9,62 |

**Interprétation.** Les deux clusters sont équilibrés, mais leur différence
porte d'abord sur le **nombre de capteurs actifs** et le **volume de mesures**.
Un cluster décrit donc un profil de densité de mesures, pas une activité humaine
identifiée. Le nommer « présence » ou « absence » serait une erreur
d'interprétation.

### Phase 6 — Streaming Kafka

Architecture complète : producteur Kafka → Structured Streaming → agrégation par
fenêtre → modèle KMeans.

Le topic a été alimenté par trois rejeux historiques de profils opposés :

| Rejeu | Période | Messages | Profil |
| --- | --- | ---: | --- |
| Nuit | 2020-06-17 21:00 → 23:59 | 209 736 | 23 capteurs |
| Journée dense | 2020-06-18 09:00 → 11:59 | 211 084 | 23 capteurs |
| Journée creuse | 2020-06-29 07:00 → 09:29 | 63 272 | 9 capteurs |

Résultat du consommateur : **484 092** messages JSON décodés et écrits en Parquet,
soit la totalité du topic.

Résultat de l'inférence :

| Indicateur | Valeur |
| --- | ---: |
| Messages consommés | 484 092 |
| Micro-batchs | 7 |
| Fenêtres prédites | 108 |
| Fenêtres distinctes | 102 |
| Cluster 0 | 78 (72,2 %) |
| Cluster 1 | 30 (27,8 %) |

**Interprétation.** C'est le résultat le plus parlant du projet. Les journées
denses (23 capteurs, ≈ 5 800 échantillons par fenêtre) et les journées creuses
(9 capteurs, ≈ 1 900 échantillons) **ne se rattachent pas au même cluster**,
alors qu'elles ne durent que quelques heures. Le modèle discrimine donc la
densité des mesures, ce qui confirme l'interprétation de la phase 5 — et le
streaming reproduit fidèlement le comportement hors-ligne.

Trois défauts ont été corrigés pendant cette phase :

- **décalage de fuseau de 2 h** : la conversion des timestamps en `datetime`
  Python appliquait le fuseau local de la machine, décalant toutes les fenêtres ;
  l'horodatage est désormais rendu par Spark SQL dans le fuseau de la session ;
- **`numInputRows` surévalué d'un facteur 3** : il cumule les lignes relues lors
  des ré-exécutions de tâches ; le compteur est lu dans les offsets Kafka ;
- **`StackOverflowError` dans la query** : causé par la combinaison
  `withWatermark` + `dropDuplicates`, qui impose un magasin d'état local instable
  sous Windows et n'apporte rien à une agrégation par micro-batch.

---

## 4. Réponse à la problématique

Le fonctionnement de la maison **peut** être décrit par des profils de mesures,
et ces profils **se calculent en continu**. En revanche, ces profils mesurent une
densité d'activité, et non une activité humaine : le jeu de données ne contient
aucune annotation qui permettrait de trancher.

Le clustering est donc **exploratoire**. La phase 6 démontre l'architecture, pas
une précision de production.

---

## 5. Limites

- La silhouette de validation reste négative (−0,145) : les centres appris sur
  la période d'entraînement ne séparent pas correctement les périodes futures.
- Le clustering est non supervisé : il donne un profil de mesures, pas un nom
  d'activité.
- Les capteurs n'utilisent pas les mêmes unités : ils ne sont comparables qu'après
  standardisation.
- L'inférence applique le modèle par micro-batch : une fenêtre à cheval sur deux
  micro-batchs est prédite deux fois. Un déploiement réel utiliserait une
  agrégation avec état et marque temporelle.
- Le flux est un rejeu historique, pas un flux de capteurs physiques temps réel.
- Les quantiles de la phase 1 sont des estimations sur 1 % des lignes, alors que
  les volumes et les contrôles d'intégrité portent sur les 247 304 708 lignes.

---

## 6. Pistes d'amélioration

- Ajouter un volume de données plus récent, ou d'autres foyers, pour vérifier si
  la séparation se stabilise dans le temps.
- Construire des pseudo-étiquettes à partir des capteurs de contact et de
  mouvement, puis mesurer si les clusters reconstruits leur correspondent.
- Remplacer l'agrégation par micro-batch par une agrégation avec état.
- Passer à un modèle supervisé dès que des annotations d'activité sont
  disponibles.

---

## 7. Reproduction

Les commandes exactes de chaque phase sont documentées dans `README.md`.
L'ordre d'exécution est le suivant :

```text
phase1_eda.py               → outputs/eda/
phase2_cleaning.py          → data/processed/measurements_clean/
phase3_features.py          → data/processed/features_5min/
phase4_clustering.py        → models/ et outputs/phase4_clustering/
phase5_sensor_selection.py  → outputs/phase5_sensor_selection/
phase5_feature_reduction.py → data/processed/features_5min/window_features_reduced/
phase4_clustering.py        → modèle réduit (47 variables, k=2)
phase5_model_selection.py   → outputs/phase5_model_selection/
phase6_kafka_producer.py    → topic smart-home-events
phase6_streaming_consumer.py→ outputs/phase6_streaming/parsed_events/
phase6_streaming_inference.py → outputs/phase6_streaming/cluster_predictions/
```

Le broker Kafka local est fourni par `docker-compose.yml` (`docker compose up -d`).
