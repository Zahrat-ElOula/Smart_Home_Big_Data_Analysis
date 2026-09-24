# Rapport EDA — Smart Home

Généré le 2026-09-24 20:33:38+00:00 UTC.

## 1. Démarche

1. Lire `sensor.csv` pour obtenir les déclarations des capteurs et leur type de mesure.
2. Lire les deux fichiers de mesures avec un schéma Spark explicite.
3. Uniformiser les types, convertir les timestamps et conserver les enregistrements invalides comme métriques de qualité.
4. Calculer les volumes, périodes, min/max, moyennes et contrôles de qualité.
5. Joindre les mesures aux métadonnées sur `sensor_id`.
6. Construire un échantillon aléatoire sans remise de **0.2000%**, soit **495,890 lignes** (seed `42`), puis conserver pour les profils temporels uniquement les événements à timestamp et valeur finis, dont le capteur est déclaré.
7. Produire des tableaux CSV, des graphiques et une synthèse Markdown.

Les volumes, périodes, min/max, valeurs manquantes et l'intégrité `sensor_id` sont calculés sur **toutes les lignes**. Les quantiles et les profils temporels sont calculés sur l'échantillon afin de limiter les recomputations et la sortie sur le driver.

## 2. Inventaire des fichiers

| Fichier | Taille (Go) | En-tête |
| --- | --- | --- |
| sensor.csv | 2e-06 | `sensor_id,node_id,type,name` |
| sensor_sample_int.csv | 9.14522 | `value_id,sensor_id,timestamp,value` |
| sensor_sample_float.csv | 3.26578 | `value_id,sensor_id,timestamp,value` |

## 3. Résumé des mesures

| Fichier | Lignes | Capteurs | Début | Fin | Valeurs valides | Négatives | Enregistrements corrompus |
| --- | --- | --- | --- | --- | --- | --- | --- |
| sensor_sample_float.csv | 60479847 | 9 | 2020-02-26 12:22:36.417725 | 2020-08-26 12:00:00.927742 | 100.0000% | 6 | 0 |
| sensor_sample_int.csv | 186824861 | 15 | 2020-02-26 01:00:00.087705 | 2020-08-26 12:30:24.290233 | 100.0000% | 0 | 0 |

## 4. Qualité globale

| Contrôle | Résultat | Interprétation |
| --- | --- | --- |
| Timestamp valide | 100.0000% | Proportion de mesures exploitables dans le temps. |
| Valeur numérique valide | 100.0000% | Exclut null, NaN et infini. |
| sensor_id manquant | 0 | Doit être proche de zéro pour pouvoir joindre au métadonnées. |
| Enregistrement CSV corrompu | 0 | Ligne qui ne respecte pas complètement le schéma. |
| Valeurs négatives | 6 | Peuvent être normales pour une température ; ne pas les supprimer sans vérifier le capteur. |
| Capteurs sans mesures | 0 | Capteurs déclarés dans sensor.csv mais absents des mesures. |
| Mesures sans capteur déclaré | 0 | Anomalie d'intégrité : ces lignes ne peuvent pas être nommées. |
| Capteurs avec type source incohérent | 0 | Le fichier INT/FLOAT ne correspond pas au type déclaré ou les deux fichiers sont mélangés. |
| Mesures avec type incohérent | 0 | Volume total associé aux incohérences de type. |
| Capteurs présents dans les deux fichiers | 0 | Un même sensor_id ne devrait normalement pas être mesuré comme INT et FLOAT. |
| sensor_id dupliqués dans métadonnées | 0 | Après tri, une ligne déterministe est conservée et le conflit est signalé. |

## 5. Capteurs les plus actifs

| Capteur | Type | Pièce | Mesures | Début | Fin | Moyenne | Min | Max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| livingroom/tv/light | INT | livingroom | 14865503 | 2020-02-26 01:00:00.485394 | 2020-08-26 12:30:23.469574 | 962.554 | 3 | 1,024.00 |
| bedroom/ambience/motion | INT | bedroom | 14860826 | 2020-02-26 01:00:00.087705 | 2020-08-26 12:30:23.464273 | 0.0445278 | 0 | 1 |
| kitchen/ambience/motion | INT | kitchen | 14855169 | 2020-02-26 01:00:00.596550 | 2020-08-26 12:30:24.166189 | 0.0217085 | 0 | 1 |
| balcon/door/contact | INT | balcon | 14828029 | 2020-02-26 12:18:07.792391 | 2020-08-26 12:30:23.900552 | 0.0515776 | 0 | 1 |
| livingroom/ambience/motion | INT | livingroom | 14780517 | 2020-02-26 01:00:00.214702 | 2020-08-26 12:30:24.069939 | 0.00767727 | 0 | 1 |
| corridor/ambience/motion | INT | corridor | 14716412 | 2020-02-26 01:00:00.698952 | 2020-08-26 12:30:23.509930 | 0.0275596 | 0 | 1 |
| kitchen/fridge/contact | INT | kitchen | 14700238 | 2020-02-27 18:38:31.785768 | 2020-08-26 12:30:24.290233 | 0.0184606 | 0 | 1 |
| entrance/door/contact | INT | entrance | 13675728 | 2020-02-26 01:00:00.540535 | 2020-08-26 12:30:23.349162 | 0.00115504 | 0 | 1 |
| kitchen/stove/light | INT | kitchen | 13003555 | 2020-02-26 01:00:00.107997 | 2020-08-24 01:42:03.540687 | 492.071 | 0 | 1,024.00 |
| bedroom/ambience_under_the_bed/motion | INT | bedroom | 12385867 | 2020-03-23 22:03:08.688680 | 2020-08-26 12:30:23.418057 | 0.00607216 | 0 | 1 |

## 6. Interprétation des résultats

- **Volume** : les deux fichiers de mesures contiennent **247,304,708 lignes**. Cette taille justifie Spark et interdit un chargement complet avec Pandas.
- **Période** : les mesures couvrent **2020-02-26 01:00:00.087705** à **2020-08-26 12:30:24.290233**, soit environ **182.48 jours**.
- **Capteurs** : **24** capteurs produisent des mesures. Les dix plus actifs sont affichés dans le tableau.
- **Heure la plus active** : 18:00–18:59 avec 21,411 mesures dans l'échantillon.
- **Jour le plus actif** : 2020-06-18 avec 3,427 mesures dans l'échantillon.
- **Jour de semaine le plus chargé** : Samedi.
- **Attention à l’interprétation** : les capteurs n'ont pas la même unité. Une valeur de 1024 pour une lumière ou une pression n’est pas directement comparable à une valeur de courant électrique.
- **Objectif ML** : aucune colonne d’activité étiquetée n’est présente. L’étape suivante devra donc créer des fenêtres temporelles puis, soit produire des pseudo-étiquettes validées, soit utiliser une méthode non supervisée.
## 7. Limites à connaître

- Les distributions de `value_quantiles_sample.csv` sont **approchatives** (`percentile_approx`) et reposent sur l'échantillon.
- Les profils d'activité sont des volumes de mesures valides, pas encore des activités humaines validées.
- `observation_span_ratio` compare seulement la période entre la première et la dernière mesure ; il ne mesure pas la couverture réelle des instants intermédiaires.
- Le comptage approximatif `approx_count_distinct` (HLL) fournit un ordre de grandeur, mais son écart avec `count` ne prouve pas la présence de doublons : une vérification exacte serait beaucoup plus coûteuse.
- Le dataset décrit un utilisateur unique. Les résultats ne doivent pas être généralisés à plusieurs foyers.
- Une valeur anormale n'est pas forcément une erreur : une température peut être négative et un pic de courant peut représenter une utilisation réelle.

## 8. Sorties

- `tables/dataset_inventory.csv`
- `tables/sensor_metadata.csv`
- `tables/summary_by_file.csv`
- `tables/overall_quality.json`
- `tables/sensor_summary.csv`
- `tables/value_quantiles_sample.csv`
- `tables/daily_activity_sample.csv`
- `tables/activity_by_hour_of_day.csv`
- `tables/activity_by_day_of_week.csv`
- `charts/daily_activity.png`
- `charts/activity_by_hour.png`
- `charts/top_sensors.png`
- `charts/sensor_coverage.png`
