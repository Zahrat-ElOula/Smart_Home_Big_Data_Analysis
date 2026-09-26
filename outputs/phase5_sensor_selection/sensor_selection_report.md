# Phase 5 — Étape 1 : sélection des capteurs stables

## Démarche

1. Lecture de la table longue produite par la phase 3.
2. Calcul du nombre total de fenêtres observées : **50,220**.
3. Calcul, pour chaque capteur, du nombre de fenêtres dans lesquelles il apparaît.
4. Calcul de la couverture : `observed_window_count / global_window_count`.
5. Sélection des capteurs dont la couverture est au moins **95%**.

## Résultat

- Capteurs analysés : **24**
- Capteurs stables sélectionnés : **8**
- Seuil de couverture : **95%**
- Première fenêtre : `2020-02-26 01:00:00`
- Dernière fenêtre : `2020-08-26 12:30:00`

| Capteur | Pièce | Type | Fenêtres observées | Couverture | Sélection |
| --- | --- | --- | ---: | ---: | --- |
| livingroom/tv/light | livingroom | INT | 49,646 | 98.86% | Oui |
| kitchen/ambience/motion | kitchen | INT | 49,643 | 98.85% | Oui |
| bedroom/ambience/motion | bedroom | INT | 49,636 | 98.84% | Oui |
| balcon/door/contact | balcon | INT | 49,511 | 98.59% | Oui |
| livingroom/ambience/motion | livingroom | INT | 49,359 | 98.29% | Oui |
| corridor/ambience/motion | corridor | INT | 49,152 | 97.87% | Oui |
| kitchen/fridge/contact | kitchen | INT | 49,134 | 97.84% | Oui |
| bedroom/bed/pressure | bedroom | INT | 48,391 | 96.36% | Oui |
| entrance/door/contact | entrance | INT | 45,659 | 90.92% | Non |
| livingroom/couch/pressure | livingroom | INT | 43,897 | 87.41% | Non |
| kitchen/stove/light | kitchen | INT | 43,497 | 86.61% | Non |
| bedroom/ambience_under_the_bed/motion | bedroom | INT | 41,779 | 83.19% | Non |
| bedroom/weightscale/pressure | bedroom | INT | 41,234 | 82.11% | Non |
| kitchen/sandwichmaker/current | kitchen | FLOAT | 37,403 | 74.48% | Non |
| kitchen/kettle/current | kitchen | FLOAT | 37,390 | 74.45% | Non |
| kitchen/coffeemaker/current | kitchen | FLOAT | 37,381 | 74.43% | Non |
| kitchen/dishwasher/current | kitchen | FLOAT | 37,297 | 74.27% | Non |
| bathroom/washingmachine/current | bathroom | FLOAT | 35,210 | 70.11% | Non |
| corridor/ilifeRobot/current | corridor | FLOAT | 30,438 | 60.61% | Non |
| kitchen/microwave/current | kitchen | FLOAT | 27,238 | 54.24% | Non |
| bathroom/ambience/light | bathroom | INT | 26,963 | 53.69% | Non |
| bathroom/ambience/temperature | bathroom | FLOAT | 22,687 | 45.18% | Non |
| bathroom/ambience/humidity | bathroom | FLOAT | 22,670 | 45.14% | Non |
| bathroom/ambience/motion | bathroom | INT | 17,647 | 35.14% | Non |

## Interprétation

La couverture mesure la présence réelle d'un capteur dans les fenêtres observées. Elle est plus informative qu'un simple ratio entre la première et la dernière mesure, car un capteur peut avoir une longue période sans données.

Les capteurs non sélectionnés restent disponibles pour une analyse descriptive, mais ne seront pas utilisés dans la première version du modèle de clustering. Cette sélection réduit le biais lié à l'activation progressive des capteurs et devrait améliorer la stabilité temporelle du modèle.

## Sorties

- `C:\Users\zahra\OneDrive\Desktop\BigData\outputs\phase5_sensor_selection\stable_sensor_selection.csv`
- `C:\Users\zahra\OneDrive\Desktop\BigData\outputs\phase5_sensor_selection\stable_sensor_names.txt`
- `C:\Users\zahra\OneDrive\Desktop\BigData\outputs\phase5_sensor_selection\sensor_selection_summary.json`
