# Phase 5 — Étape 4 : sélection robuste de k

## Démarche

1. Utiliser la table réduite de 47 variables.
2. Conserver la séparation chronologique 70 % / 15 % / 15 %.
3. Ajuster l'imputation et le standard scaler sur l'entraînement uniquement.
4. Entraîner KMeans sur toute la période d'entraînement pour chaque k.
5. Mesurer la silhouette sur l'entraînement, la validation et le test.
6. Écarter un k dont le plus petit cluster représente moins de **1%** des fenêtres d'entraînement.
7. Retenir le k éligible avec la meilleure silhouette de validation.

## Résultat

- Variables : **47**
- Entraînement : **35,154**
- Validation : **7,533**
- Test : **7,533**
- Part minimale imposée : **1%**
- k retenu : **2**

| k | Silhouette entraînement | Silhouette validation | Silhouette test | Petit cluster | Éligible |
| ---: | ---: | ---: | ---: | ---: | --- |
| 2 | 0.1730 | -0.1450 | -0.1156 | 47.11% | Oui |
| 3 | 0.1889 | -0.1437 | -0.1340 | 0.15% | Non |
| 4 | 0.2268 | 0.1440 | 0.1223 | 0.15% | Non |
| 5 | 0.2308 | 0.0691 | -0.0475 | 0.15% | Non |
| 6 | 0.2539 | 0.1064 | 0.1965 | 0.15% | Non |

## Lecture

Le k=3 obtenu précédemment sur l'échantillon d'entraînement produit un cluster de seulement 0,11 % des fenêtres après entraînement complet. Ce cluster est donc trop petit pour être considéré comme un profil stable. Le critère de taille minimale permet d'éviter ce choix.

k=2 est retenu parmi les valeurs éligibles. La lecture finale doit comparer la silhouette de validation et la stabilité des profils, plutôt que la seule silhouette d'entraînement.

## Graphique

- `silhouette_train_validation_test.png`

## Limites

- Le clustering reste non supervisé.
- Un score de validation négatif indique une difficulté de généralisation temporelle.
- Le critère de taille minimale est une règle de robustesse, pas une preuve d'activité.
