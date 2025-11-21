
# Python DuckDB - Pipeline de données orchestré

Ce projet permet de gérer un flux complet de données depuis l'import de fichiers sources.
Il inclut la transformation et l'agrégation des données pour obtenir des résultats prêts à l'analyse.
Les résultats finaux peuvent être exportés pour usage externe.

## Démarrage rapide

### Exécution classique (manuelle)
```bash
cd macro/
python3 python-run-process.py
```

### Orchestration avec Dagster
```bash
# Lancer l'interface web principale (port 3000)
cd macro/
python3 dagster-run-process-launch.py

# Lancer l'exemple sample (port 3001)
cd sample/macro/
python3 dagster-run-process-sample-launch.py

# Arrêter Dagster
python3 dagster-stop.py
```

**Interfaces web:**
- Principal: http://localhost:3000
- Sample: http://localhost:3001

## Architecture du flux de données

```
    CSV Files               DuckDB Database              CSV Files
    (Source)                  (Processing)                (Output)
       │                          │                          │
       │                          │                          │
       ▼                          ▼                          ▼
  ┌─────────┐              ┌────────────┐            ┌──────────┐
  │ data/   │   source.py  │database.   │  results.py│  data/   │
  │ source/ │─────────────▶│  duckdb    │───────────▶│processed/│
  │         │   (import)   │            │  (export)  │  final/  │
  │ *.csv   │              │  Tables:   │            │  *.csv   │
  └─────────┘              │  - SOURCE  │            └──────────┘
                           │  - TRANS.  │
                           │  - SUMMARY │
                           └────────────┘
                                 │
                                 │ transforming.py
                                 │ (transform & aggregate)
                                 ▼
                           ┌────────────┐
                           │   Python   │
                           │  + Pandas  │
                           │  + DuckDB  │
                           │  (Engine)  │
                           └────────────┘
```

## Structure du projet

```
project/
├── data/                   
│   ├── source/             Données brutes importées depuis CSV ou autres sources
│   ├── processed/          Données transformées prêtes à être utilisées
│   └── final/              Données finales ou résultats finaux
│
├── db/                     
│   └── database.duckdb     Base de données DuckDB principale du projet
│
├── scripts/                
│   ├── source.py           Script d'import des données depuis les fichiers sources
│   ├── transforming.py     Script pour transformer et agréger les données
│   └── results.py          Script pour générer et exporter les résultats finaux
│
├── macro/                  Scripts d'orchestration
│   ├── python-run-process.py              Exécution classique du pipeline
│   ├── dagster-run-process.py             Définition Dagster du pipeline
│   ├── dagster-run-process-launch.py      Lancement de Dagster (port 3000)
│   ├── dagster-stop.py                    Arrêt de tous les processus Dagster
│   ├── dagster.log                        Logs Dagster
│   ├── dagster.pid                        PID du processus Dagster
│   └── execution.log                      Logs d'exécution du pipeline
│
├── sample/                 Dossier contenant un exemple complet avec données de test
│   ├── data/
│   │   └── source/
│   │       └── SAMPLE_DATA.csv           Données d'exemple
│   ├── db/
│   │   └── database.duckdb
│   ├── scripts/            Scripts fonctionnels pour l'exemple
│   └── macro/
│       ├── python-run-process-sample.py
│       ├── dagster-run-process-sample.py
│       └── dagster-run-process-sample-launch.py (port 3001)
│
├── requirements.txt        Dépendances: pandas, duckdb, dagster, dagster-webserver
├── python-duckdb.py        Générateur de structure de projet
└── README.md               Cette documentation
```

## Modes d'exécution

### 1. Mode classique (python-run-process.py)
Exécution directe et unique du pipeline:
- Import des données
- Transformation
- Export des résultats
- Logs dans `execution.log`

### 2. Mode Dagster (orchestration)
Exécution orchestrée avec interface web:
- **Visualisation** du pipeline sous forme de graphe
- **Scheduling** automatique toutes les 5 minutes
- **Monitoring** en temps réel
- **Historique** des exécutions
- **Logs** centralisés

#### Avantages de Dagster:
- Interface graphique intuitive
- Gestion des dépendances entre assets
- Retry automatique en cas d'erreur
- Métriques et observabilité
- Exécution en arrière-plan

## Scripts utilitaires

### dagster-run-process-launch.py
Lance Dagster en arrière-plan avec:
- Détection de processus existant (évite les doublons)
- Gestion du PID dans `dagster.pid`
- Logs redirigés vers `dagster.log`
- Survit à la fermeture du terminal

### dagster-stop.py
Arrête tous les processus Dagster:
- Kill de tous les processus `dagster dev`
- Nettoyage des fichiers PID
- Vérification de l'arrêt complet

## Installation

```bash
pip install -r requirements.txt
```

Dépendances:
- `pandas` - Manipulation de données
- `duckdb` - Base de données analytique
- `dagster` - Framework d'orchestration
- `dagster-webserver` - Interface web Dagster

## Exemple avec données de test

Le dossier `sample/` contient un exemple complet fonctionnel:

```bash
cd sample/macro/

# Exécution classique
python3 python-run-process-sample.py

# Avec Dagster (port 3001)
python3 dagster-run-process-sample-launch.py
```

Données d'exemple: `sample/data/source/SAMPLE_DATA.csv`

## Ports réseau

- **Principal**: Port 3000 (http://localhost:3000)
- **Sample**: Port 3001 (http://localhost:3001)

Les deux instances peuvent fonctionner simultanément sans conflit.

## Logs

- `macro/execution.log` - Logs du pipeline (import, transformation, export)
- `macro/dagster.log` - Logs du serveur Dagster
- `macro/dagster.pid` - PID du processus Dagster actif

