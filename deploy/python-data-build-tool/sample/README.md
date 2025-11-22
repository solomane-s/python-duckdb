
# Python DuckDB - Pipeline de données orchestré

Ce projet permet de gérer un flux complet de données depuis l'import de fichiers sources.
Il inclut la transformation et l'agrégation des données pour obtenir des résultats prêts à l'analyse.
Les résultats finaux peuvent être exportés pour usage externe.

## Configuration de la base de données

**Type de base de données:** DuckDB
**Fichier:** database.duckdb
**Module:** duckdb

## Démarrage rapide

### Exécution classique (manuelle)
```bash
cd macro/
python3 python-run-process.py
```

### Orchestration avec Dagster
```bash
# Lancer l'interface web principale (ports selon DB: 3000 DuckDB, 4000 SQLite, 5000 Commune)
cd macro/
python3 dagster-run-process-launch.py

# Lancer l'exemple sample (port 3001)
cd sample/macro/
python3 dagster-run-process-sample-launch.py

# Arrêter Dagster
# Pour le principal:
python3 dagster-run-process-stop.py

# Pour le sample:
python3 dagster-run-process-sample-stop.py
```

**Interfaces web selon le type de base de données:**
- **DuckDB**: Principal (http://localhost:3000), Sample (http://localhost:3001)
- **SQLite**: Principal (http://localhost:4000), Sample (http://localhost:4001)
- **Database Commune**: Principal (http://localhost:5000), Sample (http://localhost:5001)

## Architecture du flux de données

```
    CSV Files                  Database                    CSV Files
    (Source)                 (Processing)                  (Output)
        │                          │                           │
        │                          │                           │
        ▼                          ▼                           ▼
  ┌─────────┐              ┌──────────────┐              ┌──────────┐
  │ data/   │   source.py  │   Database   │  results.py  │  data/   │
  │ source/ │─────────────▶│              │─────────────▶│processed/│
  │         │   (import)   │   Tables:    │   (export)   │  final/  │
  │ *.csv   │              │   - SOURCE   │              │  *.csv   │
  └─────────┘              │   - TRANS.   │              └──────────┘
                           │   - SUMMARY  │
                           └──────────────┘
                                   │
                                   │ transforming.py
                                   │ (transform & aggregate)
                                   ▼
                           ┌──────────────┐
                           │    Python    │
                           │   + Pandas   │
                           │   + duckdb   │
                           │   (Engine)   │
                           └──────────────┘
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
│   └── database.duckdb     Base de données principale du projet
│
├── scripts/                
│   ├── source.py           Script d'import des données depuis les fichiers sources
│   ├── transforming.py     Script pour transformer et agréger les données
│   └── results.py          Script pour générer et exporter les résultats finaux
│
├── macro/                  Scripts d'orchestration
│   ├── python-run-process.py              Exécution classique du pipeline
│   ├── dagster-run-process.py             Définition Dagster du pipeline
│   ├── dagster-run-process-launch.py      Lancement de Dagster (port selon DB)
│   ├── dagster-run-process-stop.py        Arrêt du processus principal
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
├── python-data-build-tool.py        Générateur de structure de projet
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

### dagster-run-process-stop.py / dagster-run-process-sample-stop.py
Arrête le processus Dagster spécifique:
- `dagster-run-process-stop.py`: Arrête le processus principal (port selon DB)
- `dagster-run-process-sample-stop.py`: Arrête le processus sample (port selon DB)
- Kill du processus sur le port concerné uniquement
- Nettoyage des fichiers PID
- Vérification de l'arrêt complet

**Ports par type de base de données:**
- DuckDB: 3000 (principal), 3001 (sample)
- SQLite: 4000 (principal), 4001 (sample)  
- Database Commune: 5000 (principal), 5001 (sample)

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

## Ports réseau par type de base de données

**DuckDB:**
- Principal: Port 3000 (http://localhost:3000)
- Sample: Port 3001 (http://localhost:3001)

**SQLite:**
- Principal: Port 4000 (http://localhost:4000)
- Sample: Port 4001 (http://localhost:4001)

**Database Commune:**
- Principal: Port 5000 (http://localhost:5000)
- Sample: Port 5001 (http://localhost:5001)

Les six instances peuvent fonctionner simultanément sans conflit.

## Logs

- `macro/execution.log` - Logs du pipeline (import, transformation, export)
- `macro/dagster.log` - Logs du serveur Dagster
- `macro/dagster.pid` - PID du processus Dagster actif

