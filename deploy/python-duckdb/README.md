

- Ce projet permet de gérer un flux complet de données depuis l'import de fichiers sources.
- Il inclut la transformation et l'agrégation des données pour obtenir des résultats prêts à l'analyse.
- Les résultats finaux peuvent être exportés pour usage externe.

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

Moteur de traitement: Python + Pandas + DuckDB
```


## Structure du dossier

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
├── macro/                  
│   ├── process.py          Script principal qui lance import, transformation et export
│   └── execution.log       Fichier journal pour enregistrer les logs d'exécution
│
├── sample/                 Dossier contenant un exemple complet de données et scripts
│
├── requirements.txt        Fichier listant les dépendances du projet
├── python-duckdb.py        Script pour créer toute la structure du projet
└── README.md               Description des dossiers et fichiers du projet
```

