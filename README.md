# Python Data Build Tool

## 📋 Description du projet

**Python Data Build Tool** est un générateur automatisé de pipelines ETL (Extract, Transform, Load) avec orchestration Dagster. Il crée une structure de projet complète pour le traitement de données CSV avec trois types de bases de données au choix.

## 🎯 Objectif

Simplifier la création de projelines de traitement de données en générant automatiquement :
- Une structure de projet organisée
- Des scripts ETL prêts à l'emploi
- Une orchestration Dagster configurée
- Une interface web de monitoring
- Des exemples de données pour démarrer rapidement

## 🔧 Fonctionnalités principales

### 1. **Choix de la base de données**
   - **DuckDB** (ports 3000/3001) : Base analytique haute performance
   - **SQLite** (ports 4000/4001) : Base relationnelle légère
   - **Database Commune** (ports 5000/5001) : Base générique flexible

### 2. **Structure générée automatiquement**
```
projet/
├── data/
│   ├── source/      # Fichiers CSV d'entrée
│   ├── processed/   # Données transformées
│   └── final/       # Résultats finaux
├── db/              # Base de données
├── scripts/
│   ├── source.py       # Import des données
│   ├── transforming.py # Transformations
│   └── results.py      # Export des résultats
└── macro/           # Scripts de gestion Dagster
```

### 3. **Processus ETL orchestré**
```
CSV Source → Import → Database → Transform → Export → CSV Final
                ↓                    ↓              ↓
          Table SOURCE        Table TRANSFORMED   SUMMARY_DATA.csv
```

### 4. **Gestion d'environnements**
- **dev/** : Environnement de développement
- **test/** : Environnement de test
- **prod/** : Environnement de production
- **deploy/** : Déploiement final
- **archive/** : Archives horodatées du projet

## 🚀 Utilisation

### Génération initiale du projet

```bash
# Créer la structure de base
python3 python-data-build-tool.py
```

### Génération d'un environnement

```bash
# Développement avec DuckDB
python3 update-dev.py
# Choix: 1

# Test avec SQLite
python3 update-test.py
# Choix: 2

# Production avec Database Commune
python3 update-prod.py
# Choix: 3

# Déploiement
python3 update-deploy.py
# Choix: 1, 2 ou 3
```

### Archivage du projet

```bash
# Créer une archive horodatée
python3 update-archive.py
# Archive créée dans: archive/archive - YYYY-MM-DD HH:MM:SS/
```

### Démarrage du pipeline

```bash
cd dev/python-data-build-tool/macro

# Lancer l'instance principale
python3 dagster-run-process-launch.py

# Lancer l'instance sample (exemples)
python3 dagster-run-process-sample-launch.py
```

### Arrêt sélectif

```bash
# Arrêter uniquement le processus principal
python3 dagster-run-process-stop.py

# Arrêter uniquement le processus sample
python3 dagster-run-process-sample-stop.py
```

## 🌐 Interfaces web

Chaque environnement dispose de deux interfaces Dagster :

| Base de données    | Principal        | Sample           |
|--------------------|------------------|------------------|
| DuckDB            | localhost:3000   | localhost:3001   |
| SQLite            | localhost:4000   | localhost:4001   |
| Database Commune  | localhost:5000   | localhost:5001   |

## 📦 Dépendances

- Python 3.9+
- pandas
- dagster
- dagster-webserver
- duckdb (si choix DuckDB)

Installation automatique proposée au lancement.

## 🔑 Points clés

1. **Sans conflit de ports** : Chaque type de base utilise des ports uniques
2. **Arrêt sélectif** : Stop scripts ciblés par port (pas de kill global)
3. **Prêt à l'emploi** : Données d'exemple incluses pour tester immédiatement
4. **Multi-environnements** : dev/test/prod isolés
5. **Orchestration visuelle** : Interface Dagster pour suivre les pipelines

## 📊 Cas d'usage

- Traitement batch de fichiers CSV
- Transformation de données analytiques
- Pipelines ETL automatisés
- Prototypage rapide de workflows data
- Formation à Dagster et aux pipelines de données

## 🛠️ Architecture technique

- **Générateur** : `python-data-build-tool.py` - Script principal de génération
- **Scripts de mise à jour** :
  - `update-dev.py` - Mise à jour environnement dev
  - `update-test.py` - Mise à jour environnement test
  - `update-prod.py` - Mise à jour environnement prod
  - `update-deploy.py` - Mise à jour déploiement
  - `update-archive.py` - Création d'archives horodatées
- **Templates dynamiques** : Génération avec f-strings et ports configurables
- **Gestion de processus** : `lsof` pour détection de ports, PID files pour tracking
- **Assets Dagster** : Chaque étape ETL est un asset orchestré
- **Archivage automatique** : Sauvegarde horodatée de tous les fichiers du projet
