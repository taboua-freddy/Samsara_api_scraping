# Collecte et Stockage des Données Samsara sur google cloud platform

## Objectif du Projet
Ce projet vise à collecter des données depuis l'API Samsara, les stocker sur Google Cloud Storage (GCS), puis les charger dans BigQuery pour une exploitation analytique. Le script **main.py** permet d'automatiser l'ensemble du processus en gérant la collecte, la transformation et l'importation des données.

## Fonctionnalités principales
- Extraction des données de Samsara via API.
- Stockage des données brutes dans GCS.
- Chargement des données traitées dans BigQuery.
- Gestion des logs et des fichiers manquants.
- Parallélisation des requêtes pour optimiser la performance.

## Installation
### Prérequis
- Python 3.10+
- Un compte Google Cloud avec un bucket GCS configuré.
- Un token d'accès API Samsara.
- Les bibliothèques Python nécessaires (listées dans `requirements.txt`).

### Installation des dépendances
1. Cloner le dépôt :
   ```bash
   git clone https://github.com/taboua-freddy/Samsara_api_scraping.git
   cd Samsara_api_scraping
   ```
2. Créer et activer un environnement virtuel :
   ```bash
   python -m venv venv
   source venv/bin/activate  # Sur macOS/Linux
   venv\Scripts\activate  # Sur Windows
   ```
3. Installer les dépendances :
   ```bash
   pip install -r requirements.txt
   ```

## Configuration
### Variables d'environnement
Créer un fichier `.env` à la racine du projet et y ajouter les informations suivantes :
```ini
SAMSARA_API_TOKEN=xxxxx
GCP_CREDENTIALS_FILE_NAME=service-account.json
GCS_RAW_BUCKET_NAME=datasamsara
GCS_FLATTENED_BUCKET_NAME=data_samsara_flattened
DATABASE_ID=st_samsara
```

### Credentials Google Cloud
Placer le fichier d'authentification GCP dans le dossier `credentials/` et vérifier que son nom correspond à `GCP_CREDENTIALS_FILE_NAME` défini dans `.env`.
Cette variable est facultative si Application Default Credentials ou Workload Identity est configuré. Ne copiez jamais `.env` ou les credentials dans une image Docker : injectez-les au démarrage.

## Exécution du script
Le script **main.py** peut être exécuté avec les paramètres suivants :
```bash
python main.py --start_date "01/01/2024" --end_date "10/01/2024" --table_file_path tables.xlsx --max_workers 5
```

Prévisualiser une intégration ciblée sans connexion à Samsara ou Google Cloud :
```bash
python main.py --start_date "01/01/2025" --end_date "02/01/2025" \
  --table fleet_vehicle_stats_engineRpm \
  --stages download transform load \
  --dry-run
```

Après validation du plan, retirer `--dry-run`. L'option `--table` peut être répétée et `--stages` permet aussi d'exécuter uniquement `download`, `transform` ou `load` sur les fichiers déjà présents.

### Explication des paramètres
- `--start_date` : Date de début (format `jj/mm/aaaa`).
- `--end_date` : Date de fin (format `jj/mm/aaaa`).
- `--max_workers` : Nombre de requêtes à traiter en parallèle.
- `--table_file_path` : Chemin du fichier contenant les noms des tables à traiter. Utiliser `ALL` pour toutes les tables. Vous pouvez avoir la liste des tables prises en charge dans le fichier `modules/metadata.py`.
- `--table` : Nom exact d'une table à traiter ; répétable.
- `--stages` : Sous-ensemble ordonné de `download`, `transform` et `load`.
- `--dry-run` : Validation locale du plan sans appel externe ni écriture cloud.

## Architecture du Code
- `main.py` : Script principal pour l'orchestration de la collecte et du chargement des données.
- `modules/samsara.py` : Gestion des requêtes API Samsara.
- `modules/gcp.py` : Interaction avec Google Cloud Storage et BigQuery.
- `modules/processing.py` : Transformation des données.
- `modules/logs.py` : Gestion des logs.
- `modules/metadata.py` : Extraction et gestion des métadonnées.
- `modules/utils.py` : Fonctions utilitaires pour le traitement et le stockage des données.

## Logs et Surveillance
Les logs sont stockés dans le dossier `resources/logs/` et sont automatiquement envoyés vers GCS après exécution du script.

## Tests et contrôles de qualité
Installer les dépendances de développement puis exécuter les tests sans appeler les services externes :
```bash
pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
ruff check main.py modules scripts tests
```

Les erreurs d'extraction, d'upload, de transformation ou de chargement BigQuery interrompent désormais l'exécution. Les checkpoints ne sont mis à jour qu'après la réussite complète de l'étape correspondante.

Les chargements BigQuery utilisent également un manifeste stocké dans le bucket aplati (`resources/configs/bigquery_load_manifest.json`) et des identifiants de jobs déterministes. Une relance peut ainsi reprendre un job existant sans ajouter une seconde fois les mêmes fichiers temporels. Les mises à jour des fichiers de configuration utilisent les générations GCS afin de détecter les écritures concurrentes.

### Nettoyage du manifeste BigQuery

Le nettoyage fonctionne en simulation par défaut. Il supprime uniquement les entrées orphelines, les anciennes générations `oneshot` et les fichiers temporels anciens déjà couverts par un checkpoint BigQuery :
```bash
python -m scripts.cleanup_manifest --retention-days 30
python -m scripts.cleanup_manifest --retention-days 30 --apply
```

Par sécurité, les ressources doivent contenir `test` dans leur nom. Une exécution sur une ressource de production nécessite en plus l'option explicite `--allow-production`.

Les logs locaux et GCS sont également supprimés automatiquement en fin d'exécution lorsqu'ils dépassent `LOG_RETENTION_DAYS` (30 jours par défaut). La console affiche la progression globale des étapes ainsi que le nombre de pages, chunks et objets pendant l'extraction. Un chunk est écrit dès que `SAMSARA_CHUNK_ROWS` objets (50 000 par défaut) ou `SAMSARA_CHUNK_PAGES` pages (25 par défaut) sont atteints.

## Remarque
- Assurez-vous d'avoir les permissions nécessaires sur GCS et BigQuery.
- Vérifiez que votre token API Samsara est valide avant l'exécution.

## Contact
Pour toute question ou suggestion, veuillez contacter l'équipe technique.

