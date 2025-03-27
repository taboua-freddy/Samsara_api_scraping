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
   git clone <url_du_depot>
   cd <nom_du_dossier>
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
GCP_CREDENTIALS_FILE_NAME=maintenance-predictive-445011-fb98a59d6aa3.json
GCS_BUCKET_NAME=datasamsara
GCS_BUCKET_FLATTENED_NAME=data_samsara_flattened
DWH_ID=dwh_samsara  
DATABASE_ID=st_samsara
```

### Credentials Google Cloud
Placer le fichier d'authentification GCP dans le dossier `credentials/` et vérifier que son nom correspond à `GCP_CREDENTIALS_FILE_NAME` défini dans `.env`.

## Exécution du script
Le script **main.py** peut être exécuté avec les paramètres suivants :
```bash
python main.py --start_date "01/01/2024" --end_date "10/01/2024" --table_file_path tables.xlsx --max_workers 5
```

### Explication des paramètres
- `--start_date` : Date de début (format `jj/mm/aaaa`).
- `--end_date` : Date de fin (format `jj/mm/aaaa`).
- `--max_workers` : Nombre de requêtes à traiter en parallèle.
- `--table_file_path` : Chemin du fichier contenant les noms des tables à traiter. Utiliser `ALL` pour toutes les tables. Vous pouvez avoir la liste des tables prises en charge dans le fichier `modules/metadata.py`.

## Architecture du Code
- `main.py` : Script principal pour l'orchestration de la collecte et du chargement des données.
- `modules/samsara.py` : Gestion des requêtes API Samsara.
- `modules/gcp.py` : Interaction avec Google Cloud Storage et BigQuery.
- `modules/processing.py` : Transformation des données.
- `modules/logs.py` : Gestion des logs.
- `modules/metadata.py` : Extraction et gestion des métadonnées.
- `modules/utils.py` : Fonctions utilitaires pour le traitement et le stockage des données.

## Logs et Surveillance
Les logs sont stockés dans le dossier `logs/` et sont automatiquement envoyés vers GCS après exécution du script.

## Remarque
- Assurez-vous d'avoir les permissions nécessaires sur GCS et BigQuery.
- Vérifiez que votre token API Samsara est valide avant l'exécution.

## Contact
Pour toute question ou suggestion, veuillez contacter l'équipe technique.

