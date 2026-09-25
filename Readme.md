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
La référence complète des variables d'environnement, des champs du catalogue, des options de lancement et du déploiement est dans [docs/configuration.md](docs/configuration.md). `.env.example` fournit les valeurs de départ sans secret.

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

Pour retraiter une période historique sans lire ni modifier les curseurs
incrémentaux, utiliser `--historical`. La date de fin est exclusive :

```bash
python main.py --historical --start_date "18/09/2026" --end_date "20/09/2026" \
  --table fleet_assets_reefers
```

Le mode historique exige les deux dates. Les manifestes d'extraction et de
chargement restent actifs afin de reprendre une exécution interrompue et
d'éviter les doublons. Les fichiers transformés déjà présents ne sont pas
réécrits dans ce mode, ce qui conserve leur génération GCS et empêche un
nouveau chargement `WRITE_APPEND` des mêmes données.

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
- `--default-tables` : Traite explicitement l'union des catégories actuellement renvoyées par `get_table_name_by_category()` ; cette sélection est plus restreinte que `--table_file_path ALL` et ne se combine pas avec les autres sélections de tables.
- `--table` : Nom exact d'une table à traiter ; répétable.
- `--stages` : Sous-ensemble ordonné de `download`, `transform` et `load`.
- `--dry-run` : Validation locale du plan sans appel externe ni écriture cloud.

Une table `oneshot` est un instantané rafraîchi à chaque nouvelle étape `download` après une extraction complète. Un téléchargement interrompu peut reprendre son instantané en cours. Les nouveaux chunks portent un identifiant de version ; `transform` et `load` ne prennent que les fichiers du dernier instantané terminé. Avant le chargement, les schémas Parquet des chunks sont harmonisés par union des colonnes (les valeurs absentes deviennent nulles), puis les chunks d'une même table sont chargés ensemble en remplacement dans BigQuery. Les anciens objets GCS ne sont pas supprimés automatiquement.

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

Les erreurs d'extraction, d'upload, de transformation ou de chargement BigQuery interrompent l'exécution. Lors d'un chargement BigQuery partiellement réussi, les fichiers chargés sont enregistrés dans le manifeste avant de signaler les échecs ; une reprise ne recharge donc que les fichiers manquants. Les soumissions vers une même table sont espacées pour éviter le quota de mises à jour, et un job échoué sur une erreur de quota temporaire est relancé sous un nouvel identifiant. Les checkpoints des autres étapes ne sont mis à jour qu'après leur réussite complète.

Les chargements BigQuery utilisent également un manifeste stocké dans le bucket aplati (`resources/configs/bigquery_load_manifest.json`) et des identifiants de jobs déterministes. Une relance peut ainsi reprendre un job existant sans ajouter une seconde fois les mêmes fichiers temporels. Les mises à jour des fichiers de configuration utilisent les générations GCS afin de détecter les écritures concurrentes.

### Reprise de l'extraction et découpage temporel

Chaque table possède désormais son propre manifeste d'extraction dans le bucket brut : `resources/configs/extraction_manifests/<table>.json`. Pour les endpoints `startMs`/`endMs`, il enregistre les bornes UTC exactes (fin exclusive), les fichiers confirmés, le curseur et les éventuelles sous-fenêtres créées après une erreur serveur. Une relance ne planifie que les plages non couvertes ; une fenêtre partiellement téléchargée reprend avec ses bornes et son curseur d'origine. Une fenêtre terminée sans donnée compte aussi comme couverte. La présence d'un seul fichier de quelques heures ne valide plus la journée entière.

La fenêtre initiale provient de `delta_days` dans le catalogue et peut être remplacée pour une exécution avec `SAMSARA_WINDOW_MINUTES`. Les limites du split sont `SAMSARA_SPLIT_MIN_MINUTES` (45 par défaut) et `SAMSARA_SPLIT_MAX_DEPTH` (3 par défaut) ; les valeurs propres à une table peuvent aussi être définies par `window_minutes`, `split_min_minutes` et `split_max_depth` dans le catalogue. Modifier ces paramètres ne remet pas en cause les plages déjà terminées. Les valeurs effectives sont conservées dans chaque état du manifeste pour audit. Le manifeste global précédent reste intact ; seuls ses anciens intervalles UTC vérifiables sont importés lors de la première exécution de la table.

Les seuils de chunk (`SAMSARA_CHUNK_ROWS` et `SAMSARA_CHUNK_PAGES`, ou `chunk_rows` et `chunk_pages` par table dans le catalogue) peuvent aussi changer à la relance. Le manifeste conserve l'historique des seuils à partir de l'index du prochain chunk : les fichiers déjà confirmés ne sont pas réécrits et la reprise continue avec le curseur et l'index enregistrés. Ces seuils ne font pas partie de l'identité temporelle de la partition. Ce sont des limites en nombre de lignes ou de pages, pas une taille exacte en octets du Parquet.

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

