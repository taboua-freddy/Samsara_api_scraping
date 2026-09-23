# Référence de configuration

Ce document couvre les réglages du pipeline, du catalogue et du déploiement. Les exemples désignent des ressources de **test** ; ne mettez jamais un jeton ou un fichier de credentials dans Git.

## Où régler quoi ?

| Portée | Emplacement | Effet |
| --- | --- | --- |
| Exécution locale | Variables du processus ou `.env` à la racine | Connexions et réglages globaux de l'exécution. `load_dotenv()` ne remplace pas une variable déjà définie dans le processus. `.env.test` n'est **pas** chargé automatiquement. |
| Table / endpoint | [`config/metadata_catalog.json`](../config/metadata_catalog.json) | Requête Samsara, type de chargement, cadence et réglages propres à une table. |
| Lancement | Options de `main.py` | Période, tables et étapes de cette exécution. |
| Job Cloud Run | [`cloudbuild.yaml`](../cloudbuild.yaml) ou [`deploy/deploy-cloud-run-job.ps1`](../deploy/deploy-cloud-run-job.ps1) | Variables injectées dans le conteneur, arguments et ressources du Job. Modifier `.env` local ne modifie pas le Job déployé. |
| État du pipeline | `resources/configs/*.json` dans GCS | Checkpoints et manifestes mis à jour automatiquement. Ce ne sont pas des paramètres à éditer manuellement. |

Pour les tailles de chunks et le découpage temporel, l'ordre de priorité est **variable d'environnement > champ de la table dans le catalogue > valeur par défaut du code**. Une variable globale Cloud Run écrase donc la valeur de *toutes* les tables concernées. Les changements de fenêtre ou de taille de chunk ne changent pas l'identité d'une partition déjà extraite ; les changements d'endpoint ou de filtres de requête peuvent en revanche changer les données demandées.

## Variables d'environnement du pipeline

Les variables suivantes sont lues par `main.py`, `modules/processing.py` ou `modules/utils.py`. Les valeurs indiquées comme obligatoires n'ont pas de valeur par défaut.

| Variable | Défaut / contrainte | Rôle et emplacement conseillé |
| --- | --- | --- |
| `SAMSARA_API_TOKEN` | Obligatoire | Jeton Samsara. Dans Cloud Run, l'injecter depuis Secret Manager ; ne pas le committer dans `.env`. |
| `GCS_RAW_BUCKET_NAME` | Obligatoire | Bucket des Parquet bruts, logs, verrou, checkpoints et manifestes d'extraction. |
| `GCS_FLATTENED_BUCKET_NAME` | Obligatoire | Bucket des Parquet transformés et du manifeste de chargement BigQuery. |
| `DATABASE_ID` | Obligatoire | Identifiant du **dataset BigQuery** cible ; ce n'est pas une URL ni un mot de passe de base de données. |
| `GCP_CREDENTIALS_FILE_NAME` | Facultatif | Nom d'un fichier dans `credentials/`. Dans `main.py`, s'il est défini, il construit et **écrase** `GOOGLE_APPLICATION_CREDENTIALS`. À laisser vide avec les identités Cloud Run / Application Default Credentials. |
| `GOOGLE_APPLICATION_CREDENTIALS` | Facultatif | Chemin complet d'un fichier de credentials Google. Si absent, la bibliothèque Google utilise les identifiants applicatifs disponibles. Ne pas le définir en même temps que `GCP_CREDENTIALS_FILE_NAME` pour `main.py`. |
| `SAMSARA_CHUNK_ROWS` | `50000`, entier ≥ 1 | Écrit un chunk dès que le tampon atteint ce nombre de lignes ; remplace `chunk_rows` du catalogue pour toutes les tables. |
| `SAMSARA_CHUNK_PAGES` | `25`, entier ≥ 1 | Écrit aussi un chunk dès que ce nombre de pages API est atteint ; remplace `chunk_pages`. Le premier des deux seuils atteint déclenche l'écriture. Une page entière peut faire dépasser `SAMSARA_CHUNK_ROWS` ; ce n'est pas une limite en Mo. |
| `SAMSARA_WINDOW_MINUTES` | Non défini ; entier ou décimal ≥ 1 si défini | Fenêtre temporelle initiale pour cette exécution ; remplace `window_minutes`, puis `delta_days`. Pour les API à dates seules, une fenêtre inférieure à un jour reste un jour. |
| `SAMSARA_SPLIT_MIN_MINUTES` | `45`, nombre ≥ 1 | Taille minimale de **chaque moitié** créée après une erreur serveur ; remplace `split_min_minutes`. |
| `SAMSARA_SPLIT_MAX_DEPTH` | `3`, entier ≥ 0 | Nombre maximal de subdivisions récursives ; `0` désactive la subdivision, même si `split_on_server_error` est vrai. Remplace `split_max_depth`. |
| `PARALLEL_PROGRESS_INTERVAL_SECONDS` | `60`, nombre de secondes | Intervalle entre deux messages « toujours en cours » quand aucune tâche parallèle ne finit. Une valeur ≤ 0 revient à 60 s. N'agit pas sur la fréquence des appels API. |
| `BIGQUERY_MANIFEST_RETENTION_DAYS` | `30`, entier ≥ 0 | Nettoyage du manifeste **BigQuery** après la réussite des étapes : retire seulement les entrées devenues sûres à oublier (voir README). Ne supprime ni les fichiers de données ni les manifestes d'extraction par table. `0` ne signifie pas « tout supprimer ». |
| `LOG_RETENTION_DAYS` | `30`, entier ≥ 1 | Supprime après la réussite des étapes les anciens logs locaux et les logs du bucket brut. |
| `PIPELINE_LOCK_TTL_MINUTES` | `1440`, entier ≥ 1 | Durée de vie du verrou GCS qui empêche deux exécutions simultanées. Le verrou est libéré à la fin normale ; un TTL trop court peut laisser démarrer un second Job pendant le premier. |

`PYTHONUNBUFFERED=1` est défini dans le Dockerfile pour afficher les sorties sans tampon ; l'application ne le lit pas directement. Une variable numérique non convertible provoque une erreur au lancement ou à l'utilisation du réglage.

Les quatre premières variables obligatoires ne sont pas exigées pour `--dry-run`, qui s'arrête après la validation locale du plan.

Exemple local sans secret :

```dotenv
GCS_RAW_BUCKET_NAME=raw-example-test
GCS_FLATTENED_BUCKET_NAME=flattened-example-test
DATABASE_ID=dwh_example_test
SAMSARA_CHUNK_ROWS=10000
SAMSARA_CHUNK_PAGES=10
PARALLEL_PROGRESS_INTERVAL_SECONDS=30
```

## Champs de `metadata_catalog.json`

Le fichier est une liste d'objets, un par `table_name` (nom unique). Les champs structurants suivants sont présents dans le catalogue ; `window_minutes`, `split_min_minutes`, `split_max_depth`, `chunk_rows` et `chunk_pages` sont des ajouts facultatifs par table.

| Champ | Valeur / effet |
| --- | --- |
| `family` | Préfixe du dossier GCS, par exemple `assets` ou `vehicle_stats`. Obligatoire. Changer cette valeur déplace le chemin logique des fichiers. |
| `table_name` | Identifiant unique utilisé pour sélectionner la table, nommer les fichiers, le manifeste d'extraction et la table BigQuery (ou ses sous-tables). Obligatoire ; changer le nom crée une nouvelle identité de pipeline. |
| `endpoint` | Chemin de l'API Samsara, relatif à `https://api.eu.samsara.com/`. Obligatoire. Peut contenir un emplacement `{vehicleId}` pour une URL dynamique. |
| `params` | Paramètres de requête, généralement une chaîne `clé=valeur,clé=valeur`. Les paramètres de dates et les filtres tels que `Types` déterminent les données demandées. `null`/vide convient aux endpoints sans paramètres. |
| `rate_limit_per_seconde` | Limite d'appels par seconde pour l'endpoint, strictement positive. Le catalogue doit contenir le champ ; `null` utilise `5`. Une limite globale de 150 appels/s existe aussi dans le code. |
| `download_type` | `time` : fichiers temporels et chargement BigQuery en ajout ; `oneshot` : instantané et chargement en remplacement. Obligatoire. Ne pas changer sans étudier les checkpoints et les fichiers existants. |
| `is_processed` | `0`/`false` : extraction normale ; `1`/`true` : la ligne est sautée par la collecte principale. La recherche ultérieure de journées manquantes suit sa propre logique ; ce n'est donc ni un verrou complet ni un indicateur d'avancement des chunks. |
| `description` | Texte documentaire ; pas d'effet sur l'exécution. |
| `is_exception` | `0`/`false` : chemin normal ; `1`/`true` : traitement spécial défini par `exception_config`. |
| `exception_config` | Objet de routage des cas spéciaux ; voir ci-dessous. `{}` si inutile. |
| `delta_days` | Fenêtre initiale en jours (`1` par défaut si `null`, `0.25` = 6 h). Peut être remplacée par `window_minutes`, puis par `SAMSARA_WINDOW_MINUTES`. |
| `window_minutes` | Facultatif : fenêtre initiale propre à cette table, en minutes (≥ 1). |
| `split_on_server_error` | Facultatif : si vrai, découpe une requête `startMs`/`endMs` échouée en HTTP 500, 502, 503 ou 504, à condition qu'aucun chunk/curseur confirmé ne rende ce découpage dangereux. `split_on_gateway_timeout` reste un ancien alias utilisé seulement si ce champ n'est pas défini. |
| `split_min_minutes` / `split_max_depth` | Facultatifs : minima et profondeur du split, respectivement `45` et `3` par défaut. Surchargés par les variables `SAMSARA_SPLIT_*`. |
| `chunk_rows` / `chunk_pages` | Facultatifs : seuils de chunk propres à la table, par défaut `50000` et `25`. Surchargés par `SAMSARA_CHUNK_ROWS` / `SAMSARA_CHUNK_PAGES`. |
| `time_partitioning_field` | Nom d'une colonne de date/heure pour partitionner la table BigQuery ; `null` désactive ce réglage. La colonne doit exister dans le schéma transformé. |
| `clustering_fields` | Colonne ou liste de colonnes pour le clustering BigQuery ; `null` si aucun. Les colonnes doivent exister dans le schéma transformé. |

### Variables de date dans `params`

Elles sont résolues au chargement du catalogue à partir de `--start_date` et `--end_date` :

| Marqueur | Format produit |
| --- | --- |
| `${START_RAW}` / `${END_RAW}` | Date textuelle `jj/mm/aaaa`. |
| `${START_DATE}` / `${END_DATE}` | Date ISO `aaaa-mm-jj`. |
| `${START_MS}` / `${END_MS}` | Timestamp Unix en millisecondes. Le code actuel convertit les dates selon le fuseau local du processus : conserver un fuseau cohérent entre le poste et Cloud Run pour éviter de décaler les fenêtres. |
| `${START_ISO}` | Début au format ISO avec suffixe UTC `+00:00`. Aucun `${END_ISO}` n'est implémenté. |

Pour les endpoints `startMs`/`endMs`, le planificateur traite la borne de fin demandée comme **exclusive** et envoie `endMs = fin - 1 ms` à Samsara. Les bornes exactes, le curseur et les fichiers confirmés sont conservés par partition ; changer les réglages de split ou de chunk ne retélécharge pas les plages complètes.

### `exception_config`

| Clé | Usage |
| --- | --- |
| `exception_type` | `table` pour une dépendance à une autre table ; `date` pour les cas particuliers de dates. |
| `constraint` | Pour `table` : `dynamic_url`. Pour `date` : `is_data_but_datetime` ou `only_start_date`. |
| `table_name` | Table source des identifiants nécessaires à l'appel dynamique. |
| `table_column_name` / `table_column_aliases` | Colonne d'identifiants attendue dans les Parquet de la dépendance et noms alternatifs acceptés. |
| `exception_param_name` | Nom de l'emplacement à remplacer, par exemple `vehicleId`. |
| `key_to_apply_on` | `endpoint` ou `params` : champ où remplacer l'emplacement. |
| `is_list` | Vrai : groupes d'identifiants dans un paramètre (50 par groupe dans le code) ; faux : une URL par identifiant. |

Les champs `exception_config` ne sont utiles que si `is_exception` est vrai. Les noms de colonnes doivent correspondre aux données **brutes** ou à un alias configuré.

Par exemple, pour cette seule table, ajouter `"window_minutes": 180` dans l'objet `fleet_assets_reefers` crée des fenêtres initiales de trois heures. Si `SAMSARA_WINDOW_MINUTES=60` est également défini dans Cloud Run, la valeur effective est une heure **pour toutes les tables**, y compris `fleet_assets_reefers`. Le même ordre de priorité s'applique à `chunk_rows`/`SAMSARA_CHUNK_ROWS` et à `chunk_pages`/`SAMSARA_CHUNK_PAGES`.

## Options de `main.py`

| Option | Rôle / défaut |
| --- | --- |
| `--start_date jj/mm/aaaa` | Début de période ; par défaut `13/07/2020`. |
| `--end_date jj/mm/aaaa` | Fin de période ; par défaut la date du jour. Pour `startMs`/`endMs`, borne exclusive. |
| `--lookback-days N` | Fenêtre de `N` jours se terminant aujourd'hui (`N ≥ 1`) ; incompatible avec `--start_date` et `--end_date`. |
| `--table NOM` | Table précise ; répétable. Prioritaire sur les autres sélections de tables. |
| `--table_file_path ALL` ou chemin Excel | `ALL` prend les 51 tables du catalogue ; sinon la première colonne du fichier Excel est lue. Ignoré si `--table` est fourni. |
| `--table_cat CATEGORIE` | Groupe `ev`, `time`, `stats` ou `core`, si aucune sélection plus prioritaire. Sans catégorie reconnue, la liste par défaut est utilisée. Sert aussi de version dans le chemin des logs GCS. |
| `--max_workers N` | Concurrence des appels de téléchargement (`N ≥ 1`). Sans valeur : comportement automatique du pool Python. N'augmente pas la limite API autorisée. |
| `--stages ...` | Choisir parmi `download transform load` ; les trois étapes sont exécutées par défaut. Permet par exemple `--stages transform load` sans retélécharger. |
| `--dry-run` | Affiche le plan validé sans appeler Samsara, GCS ou BigQuery. |

Exemple de contrôle avant exécution :

```powershell
python .\main.py --start_date 18/09/2026 --end_date 19/09/2026 --table fleet_assets_reefers --stages download transform load --dry-run
```

## Déploiement et maintenance

Dans `cloudbuild.yaml`, les substitutions suivantes sont modifiables :

| Substitution | Valeur actuelle | Usage |
| --- | --- | --- |
| `_REGION` | `europe-west1` | Région Artifact Registry et Cloud Run Job. |
| `_REPOSITORY` | `pipelines-test` | Dépôt Artifact Registry. |
| `_JOB_NAME` | `samsara-pipeline-test` | Nom du Job Cloud Run. |
| `_RUNTIME_SERVICE_ACCOUNT` | `samsara-pipeline-test@maintenance-predictive-445011.iam.gserviceaccount.com` | Identité d'exécution du Job, distincte de l'identité Cloud Build qui déploie. |
| `_RAW_BUCKET` | `raw-samsara-data-test-maintenance-predictive-445011` | Bucket brut injecté via `GCS_RAW_BUCKET_NAME`. |
| `_FLATTENED_BUCKET` | `samsara-data-flattened-test-maintenance-predictive-445011` | Bucket transformé injecté via `GCS_FLATTENED_BUCKET_NAME`. |
| `_DATASET_ID` | `dwh_samsara_test` | Dataset BigQuery injecté via `DATABASE_ID`. |
| `_SAMSARA_SECRET` | `samsara-api-token-test` | **Nom** du secret injecté dans `SAMSARA_API_TOKEN` ; jamais la valeur du jeton. |

`${PROJECT_ID}` et `${COMMIT_SHA}` sont fournis par Cloud Build ; le secret lui-même n'est pas une substitution en clair.

Le même fichier fixe actuellement `--tasks=1`, `--max-retries=1`, `--task-timeout=24h`, `--cpu=8`, `--memory=16Gi`, les variables injectées par `--set-env-vars`, le jeton par `--set-secrets` et les arguments par `--args=--lookback-days=1,--table_file_path=ALL`. Son `timeout: 1800s` limite **la construction et le déploiement Cloud Build**, pas l'exécution de 24 h du Job. Modifier ces valeurs dans le dépôt ne change le Job qu'après un nouveau déploiement. Le script PowerShell de déploiement possède ses propres valeurs (`2` CPU, `4Gi`) : ce sont deux chemins de déploiement distincts.

`cloudbuild.yaml` et le script PowerShell injectent actuellement `SAMSARA_CHUNK_ROWS=50000` et `SAMSARA_CHUNK_PAGES=25` au niveau du Job. Pour que les valeurs `chunk_rows`/`chunk_pages` propres aux tables prennent effet dans Cloud Run, retirer ces deux variables globales de `--set-env-vars` ou les ajuster volontairement : elles ont toujours priorité.

Le script PowerShell accepte `-ProjectId`, `-Region`, `-Repository`, `-ServiceAccount`, `-RawBucket`, `-FlattenedBucket`, `-DatasetId` (obligatoires), puis `-JobName`, `-SecretName`, `-LookbackDays` (défauts `samsara-pipeline-test`, `samsara-api-token-test`, `1`). `-Apply` réalise le déploiement au lieu d'afficher les commandes ; `-AllowProduction` retire le garde-fou exigeant `test` dans les noms des ressources. À utiliser avec prudence.

Pour le nettoyage manuel, `python -m scripts.cleanup_manifest --retention-days N` simule par défaut ; `--apply` écrit réellement, `--allow-production` lève le garde-fou test. La rétention de ce script est indépendante de `BIGQUERY_MANIFEST_RETENTION_DAYS` (qui concerne le nettoyage automatique en fin de programme).

`scripts.provision_test_resources` et `scripts.verify_test_run` utilisent les mêmes variables de buckets, dataset et credentials. Ils exigent `test` dans les trois noms. Le premier peut créer des buckets et un dataset en localisation `EU` ; le second est un contrôle en lecture seule actuellement ciblé sur `fleet_vehicle_stats_faultCodes` (ce nom est codé dans le script, pas paramétré).

Les valeurs d'état `last_download_time`, `last_transformation_time`, `last_db_migration_time` et `date_no_data` dans `resources/configs/configs_for_update.json`, ainsi que les manifestes d'extraction et de chargement, sont gérées automatiquement. Les modifier manuellement peut provoquer des doublons ou faire sauter des plages de données.

Enfin, certains paramètres restent **codés en dur** : API `api.eu.samsara.com`, délai HTTP `(10 s connexion, 120 s lecture)`, trois tentatives par requête, limite globale de 150 appels/s et groupes de 50 identifiants pour les endpoints dynamiques. Il faut modifier le code et le tester pour les changer ; aucune variable d'environnement actuelle ne les pilote.
