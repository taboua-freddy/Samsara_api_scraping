# Déploiement Cloud Run Job

Le pipeline est déployé comme un job batch avec une seule tâche. Le verrou atomique stocké dans le bucket brut empêche deux exécutions simultanées.

Prérequis : Google Cloud CLI, Artifact Registry, Cloud Build, Cloud Run, Secret Manager et un compte de service disposant uniquement des droits nécessaires sur les deux buckets et le dataset BigQuery de test.

Le jeton Samsara doit être stocké dans Secret Manager. Il ne doit jamais être placé dans le script, l'image ou Git.

Commencer par une simulation :

```powershell
.\deploy\deploy-cloud-run-job.ps1 `
  -ProjectId maintenance-predictive-445011 `
  -Region europe-west1 `
  -Repository pipelines-test `
  -ServiceAccount samsara-pipeline-test@maintenance-predictive-445011.iam.gserviceaccount.com `
  -RawBucket raw-samsara-data-test-maintenance-predictive-445011 `
  -FlattenedBucket samsara-data-flattened-test-maintenance-predictive-445011 `
  -DatasetId dwh_samsara_test
```

Après revue des commandes affichées, ajouter `-Apply`. Le script refuse par défaut tout nom ne contenant pas `test`. Aucune planification n'est créée automatiquement : exécuter d'abord le job manuellement et valider les données de test.

## Déploiement continu depuis GitHub

Le fichier `cloudbuild.yaml` permet de configurer le déploiement continu du Job depuis l'interface Google Cloud :

1. Ouvrir **Cloud Build > Déclencheurs** et connecter le dépôt GitHub avec la GitHub App Cloud Build.
2. Créer un déclencheur **Push vers une branche** limité à la branche de déploiement (par exemple `^main$`).
3. Choisir **Fichier de configuration Cloud Build** et saisir `/cloudbuild.yaml`.
4. Vérifier ou remplacer les substitutions `_REGION`, `_REPOSITORY`, `_JOB_NAME`, `_RUNTIME_SERVICE_ACCOUNT`, `_RAW_BUCKET`, `_FLATTENED_BUCKET`, `_DATASET_ID` et `_SAMSARA_SECRET`.
5. Utiliser un compte de service Cloud Build dédié, autorisé à écrire dans Artifact Registry, déployer le Job et agir comme le compte de service d'exécution.

Chaque push construit une image identifiée par le SHA Git et met à jour le Job de test. Le Job n'est pas exécuté automatiquement par le déploiement.

## Catalogue des endpoints

Les définitions déclaratives ont été déplacées dans `config/metadata_catalog.json`. Les variables `${START_RAW}`, `${END_RAW}`, `${START_DATE}`, `${END_DATE}`, `${START_MS}`, `${END_MS}` et `${START_ISO}` sont résolues par `modules/metadata_catalog.py`. `modules/metadata.py` conserve uniquement la validation, les filtres et la construction du DataFrame.
