param(
    [Parameter(Mandatory = $true)][string]$ProjectId,
    [Parameter(Mandatory = $true)][string]$Region,
    [Parameter(Mandatory = $true)][string]$Repository,
    [Parameter(Mandatory = $true)][string]$ServiceAccount,
    [Parameter(Mandatory = $true)][string]$RawBucket,
    [Parameter(Mandatory = $true)][string]$FlattenedBucket,
    [Parameter(Mandatory = $true)][string]$DatasetId,
    [string]$JobName = "samsara-pipeline-test",
    [string]$SecretName = "samsara-api-token-test",
    [int]$LookbackDays = 1,
    [switch]$Apply,
    [switch]$AllowProduction
)

$ErrorActionPreference = "Stop"

if (-not $AllowProduction) {
    $scopedNames = @($JobName, $RawBucket, $FlattenedBucket, $DatasetId, $SecretName)
    foreach ($name in $scopedNames) {
        if ($name -notmatch "test") {
            throw "Ressource non-test refusée: $name. Utilisez -AllowProduction explicitement."
        }
    }
}

if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    throw "Google Cloud CLI (gcloud) est requis et doit être disponible dans PATH."
}

$image = "$Region-docker.pkg.dev/$ProjectId/$Repository/samsara-api-scraping:latest"
$envVars = "GCS_RAW_BUCKET_NAME=$RawBucket,GCS_FLATTENED_BUCKET_NAME=$FlattenedBucket,DATABASE_ID=$DatasetId,SAMSARA_CHUNK_ROWS=50000,SAMSARA_CHUNK_PAGES=25,BIGQUERY_MANIFEST_RETENTION_DAYS=30,LOG_RETENTION_DAYS=30,PIPELINE_LOCK_TTL_MINUTES=1440"
$jobArgs = "--lookback-days=$LookbackDays,--table_file_path=ALL"

$buildArgs = @("builds", "submit", "--project", $ProjectId, "--tag", $image, ".")
$deployArgs = @(
    "run", "jobs", "deploy", $JobName,
    "--project", $ProjectId,
    "--region", $Region,
    "--image", $image,
    "--service-account", $ServiceAccount,
    "--tasks", "1",
    "--max-retries", "1",
    "--task-timeout", "24h",
    "--memory", "4Gi",
    "--cpu", "2",
    "--set-env-vars", $envVars,
    "--set-secrets", "SAMSARA_API_TOKEN=$SecretName`:latest",
    "--args", $jobArgs
)

Write-Host "gcloud $($buildArgs -join ' ')"
Write-Host "gcloud $($deployArgs -join ' ')"
if (-not $Apply) {
    Write-Host "Simulation uniquement. Relancez avec -Apply après vérification."
    exit 0
}

& gcloud @buildArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& gcloud @deployArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
