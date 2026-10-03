[CmdletBinding()]
param(
    [switch]$Force,
    [ValidateSet('normal', 'backfill')]
    [string]$Mode = 'normal',
    [ValidateSet('', 'dev', 'test', 'prod')]
    [string]$Target = '',
    [string]$ManifestPath = ''
)

$ErrorActionPreference = 'Stop'

$manifest = '{}'
if ($ManifestPath) {
    $resolved = (Resolve-Path -LiteralPath $ManifestPath).Path
    $manifest = Get-Content -LiteralPath $resolved -Raw
    $null = $manifest | ConvertFrom-Json
}
elseif ($Mode -eq 'backfill') {
    throw 'Silver backfill requires -ManifestPath with 12 preserved Bronze versions.'
}

$manifestBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($manifest))
$forceText = $Force.IsPresent.ToString().ToLowerInvariant()

docker compose exec -T `
    -e "SILVER_FORCE=$forceText" `
    -e "SILVER_MODE=$Mode" `
    -e "SILVER_TARGET=$Target" `
    -e "SILVER_MANIFEST_B64=$manifestBase64" `
    kestra sh -lc 'SILVER_MANIFEST=$(printf %s "$SILVER_MANIFEST_B64" | base64 --decode); curl --fail-with-body --silent --show-error --user "$KESTRA_BASIC_AUTH_USERNAME:$KESTRA_BASIC_AUTH_PASSWORD" --request POST --form-string "force=$SILVER_FORCE" --form-string "mode=$SILVER_MODE" --form-string "target=$SILVER_TARGET" --form-string "source_manifest=$SILVER_MANIFEST" http://localhost:8080/api/v1/main/executions/football.transfermarkt/transfermarkt_silver'
if ($LASTEXITCODE -ne 0) { throw 'Could not start the Kestra Silver flow.' }
