[CmdletBinding()]
param(
    [ValidateSet('incremental', 'backfill')]
    [string]$Mode = 'incremental',
    [string[]]$Assets = @('competitions')
)

$ErrorActionPreference = 'Stop'
$assetsJson = ConvertTo-Json -InputObject @($Assets) -Compress
$assetsBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($assetsJson))

docker compose exec -T -e "TM_MODE=$Mode" -e "TM_ASSETS_B64=$assetsBase64" kestra sh -lc 'TM_ASSETS=$(printf %s "$TM_ASSETS_B64" | base64 --decode); curl --fail-with-body --silent --show-error --user "$KESTRA_BASIC_AUTH_USERNAME:$KESTRA_BASIC_AUTH_PASSWORD" --request POST --form-string "mode=$TM_MODE" --form-string "assets=$TM_ASSETS" http://localhost:8080/api/v1/main/executions/football.transfermarkt/transfermarkt_ingest_bronze'
if ($LASTEXITCODE -ne 0) { throw 'Could not start the Kestra flow.' }

