[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

$flows = @(
    @{ Id = 'transfermarkt_ingest_asset'; File = '/opt/transfermarkt/flows/transfermarkt_ingest_asset.yml' },
    @{ Id = 'transfermarkt_ingest_bronze'; File = '/opt/transfermarkt/flows/transfermarkt_ingest_bronze.yml' }
)

foreach ($flow in $flows) {
    $update = 'curl --fail-with-body --silent --show-error --user "$KESTRA_BASIC_AUTH_USERNAME:$KESTRA_BASIC_AUTH_PASSWORD" --request PUT --header "Content-Type:application/x-yaml" --data-binary @' + $flow.File + ' http://localhost:8080/api/v1/main/flows/football.transfermarkt/' + $flow.Id + ' >/dev/null'
    docker compose exec -T kestra sh -lc $update
    if ($LASTEXITCODE -ne 0) {
        $create = 'curl --fail-with-body --silent --show-error --user "$KESTRA_BASIC_AUTH_USERNAME:$KESTRA_BASIC_AUTH_PASSWORD" --request POST --header "Content-Type:application/x-yaml" --data-binary @' + $flow.File + ' http://localhost:8080/api/v1/main/flows >/dev/null'
        docker compose exec -T kestra sh -lc $create
        if ($LASTEXITCODE -ne 0) { throw "Kestra rejected flow $($flow.Id)." }
    }
}

Write-Host 'Validated and registered flows in namespace football.transfermarkt.'

