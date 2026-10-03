[CmdletBinding()]
param([int]$TimeoutSeconds = 180)

$ErrorActionPreference = 'Stop'
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
do {
    docker compose exec -T kestra sh -lc 'curl --fail --silent http://localhost:8081/health >/dev/null' 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host 'Kestra is ready.'
        exit 0
    }
    Start-Sleep -Seconds 3
} while ((Get-Date) -lt $deadline)

throw "Kestra did not become ready within $TimeoutSeconds seconds."

