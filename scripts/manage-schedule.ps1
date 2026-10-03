[CmdletBinding()]
param(
    [ValidateSet('Preview', 'Create', 'Pause', 'Resume', 'Cancel', 'Enable', 'Disable')]
    [string]$Action = 'Preview',
    [string]$Start,
    [string]$End,
    [string[]]$Assets = @(
        'competitions', 'clubs', 'players', 'games', 'appearances',
        'player_valuations', 'transfers', 'club_games', 'game_events',
        'game_lineups', 'countries', 'national_teams'
    )
)

$ErrorActionPreference = 'Stop'
$namespace = 'football.transfermarkt'
$flowId = 'transfermarkt_ingest_bronze'
$triggerId = 'monthly_first_day'

function Convert-ToInstant([string]$Value, [string]$Name) {
    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw "-$Name is required for $Action. Use an explicit finite RFC3339 timestamp."
    }
    $parsed = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse(
        $Value,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::RoundtripKind,
        [ref]$parsed
    )) {
        throw "-$Name must be an RFC3339 timestamp, for example 2026-01-01T00:00:00-05:00."
    }
    return $parsed
}

function Get-MonthlyOccurrences([DateTimeOffset]$From, [DateTimeOffset]$To) {
    # America/Guayaquil is UTC-05:00 year-round; no DST adjustment is required.
    $guayaquilOffset = [TimeSpan]::FromHours(-5)
    $localFrom = $From.ToOffset($guayaquilOffset)
    $cursor = [DateTimeOffset]::new(
        $localFrom.Year,
        $localFrom.Month,
        1,
        6,
        0,
        0,
        $guayaquilOffset
    )
    if ($cursor -lt $From) { $cursor = $cursor.AddMonths(1) }
    $dates = @()
    while ($cursor -le $To) {
        $dates += $cursor
        $cursor = $cursor.AddMonths(1)
    }
    return $dates
}

function Invoke-KestraJson([string]$Method, [string]$Path, [object]$Body) {
    $json = ConvertTo-Json -InputObject $Body -Depth 8 -Compress
    $bodyBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($json))
    $command = 'TM_API_BODY=$(printf %s "$TM_API_BODY_B64" | base64 --decode); ' +
        'curl --fail-with-body --silent --show-error ' +
        '--user "$KESTRA_BASIC_AUTH_USERNAME:$KESTRA_BASIC_AUTH_PASSWORD" ' +
        "--request $Method --header 'Content-Type:application/json' " +
        '--data "$TM_API_BODY" ' +
        "http://localhost:8080$Path"
    docker compose exec -T -e "TM_API_BODY_B64=$bodyBase64" kestra sh -lc $command
    if ($LASTEXITCODE -ne 0) { throw "Kestra rejected $Method $Path." }
}

if ($Action -in @('Preview', 'Create')) {
    $startInstant = Convert-ToInstant $Start 'Start'
    $endInstant = Convert-ToInstant $End 'End'
    if ($endInstant -le $startInstant) { throw '-End must be later than -Start.' }
    $occurrences = @(Get-MonthlyOccurrences $startInstant $endInstant)

    Write-Host "Finite backfill preview: $($occurrences.Count) monthly execution(s)"
    foreach ($occurrence in $occurrences) {
        Write-Host ("  logical_date={0}  effective_query=current snapshot at execution time" -f $occurrence.ToString('o'))
    }
    if ($Action -eq 'Preview') { return }
    if ($occurrences.Count -eq 0) {
        throw 'The finite interval contains no first-day 06:00 America/Guayaquil schedule.'
    }

    $payload = @{
        namespace = $namespace
        flowId = $flowId
        triggerId = $triggerId
        backfill = @{
            start = $startInstant.ToUniversalTime().ToString('o')
            end = $endInstant.ToUniversalTime().ToString('o')
            inputs = @{
                mode = 'backfill'
                assets = @($Assets)
            }
            labels = @(
                @{ key = 'reason'; value = 'finite-monthly-backfill' }
            )
        }
    }
    Invoke-KestraJson 'PUT' '/api/v1/main/triggers' $payload
    return
}

$triggerReference = @{
    namespace = $namespace
    flowId = $flowId
    triggerId = $triggerId
}

switch ($Action) {
    'Pause' {
        Invoke-KestraJson 'PUT' '/api/v1/main/triggers/backfill/pause' $triggerReference
    }
    'Resume' {
        Invoke-KestraJson 'PUT' '/api/v1/main/triggers/backfill/unpause' $triggerReference
    }
    'Cancel' {
        Invoke-KestraJson 'POST' '/api/v1/main/triggers/backfill/delete' $triggerReference
    }
    'Enable' {
        Invoke-KestraJson 'POST' '/api/v1/main/triggers/set-disabled/by-triggers' @{
            triggers = @($triggerReference)
            disabled = $false
        }
    }
    'Disable' {
        Invoke-KestraJson 'POST' '/api/v1/main/triggers/set-disabled/by-triggers' @{
            triggers = @($triggerReference)
            disabled = $true
        }
    }
}
