param(
    [switch]$ProducerOnce,
    [double]$IntervalMinutes = 15
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$venvActivate = Join-Path $root 'venv\Scripts\Activate.ps1'
if (-not (Test-Path $venvActivate)) {
    throw "Virtualenv not found at: $venvActivate. Create it with: python -m venv venv"
}

function Start-AgentTerminal {
    param(
        [Parameter(Mandatory=$true)][string]$Title,
        [Parameter(Mandatory=$true)][string]$PyCommand
    )

    $cmd = "Set-Location '$root'; . '$venvActivate'; $PyCommand"

    Start-Process -FilePath 'powershell' -ArgumentList @(
        '-NoExit',
        '-Command',
        $cmd
    ) -WindowStyle Normal | Out-Null

    Write-Host "[OK] Started: $Title"
}

Write-Host "============================================================"
Write-Host "🚀 Starting Market Intelligence Pipeline"
Write-Host "Root: $root"
Write-Host "============================================================"

# Start long-running consumers first
Start-AgentTerminal -Title 'News + Sentiment Enricher' -PyCommand 'python backend/streaming/news_sentiment_enricher.py'
Start-Sleep -Seconds 1

Start-AgentTerminal -Title 'Signal Engine' -PyCommand 'python backend/streaming/signal_engine.py'
Start-Sleep -Seconds 1

Start-AgentTerminal -Title 'Dashboard API (Socket.IO)' -PyCommand 'python backend/app.py'
Start-Sleep -Seconds 2

# Producer: once (demo) or continuous
if ($ProducerOnce) {
    Start-AgentTerminal -Title 'Kafka Producer (once)' -PyCommand 'python backend/streaming/kafka_producer.py --once'
} else {
    Start-AgentTerminal -Title 'Kafka Producer (continuous)' -PyCommand ("python backend/streaming/kafka_producer.py --interval-minutes $IntervalMinutes")
}

Write-Host "============================================================"
Write-Host "✅ Services launched"
Write-Host "- WebSocket UI connects to: http://localhost:5051"
Write-Host "- HTTP verify: http://localhost:5051/api/signals"
Write-Host "============================================================"
