param(
    [string]$Master = "local[8]",
    [int]$ShufflePartitions = 32,
    [double]$SampleFraction = 0.01,
    [string]$OutputDir = "outputs/eda",
    [string]$ScratchDir = ""
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $python)) {
    throw "Environnement Python absent. Exécutez d'abord : python -m venv .venv; .\.venv\Scripts\python.exe -m pip install -r requirements.txt"
}

if (-not $env:JAVA_HOME) {
    $javaCandidates = @(
        "C:\Program Files\Neo4j Desktop 2\resources\offline\runtime\zulu17.60.17-ca-jdk17.0.16-win_x64",
        "C:\Program Files\Eclipse Adoptium\jdk-17"
    )
    $env:JAVA_HOME = $javaCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
}

if (-not $env:JAVA_HOME -or -not (Test-Path -LiteralPath (Join-Path $env:JAVA_HOME "bin\java.exe"))) {
    throw "Java 17 est introuvable. Configurez JAVA_HOME puis relancez le script."
}

$env:Path = "$(Join-Path $env:JAVA_HOME 'bin');$env:Path"
$arguments = @(
    "src/phase1_eda.py",
    "--master", $Master,
    "--shuffle-partitions", $ShufflePartitions,
    "--sample-fraction", $SampleFraction,
    "--output-dir", $OutputDir
)
if ($ScratchDir) {
    $arguments += @("--scratch-dir", $ScratchDir)
}

Push-Location $projectRoot
try {
    & $python @arguments
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
