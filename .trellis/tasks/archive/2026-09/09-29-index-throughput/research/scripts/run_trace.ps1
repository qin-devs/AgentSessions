param(
  [Parameter(Mandatory=$true)][string]$Dataset,
  [Parameter(Mandatory=$true)][string]$DbDir,
  [Parameter(Mandatory=$true)][string]$TraceFile,
  [int]$MaxSourcesPerBatch = 40
)
$ErrorActionPreference = 'Stop'
$ws = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../../../../../../..')).Path
$bin = "$ws/target/release/agent-session-grep.exe"
New-Item -ItemType Directory -Force -Path $DbDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $TraceFile) | Out-Null
if (Test-Path $TraceFile) { throw "trace file exists: $TraceFile" }
if (Test-Path (Join-Path $DbDir 'catalog.db')) { throw "catalog exists in $DbDir" }
$env:ASG_INDEX_TRACE = ($TraceFile -replace '/', '\')
$files = @(Get-ChildItem -LiteralPath $Dataset -Filter 'session-*.jsonl' | Sort-Object Name | ForEach-Object { $_.FullName })
Write-Output "files=$($files.Count) batches=$([math]::Ceiling($files.Count / $MaxSourcesPerBatch))"
$db = (Join-Path $DbDir 'catalog.db') -replace '/', '\'
$index = 0
for ($offset = 0; $offset -lt $files.Count; $offset += $MaxSourcesPerBatch) {
  $batch = $files[$offset..([math]::Min($offset + $MaxSourcesPerBatch - 1, $files.Count - 1))]
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $out = & $bin --db $db --output json sync @batch
  $sw.Stop()
  Write-Output ("batch {0} files={1} wall_ms={2} out={3}" -f $index, $batch.Count, $sw.ElapsedMilliseconds, ($out -join ' '))
  $index++
}
$env:ASG_INDEX_TRACE = $null
