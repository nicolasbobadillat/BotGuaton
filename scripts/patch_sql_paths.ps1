param(
  [string]$OldDagsBaseFwd = "C:/Users/Nico/Documents/py/datitos_nam/dags",
  [string]$NewDagsBaseFwd = "C:/Users/Nico/Documents/py/datitos_nam_portfolio/dags",
  [string]$OldDagsBaseBwd = "C:\Users\Nico\Documents\py\datitos_nam\dags",
  [string]$NewDagsBaseBwd = "C:\Users\Nico\Documents\py\datitos_nam_portfolio\dags"
)

$transformerDir = Join-Path $PSScriptRoot "..\dags\sql\transformers"
$files = Get-ChildItem $transformerDir -Filter *.sql -File -Recurse
$changed = 0

foreach ($f in $files) {
  $content = Get-Content $f.FullName -Raw -Encoding UTF8
  $updated = $content.Replace($OldDagsBaseFwd, $NewDagsBaseFwd)
  $updated = $updated.Replace($OldDagsBaseBwd, $NewDagsBaseBwd)

  if ($updated -ne $content) {
    Set-Content -Path $f.FullName -Value $updated -Encoding UTF8
    Write-Output "patched: $($f.FullName)"
    $changed++
  }
}

Write-Output "total_patched_files=$changed"
