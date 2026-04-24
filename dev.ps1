# Windows session bootstrap — dot-source this once per terminal:
#   . .\dev.ps1
#
# This activates the venv AND loads .env vars into the current shell process
# so that dbt, Python scripts, and make targets all see them.
# Without dot-sourcing (i.e. running .\dev.ps1 directly), the env vars
# disappear when the script exits.

# 1. Activate venv
if (Test-Path ".venv\Scripts\Activate.ps1") {
    . .venv\Scripts\Activate.ps1
} else {
    Write-Warning "No .venv found. Run: python -m venv .venv && pip install -r requirements.txt"
    return
}

# 2. Load .env into current process
if (Test-Path ".env") {
    Get-Content .env | Where-Object { $_ -match '^\w' } | ForEach-Object {
        $k, $v = $_ -split '=', 2
        [System.Environment]::SetEnvironmentVariable($k, $v.Trim(), 'Process')
    }
    Write-Host "venv activated + .env loaded" -ForegroundColor Green
} else {
    Write-Warning ".env not found. Copy .env.example to .env and fill in your credentials."
}
