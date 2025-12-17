# PowerShell helper to run faker_db_populate.py safely
# Avoid using the automatic variable name $host; use $pgHost instead.

Set-StrictMode -Version Latest

if (-not (Test-Path .\venv\Scripts\Activate.ps1)) {
    Write-Host "Virtual environment not found. Creating venv..."
    python -m venv .\venv
}

Write-Host "Activating venv..."
. .\venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

$pgUser = Read-Host "Postgres username"
$pgPassSecure = Read-Host -AsSecureString "Postgres password"
$bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($pgPassSecure)
$pgPass = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)

$pgHost = Read-Host "Host (default: localhost)"
if ([string]::IsNullOrWhiteSpace($pgHost)) { $pgHost = 'localhost' }

$pgPort = Read-Host "Port (default: 5432)"
if ([string]::IsNullOrWhiteSpace($pgPort)) { $pgPort = '5432' }

$ops = Read-Host "Operations (default: 200)"
if ([string]::IsNullOrWhiteSpace($ops)) { $ops = '200' }

python faker_db_populate.py --host $pgHost --port $pgPort --user $pgUser --password $pgPass --dbname finance --operations $ops
