param(
    [switch]$StopExisting
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendDir = Join-Path $root "apps\backend"
$frontendDir = Join-Path $root "apps\frontend"
$logsDir = Join-Path $root "logs"

$backendOutLog = Join-Path $logsDir "backend_8001.out.log"
$backendErrLog = Join-Path $logsDir "backend_8001.err.log"
$frontendOutLog = Join-Path $logsDir "frontend_3000.out.log"
$frontendErrLog = Join-Path $logsDir "frontend_3000.err.log"

function Get-ListeningProcessId([int]$Port) {
    $conn = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue
    if ($null -eq $conn) {
        return $null
    }
    return $conn.OwningProcess
}

function Ensure-EnvFile([string]$targetPath, [string]$examplePath) {
    if (-not (Test-Path $targetPath)) {
        if (-not (Test-Path $examplePath)) {
            throw "Missing env file and example: $targetPath / $examplePath"
        }
        Copy-Item $examplePath $targetPath
        Write-Host "Created env file from example: $targetPath"
    }
}

function Resolve-BackendPython() {
    $pythonFromPath = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonFromPath) {
        return $pythonFromPath.Path
    }
    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($pyLauncher) {
        return "$($pyLauncher.Path) -3"
    }
    throw "Python was not found. Install Python or create apps/backend/.venv first."
}

function Resolve-NpmCommand() {
    $npmFromPath = Get-Command npm.cmd -ErrorAction SilentlyContinue
    if ($npmFromPath) {
        return $npmFromPath.Path
    }
    $fallback = "C:\Program Files\nodejs\npm.cmd"
    if (Test-Path $fallback) {
        return $fallback
    }
    throw "npm.cmd was not found. Install Node.js or add npm to PATH."
}

New-Item -ItemType Directory -Force -Path $logsDir | Out-Null

$backendEnv = Join-Path $backendDir ".env"
$backendEnvExample = Join-Path $backendDir ".env.example"
$frontendEnv = Join-Path $frontendDir ".env.local"
$frontendEnvExample = Join-Path $frontendDir ".env.example"

Ensure-EnvFile -targetPath $backendEnv -examplePath $backendEnvExample
Ensure-EnvFile -targetPath $frontendEnv -examplePath $frontendEnvExample

$backendPid = Get-ListeningProcessId -Port 8001
$frontendPid = Get-ListeningProcessId -Port 3000

if ($StopExisting) {
    if ($backendPid) {
        Stop-Process -Id $backendPid -Force
        Start-Sleep -Seconds 1
        Write-Host "Stopped existing backend process on 8001 (PID: $backendPid)"
    }
    if ($frontendPid) {
        Stop-Process -Id $frontendPid -Force
        Start-Sleep -Seconds 1
        Write-Host "Stopped existing frontend process on 3000 (PID: $frontendPid)"
    }
    $backendPid = $null
    $frontendPid = $null
}

if (-not $backendPid) {
    $backendPython = Resolve-BackendPython
    if ($backendPython -match " -3$") {
        $parts = $backendPython.Split(" ", 2)
        Start-Process -FilePath $parts[0] -ArgumentList $parts[1], "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8001" -WorkingDirectory $backendDir -RedirectStandardOutput $backendOutLog -RedirectStandardError $backendErrLog
    } else {
        Start-Process -FilePath $backendPython -ArgumentList "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8001" -WorkingDirectory $backendDir -RedirectStandardOutput $backendOutLog -RedirectStandardError $backendErrLog
    }
    Write-Host "Started backend on http://127.0.0.1:8001"
} else {
    Write-Host "Backend already running on 8001 (PID: $backendPid)"
}

if (-not $frontendPid) {
    $npmCmd = Resolve-NpmCommand
    Start-Process -FilePath $npmCmd -ArgumentList "run", "dev", "--", "--port", "3000", "--hostname", "127.0.0.1" -WorkingDirectory $frontendDir -RedirectStandardOutput $frontendOutLog -RedirectStandardError $frontendErrLog
    Write-Host "Started frontend on http://127.0.0.1:3000"
} else {
    Write-Host "Frontend already running on 3000 (PID: $frontendPid)"
}

Write-Host ""
Write-Host "App URLs:"
Write-Host "  Frontend: http://127.0.0.1:3000"
Write-Host "  Backend:  http://127.0.0.1:8001"
Write-Host ""
Write-Host "Logs:"
Write-Host "  $backendOutLog"
Write-Host "  $backendErrLog"
Write-Host "  $frontendOutLog"
Write-Host "  $frontendErrLog"
Write-Host ""
Write-Host "Tip: run '.\start.ps1 -StopExisting' to restart both services."
