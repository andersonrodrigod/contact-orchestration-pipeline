Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Garante execucao a partir da raiz do projeto, mesmo se chamado de outra pasta.
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

Write-Host "Executando suite de testes (unittest discover)..." -ForegroundColor Cyan
python -m unittest discover -s tests -p "test_*.py"
