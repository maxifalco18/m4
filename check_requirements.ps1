# check_requirements.ps1
# Verifica herramientas necesarias para el Proyecto Integrador M4
# Uso: powershell -ExecutionPolicy Bypass -File check_requirements.ps1

$issues = @()

function Check-Cmd($label, $cmd, $arg, $hint) {
    try {
        $out = (& $cmd $arg 2>&1) | Out-String
        $ver = ($out -split "`n")[0].Trim()
        Write-Host " [OK]    $label  ->  $ver" -ForegroundColor Green
    } catch {
        Write-Host " [FALTA] $label  ->  No encontrado.  $hint" -ForegroundColor Red
        $script:issues += $label
    }
}

function Check-Py($pkg, $hint) {
    $r = python -c "import $pkg; print('ok')" 2>&1
    if ($r -match "ok") {
        $v = python -c "import $pkg; v=getattr($pkg,'__version__','?'); print(v)" 2>&1
        Write-Host " [OK]    $pkg  ->  $v" -ForegroundColor Green
    } else {
        Write-Host " [FALTA] $pkg  ->  pip install $hint" -ForegroundColor Red
        $script:issues += "pip:$pkg"
    }
}

Write-Host ""
Write-Host "========================================================"
Write-Host "  CHECK DE REQUISITOS - Proyecto Integrador M4"
Write-Host "========================================================"
Write-Host ""

Write-Host "--- Python ---" -ForegroundColor Yellow
Check-Cmd "Python"   "python"  "--version"  "https://python.org/downloads"
Check-Cmd "pip"      "pip"     "--version"  "Viene con Python"

Write-Host ""
Write-Host "--- AWS CLI ---" -ForegroundColor Yellow
Check-Cmd "AWS CLI"  "aws"     "--version"  "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"

# Verificar credenciales
try {
    $id = aws sts get-caller-identity 2>&1 | Out-String
    if ($id -match "UserId") {
        Write-Host " [OK]    AWS credenciales configuradas" -ForegroundColor Green
    } else {
        Write-Host " [WARN]  AWS CLI instalado pero SIN credenciales ->  aws configure sso" -ForegroundColor DarkYellow
    }
} catch {
    Write-Host " [WARN]  No se pudo verificar credenciales AWS" -ForegroundColor DarkYellow
}

Write-Host ""
Write-Host "--- Docker ---" -ForegroundColor Yellow
Check-Cmd "Docker"           "docker"          "--version"       "https://docs.docker.com/desktop/install/windows-install"
Check-Cmd "Docker Compose"   "docker-compose"  "--version"       "Incluido en Docker Desktop"

# Verificar daemon
$dp = docker ps 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host " [OK]    Docker daemon corriendo" -ForegroundColor Green
} else {
    Write-Host " [WARN]  Docker instalado pero daemon NO corre  ->  Abrir Docker Desktop" -ForegroundColor DarkYellow
}

Write-Host ""
Write-Host "--- Git ---" -ForegroundColor Yellow
Check-Cmd "Git"  "git"  "--version"  "https://git-scm.com/download/win"

Write-Host ""
Write-Host "--- Java (requerido por Spark) ---" -ForegroundColor Yellow
Check-Cmd "Java"  "java"  "-version"  "https://adoptium.net (Temurin 11 o 17)"

Write-Host ""
Write-Host "--- Apache Spark ---" -ForegroundColor Yellow
Check-Cmd "spark-submit"  "spark-submit"  "--version"  "pip install pyspark  O  https://spark.apache.org/downloads.html"

Write-Host ""
Write-Host "--- Paquetes Python ---" -ForegroundColor Yellow
Check-Py "boto3"               "boto3"
Check-Py "pyspark"             "pyspark"
Check-Py "pytest"              "pytest pytest-cov"
Check-Py "great_expectations"  "great-expectations"
Check-Py "kafka"               "kafka-python"
Check-Py "requests"            "requests"
Check-Py "dotenv"              "python-dotenv"

Write-Host ""
Write-Host "--- Airbyte ---" -ForegroundColor Yellow
Write-Host " [OK]    Airbyte es SaaS - no se instala localmente. Ver: https://cloud.airbyte.com" -ForegroundColor Green

Write-Host ""
Write-Host "========================================================"
if ($issues.Count -eq 0) {
    Write-Host "  RESULTADO: Todo OK. Listo para ejecutar el proyecto." -ForegroundColor Green
} else {
    Write-Host "  RESULTADO: Faltan $($issues.Count) herramienta(s):" -ForegroundColor Red
    foreach ($i in $issues) { Write-Host "    - $i" -ForegroundColor Red }
    Write-Host ""
    Write-Host "  Instalar paquetes Python de una sola vez:" -ForegroundColor Yellow
    Write-Host "    pip install -r requirements.txt" -ForegroundColor Yellow
}
Write-Host "========================================================"
Write-Host ""
