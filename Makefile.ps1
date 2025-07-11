# Makefile.ps1

$TUNEL_SCRIPT = ".\abrir_tunel.bat"
$PYTHON_ENV_PATH = "C:\1_IXORIGUE\1_Proyectos\1_envs\env_statsgenerales\Scripts\python.exe"
$CONSULTA_SCRIPT = ".\scripts\consultas\db_explorer.py"

Write-Host "🌀 Abriendo túnel con Kubernetes..."
Start-Process -FilePath $TUNEL_SCRIPT
Start-Sleep -Seconds 2

Write-Host "`n📡 Ejecutando consulta..."
& $PYTHON_ENV_PATH $CONSULTA_SCRIPT
