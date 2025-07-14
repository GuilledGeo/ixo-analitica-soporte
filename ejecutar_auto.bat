@echo off
REM Ejecutar script con entorno virtual activado desde PowerShell

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"& 'C:\1_IXORIGUE\1_Proyectos\1_envs\env_statsgenerales\Scripts\Activate.ps1'; ^
cd 'C:\1_IXORIGUE\1_Proyectos\0_K8S'; ^
python main_consultas.py"
