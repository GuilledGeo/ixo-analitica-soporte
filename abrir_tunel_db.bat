@echo off
set KUBECONFIG=C:\1_IXORIGUE\1_Proyectos\0_K8S\kubeconfig\guille.kubeconfig.yaml

echo ===============================
echo Abriendo túnel con Kubernetes...
echo ===============================
kubectl port-forward svc/pro-configdb-repl 5433:5432 -n database
pause
