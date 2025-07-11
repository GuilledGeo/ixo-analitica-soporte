# 📊 Sistema de Análisis y Visualización de Consultas

Este sistema automatiza el análisis y la visualización de CSVs generados por scripts de consulta como `consulta_01`, `consulta_02`, etc. Permite procesar múltiples archivos, evitar duplicados y organizar las gráficas en subcarpetas temáticas por tipo de análisis.

---

## 🛠️ Requisitos

- Python 3.8 o superior
- Librerías: `pandas`, `matplotlib`, `seaborn`

Instalación de dependencias:

```bash
pip install -r requirements.txt
```

Activación del entorno virtual:

```powershell
& "C:\1_IXORIGUE\1_Proyectos\1_envs\env_statsgenerales\Scripts\Activate.ps1"
```

---

## 📂 Estructura del proyecto

```
data/
└── processed/
    ├── consulta_01_2025-07-01_12-00_v01.csv
    └── ...

src/
└── analysis/
    ├── consulta_01.py
    └── consulta_02.py

outputs/
└── figures/
    └── consulta_01/
        ├── Distribución ratio de mensajes recibidos/
        │   └── consulta_01_2025-07-01_12-00_v01_hist_ratio_mensajes.png
        └── Clasificación de fallos por dispositivo/
            └── consulta_01_2025-07-01_12-00_v01_clasificacion_fallos.png
```

---

## ⚙️ ¿Cómo funciona?

### 1. CSVs de entrada

Los archivos CSV de entrada siguen este formato:

```
consulta_01_YYYY-MM-DD_HH-MM_vNN.csv
```

Se almacenan en `data/processed/`.

---

### 2. Ejecución del análisis

Edita `main_resultados.py` y define las consultas a ejecutar:

```python
CONSULTAS = ["consulta_01", "consulta_02"]
```

Luego lanza:

```bash
python main_resultados.py
```

Este script:

- Detecta los CSVs disponibles para cada consulta.
- Verifica si ya existen las gráficas correspondientes.
- Genera únicamente las gráficas que falten.
- Guarda las figuras en carpetas temáticas dentro de `outputs/figures/`.

---

### 3. Lógica del análisis

Cada script de análisis:

- Procesa automáticamente todos los CSVs existentes.
- Genera dos gráficas por CSV:
  - **Histograma del ratio de mensajes**
  - **Clasificación de fallos técnicos**
- Las figuras se nombran así:

```
consulta_01_2025-07-01_12-00_v01_hist_ratio_mensajes.png
consulta_01_2025-07-01_12-00_v01_clasificacion_fallos.png
```

Y se organizan en:

```
outputs/figures/consulta_01/Distribución ratio de mensajes recibidos/
outputs/figures/consulta_01/Clasificación de fallos por dispositivo/
```

---

## ✅ Mensajes del sistema

- Si una figura ya existe:  
  `✅ Figuras ya generadas para consulta_01_2025-07-01_12-00_v01.csv`

- Si todo está al día:  
  `✅ Todos los análisis de consulta_01 están al día. No se generaron nuevas figuras.`

---

## ➕ Añadir nuevas consultas

1. Crea un nuevo archivo en `src/analysis/` como `consulta_03.py`.
2. Copia la estructura de `consulta_01.py` y ajusta los gráficos.
3. Añade `"consulta_03"` a la lista `CONSULTAS` en `main_resultados.py`.

---

## 🧼 Limpieza manual (opcional)

Puedes eliminar todas las figuras de una consulta con:

```bash
rm -rf outputs/figures/consulta_01/*
```

---

## 🔧 Problemas con el intérprete de Python y solución

Si al ejecutar el proyecto en VS Code (o en notebooks interactivos) aparece que se está usando el **intérprete global** en lugar del entorno virtual `env_statsgenerales`, sigue estos pasos:

### ✅ Verificar el entorno virtual

1. Asegúrate de que el entorno virtual está correctamente activado en PowerShell:

```powershell
& "C:\1_IXORIGUE\1_Proyectos\1_envs\env_statsgenerales\Scripts\Activate.ps1"
```

2. Comprueba que `python` apunta al entorno y no al global:

```powershell
where python
```

La ruta correcta debería ser:

```
C:\1_IXORIGUE\1_Proyectos\1_envs\env_statsgenerales\Scripts\python.exe
```

---

### ⚙️ Configurar el intérprete en VS Code

1. Pulsa `Ctrl+Shift+P` → escribe `Python: Select Interpreter`
2. Si no aparece el entorno, selecciona manualmente:
```
C:\1_IXORIGUE\1_Proyectos\1_envs\env_statsgenerales\Scripts\python.exe
```

3. Verifica en la parte inferior izquierda que esté activo (`Python 3.10.x (env_statsgenerales)`)

---

### 🛠 Crear archivo `.vscode/settings.json` (opcional)

Para forzar permanentemente el intérprete correcto en este proyecto:

1. Crea la carpeta `.vscode` (si no existe).
2. Crea el archivo `.vscode/settings.json` con el siguiente contenido:

```json
{
  "python.defaultInterpreterPath": "C:\\1_IXORIGUE\\1_Proyectos\\1_envs\\env_statsgenerales\\Scripts\\python.exe"
}
```

---

### ❗ Si nada funciona

El entorno puede estar corrupto. En ese caso:

```powershell
# (1) Eliminar entorno
Remove-Item -Recurse -Force C:\1_IXORIGUE\1_Proyectos\1_envs\env_statsgenerales

# (2) Crear entorno nuevo
python -m venv C:\1_IXORIGUE\1_Proyectos\1_envs\env_statsgenerales

# (3) Activar
& "C:\1_IXORIGUE\1_Proyectos\1_envs\env_statsgenerales\Scripts\Activate.ps1"

# (4) Instalar dependencias
pip install -r requirements.txt
```