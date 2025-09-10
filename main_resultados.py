import sys
import os
import importlib
import traceback

# === CONFIGURACIÓN MANUAL ===
CONSULTAS = [
    "consulta_01",
    "consutla_02"
    #"consulta_03"
    
    ]  # ← Selecciona aquí las consultas a ejecutar

# Añadir src al path para importaciones
sys.path.append(os.path.abspath("."))

# Ruta robusta a la carpeta de análisis
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
analysis_path = os.path.join(BASE_DIR, "src", "analysis")

def verificar_consultas_disponibles():
    archivos = [
        f for f in os.listdir(analysis_path)
        if f.startswith("consulta_") and f.endswith(".py")
    ]
    disponibles = [f.replace(".py", "") for f in archivos]
    return sorted(disponibles)

def main():
    print("\n📈 EJECUTANDO INFORMES ANALÍTICOS")
    exitosos = []
    fallidos = []

    disponibles = verificar_consultas_disponibles()
    consultas_a_ejecutar = [c for c in CONSULTAS if c in disponibles]

    for nombre in consultas_a_ejecutar:
        print(f"\n🚀 Analizando: {nombre}")
        try:
            modulo = importlib.import_module(f"src.analysis.{nombre}")
            modulo.ejecutar()
            exitosos.append(nombre)
        except Exception as e:
            print(f"❌ Error en {nombre}:\n{traceback.format_exc()}")
            fallidos.append(nombre)

    no_encontradas = [c for c in CONSULTAS if c not in disponibles]

    # Resumen final
    print("\n📊 RESUMEN DE ANÁLISIS:")
    print(f"✅ Completados: {len(exitosos)}/{len(consultas_a_ejecutar)}")
    if exitosos:
        print("   - " + ", ".join(exitosos))
    if fallidos:
        print(f"❌ Fallidos: {len(fallidos)}")
        print("   - " + ", ".join(fallidos))
    if no_encontradas:
        print(f"⚠️ No encontradas en src/analysis/: {len(no_encontradas)}")
        print("   - " + ", ".join(no_encontradas))

if __name__ == "__main__":
    main()
