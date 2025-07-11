import sys
import os
import pandas as pd
import importlib
import inspect
from datetime import datetime
from src.db.connection import conectar_db
import src.features.consulta_1 as features_modulo

sys.path.append(os.path.abspath("."))

CONSULTAS = [
    "consulta_01",
    # "consulta_02",
    # "consulta_03"
]

def generar_nombre_versionado(nombre_base, carpeta="data/processed", extension=".csv"):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    base = f"{nombre_base}_{timestamp}"
    version = 1

    while True:
        nombre_completo = f"{base}_v{version:02d}{extension}"
        ruta_completa = os.path.join(carpeta, nombre_completo)
        if not os.path.exists(ruta_completa):
            return ruta_completa
        version += 1

def probar_conexion():
    print("🔗 Probando conexión a la base de datos...")
    try:
        engine = conectar_db()
        conn = engine.connect()
        print("✅ Conexión exitosa.")
        return conn
    except Exception as e:
        print("❌ Error al conectar:", e)
        return None

def aplicar_features_dinamicamente(df):
    print("✨ Aplicando funciones de features...")
    funciones = inspect.getmembers(features_modulo, inspect.isfunction)
    aplicadas = []

    for nombre_funcion, funcion in funciones:
        if nombre_funcion.startswith("aplicar_"):
            try:
                print(f"  ➤ Aplicando `{nombre_funcion}`")
                df = funcion(df)
                aplicadas.append(nombre_funcion)
            except Exception as e:
                print(f"    ⚠️ Error en `{nombre_funcion}`: {e}")

    print(f"✅ Se aplicaron {len(aplicadas)} funciones de features.")
    return df

def ejecutar_consulta(nombre_consulta, conn):
    print(f"\n🚀 Ejecutando consulta: {nombre_consulta}")
    try:
        # Cargar módulo de consulta
        modulo = importlib.import_module(f"scripts.consultas.{nombre_consulta}")
        df = modulo.ejecutar(conn)

        # Aplicar features automáticamente si es consulta_01
        if nombre_consulta == "consulta_01":
            df = aplicar_features_dinamicamente(df)

        # Guardar resultado
        os.makedirs("data/processed", exist_ok=True)
        ruta_salida = generar_nombre_versionado(nombre_consulta)
        df.to_csv(ruta_salida, index=False)
        print(f"📁 Consulta guardada en: {ruta_salida}")
        return True

    except Exception as e:
        print(f"❌ Error al ejecutar la consulta {nombre_consulta}: {e}")
        return False

def main():
    conn = probar_conexion()
    if conn:
        exitosas = []
        fallidas = []

        for consulta in CONSULTAS:
            resultado = ejecutar_consulta(consulta, conn)
            if resultado:
                exitosas.append(consulta)
            else:
                fallidas.append(consulta)

        conn.close()

        # 🔚 Resumen
        print("\n📊 RESUMEN FINAL:")
        print(f"✅ Consultas exitosas: {len(exitosas)}/{len(CONSULTAS)}")
        if exitosas:
            print("   - " + ", ".join(exitosas))
        if fallidas:
            print(f"❌ Consultas con error: {len(fallidas)}")
            print("   - " + ", ".join(fallidas))
        else:
            print("🎉 Todas las consultas se ejecutaron correctamente.")

if __name__ == "__main__":
    main()
