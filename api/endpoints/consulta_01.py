from fastapi import APIRouter

router = APIRouter()

# Endpoint de diagnóstico: paso 1 – Verifica si el endpoint funciona sin importar nada
@router.get("/consulta_01")
def obtener_resultados():
    try:
        # Paso 2 – Importar dentro del try para capturar fallos de importación
        from api.db.connection import get_engine
        from scripts.consultas.consulta_01 import ejecutar

        # Paso 3 – Verificar si la conexión se establece
        engine = get_engine()

        # Paso 4 – Ejecutar la consulta
        df = ejecutar(engine)

        # Paso 5 – Convertir resultado
        return df.to_dict(orient="records")

    except Exception as e:
        # Devuelve el error como texto legible
        return {"error": str(e)}
