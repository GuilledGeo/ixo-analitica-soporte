from fastapi import APIRouter
import pandas as pd
from api.db.connection import get_engine
from scripts.consultas.consulta_01 import ejecutar

router = APIRouter()

@router.get("/consulta_01")
def obtener_resultados():
    try:
        engine = get_engine()
        df = ejecutar(engine)

        # Seguridad extra por si quedaron NaN, inf o tipos no JSON-compatibles
        df = df.replace([float('inf'), float('-inf')], 0)
        df = df.where(pd.notnull(df), None)  # convierte NaN/NaT a None

        return df.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}
