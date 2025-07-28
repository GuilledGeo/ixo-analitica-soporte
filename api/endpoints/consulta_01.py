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

        # Convertir NaN/inf a None para que sea JSON serializable
        df = df.replace({pd.NA: None, pd.NaT: None})
        df = df.fillna(value=None)

        return df.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}
