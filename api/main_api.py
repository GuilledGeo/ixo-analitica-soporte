from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.endpoints import consulta_01  # Asegúrate de que esta ruta es correcta

app = FastAPI(
    title="API IXORIGUE",
    description="API para servir datos a dashboards y herramientas internas.",
    version="1.0.0"
)

# Middleware CORS para permitir llamadas desde Streamlit u otras apps externas
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Puedes reemplazar "*" por ["https://tuapp.streamlit.app"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ruta raíz de bienvenida o test
@app.get("/")
def read_root():
    return {"message": "🚀 API IXORIGUE activa y funcionando"}

# Incluir el router de consulta_01
app.include_router(consulta_01.router)
