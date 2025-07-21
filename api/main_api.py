from fastapi import FastAPI
from api.endpoints import consulta_01
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="API IXORIGUE")

# Permitir llamadas desde tu frontend Streamlit
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # o especifica tu dominio
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(consulta_01.router)
