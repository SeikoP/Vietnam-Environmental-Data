# This file should only be the FastAPI app entry point and router registration.
from fastapi import FastAPI
from api.routes import process,air_quality

app = FastAPI()

app.include_router(process.router)
app.include_router(air_quality.router)