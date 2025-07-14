# This file should only be the FastAPI app entry point and router registration.
from fastapi import FastAPI
from api.routes import process
app = FastAPI()

app.include_router(process.router)
