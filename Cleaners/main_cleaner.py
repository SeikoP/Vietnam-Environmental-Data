from fastapi import FastAPI
from Cleaners.air_cleaner import router as air_router
from Cleaners.water_cleaner import router as water_router
from Cleaners.soil_cleaner import router as soil_router
from Cleaners.climate_cleaner import router as climate_router

app = FastAPI(title="Unified Cleaner Service")

app.include_router(air_router, prefix="/air_cleaner")
app.include_router(water_router, prefix="/water_cleaner")
app.include_router(soil_router, prefix="/soil_cleaner")
app.include_router(climate_router, prefix="/climate_cleaner")

@app.get("/")
def root():
    return {"service": "unified_cleaner", "status": "ok"}

@app.get("/health")
def health():
    return {"service": "unified_cleaner", "status": "ok"}
