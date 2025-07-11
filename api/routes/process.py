from sqlalchemy import create_engine
import os
import json
from datetime import datetime

DB_URL = (
    os.getenv("DATABASE_URL")
    or "postgresql+psycopg2://postgres:0946932602a@postgres:5432/air_quality_db"
)
engine = create_engine(DB_URL)

def query_postgres(table_name: str, limit: int = 100):
    """Truy vấn dữ liệu mới nhất từ bảng Postgres."""
    import pandas as pd
    try:
        query = f'SELECT * FROM "{table_name}" ORDER BY timestamp DESC LIMIT {limit}'
        df = pd.read_sql(query, engine)
        return {
            "success": True,
            "total_rows": len(df),
            "columns": list(df.columns),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        return {"success": False, "message": str(e), "data": []}

from fastapi import APIRouter

router = APIRouter()

@router.get("/pg_clean_air")
def pg_clean_air():
    """API lấy dữ liệu sạch mới nhất của air từ Postgres."""
    return query_postgres("AirQualityRecord", 100)

@router.get("/pg_clean_water")
def pg_clean_water():
    """API lấy dữ liệu sạch mới nhất của water từ Postgres."""
    return query_postgres("water_record", 100)

@router.get("/pg_clean_soil")
def pg_clean_soil():
    """API lấy dữ liệu sạch mới nhất của soil từ Postgres."""
    return query_postgres("soil_record", 100)

@router.get("/pg_clean_climate")
def pg_clean_climate():
    """API lấy dữ liệu sạch mới nhất của climate từ Postgres."""
    return query_postgres("climate_record", 100)

@router.get("/pg_clean_all")
def pg_clean_all():
    """API tổng hợp dữ liệu sạch mới nhất của tất cả loại từ Postgres."""
    return {
        "air_quality": query_postgres("AirQualityRecord", 100).get("data", []),
        "water_quality": query_postgres("water_record", 100).get("data", []),
        "soil_quality": query_postgres("soil_record", 100).get("data", []),
        "climate": query_postgres("climate_record", 100).get("data", [])
    }

@router.get("/pg_ai_preprocess")
def pg_ai_preprocess():
    """
    API tổng hợp và tiền xử lý dữ liệu sạch từ Postgres để chuẩn bị cho AI phân tích.
    Trả về dict gồm các trường dữ liệu và trường environmental_data (JSON string).
    """
    air = query_postgres("AirQualityRecord", 100).get("data", [])
    water = query_postgres("water_record", 100).get("data", [])
    soil = query_postgres("soil_record", 100).get("data", [])
    climate = query_postgres("climate_record", 100).get("data", [])
    combined = {
        "timestamp": datetime.now().isoformat(),
        "air_quality": air,
        "water_quality": water,
        "soil_quality": soil,
        "climate": climate
    }
    return {
        "success": True,
        "environmental_data": json.dumps(combined, ensure_ascii=False),
        "air_quality": air,
        "water_quality": water,
        "soil_quality": soil,
        "climate": climate
    }