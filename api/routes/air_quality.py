from fastapi import APIRouter
from api.utils.db import get_engine
import pandas as pd

router = APIRouter()

@router.get("/latest-by-city")
def latest_by_city():
    engine = get_engine()
    try:
        query = """
            SELECT DISTINCT ON (city_id) *
            FROM "AirQualityRecord"
            ORDER BY city_id, timestamp DESC
        """
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
