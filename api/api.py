# This file should only be the FastAPI app entry point and router registration.
from fastapi import FastAPI, Request, Query
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
import pandas as pd
import os

DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)
templates = Jinja2Templates(directory="templates")

app = FastAPI()

# --- Core API routes ---
@app.get("/")
def dashboard(request: Request):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT * FROM "AirQualityRecord"
                ORDER BY timestamp DESC
                LIMIT 100
            """))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        avg_aqi = float(round(df['aqi'].mean(), 1))
        avg_pm25 = float(round(df['pm25'].mean(), 1))
        top_city = str(df.loc[df['aqi'].idxmax(), 'city_id'])
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "df": df.to_dict(orient="records"),
            "avg_aqi": avg_aqi,
            "avg_pm25": avg_pm25,
            "top_city": top_city
        })
    except Exception as e:
        return {"error": str(e)}

@app.get("/air-quality")
def get_latest_air_quality():
    try:
        query = 'SELECT * FROM "AirQualityRecord" ORDER BY timestamp DESC LIMIT 100'
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/kpi-summary")
def kpi_summary():
    try:
        query = 'SELECT aqi, pm25, city_id FROM "AirQualityRecord" ORDER BY timestamp DESC LIMIT 100'
        df = pd.read_sql(query, engine)
        return {
            "avg_aqi": float(round(df['aqi'].mean(), 1)),
            "avg_pm25": float(round(df['pm25'].mean(), 1)),
            "top_city_id": str(df.loc[df['aqi'].idxmax(), 'city_id'])
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/cities")
def get_cities():
    try:
        query = 'SELECT DISTINCT city_id FROM "AirQualityRecord"'
        df = pd.read_sql(query, engine)
        return df['city_id'].tolist()
    except Exception as e:
        return {"error": str(e)}

@app.get("/province-summary")
def get_province_summary():
    try:
        query = '''
            SELECT c.province, AVG(a.aqi) as avg_aqi
            FROM "AirQualityRecord" a
            JOIN "City" c ON a.city_id = c.city_id
            GROUP BY c.province
            ORDER BY avg_aqi DESC
        '''
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/time-series")
def time_series(city_id: int = Query(...)):
    try:
        query = text('''
            SELECT timestamp, aqi
            FROM "AirQualityRecord"
            WHERE city_id = :city_id
            ORDER BY timestamp ASC
        ''')
        df = pd.read_sql(query, engine, params={"city_id": city_id})
        df['timestamp'] = df['timestamp'].astype(str)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/map-data")
def get_map_data():
    try:
        query = '''
            SELECT aqi, c.city_name, c.latitude, c.longitude
            FROM "AirQualityRecord" a
            JOIN "City" c ON a.city_id = c.city_id
            ORDER BY timestamp DESC
            LIMIT 200
        '''
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/source-breakdown")
def get_source_summary():
    try:
        query = '''
            SELECT s.source_name, COUNT(*) as total
            FROM "AirQualityRecord" a
            JOIN "Source" s ON a.source_id = s.source_id
            GROUP BY s.source_name
        '''
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/table/{table_name}")
def get_entire_table(table_name: str):
    # Chỉ cho phép các bảng hợp lệ, tránh SQL injection
    allowed_tables = {"AirQualityRecord", "City", "Source"}
    if table_name not in allowed_tables:
        return {"error": "Invalid table name"}
    try:
        query = f'SELECT * FROM "{table_name}"'
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/filter")
def filter_data(
    city_id: int = Query(None),
    source_id: int = Query(None),
    start_time: str = Query(None),
    end_time: str = Query(None)
):
    try:
        filters = []
        params = {}
        if city_id:
            filters.append("city_id = :city_id")
            params["city_id"] = city_id
        if source_id:
            filters.append("source_id = :source_id")
            params["source_id"] = source_id
        if start_time and end_time:
            filters.append("timestamp BETWEEN :start_time AND :end_time")
            params["start_time"] = start_time
            params["end_time"] = end_time

        filter_clause = " AND ".join(filters)
        if filter_clause:
            query = f'SELECT * FROM "AirQualityRecord" WHERE {filter_clause} ORDER BY timestamp DESC LIMIT 200'
            df = pd.read_sql(text(query), engine, params=params)
        else:
            query = 'SELECT * FROM "AirQualityRecord" ORDER BY timestamp DESC LIMIT 100'
            df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/realtime-tab")
def realtime_tab():
    try:
        query = '''
            SELECT * FROM "AirQualityRecord"
            WHERE timestamp >= NOW() - INTERVAL '1 hour'
            ORDER BY timestamp DESC
        '''
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/calculation-tab")
def calculation_tab():
    try:
        query = '''
            SELECT city_id, date_trunc('day', timestamp) as day, AVG(aqi) as avg_aqi, MAX(aqi) as max_aqi
            FROM "AirQualityRecord"
            GROUP BY city_id, day
            ORDER BY day DESC, city_id
            LIMIT 200
        '''
        df = pd.read_sql(query, engine)
        df['day'] = df['day'].astype(str)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/latest-by-city")
def latest_by_city():
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

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "api"}

# --- Metrics route ---
@app.post("/log-execution")
async def log_execution(request: Request):
    try:
        if request.headers.get("content-type", "").startswith("application/json"):
            data = await request.json()
        else:
            data = {}
    except Exception:
        data = {}
    return {"status": "ok", "logged": data}


@app.post("/process-data")
async def process_data(request: Request):
    """
    Xử lý dữ liệu chất lượng không khí và tạo cảnh báo nếu AQI cao.
    """
    try:
        if request.headers.get("content-type", "").startswith("application/json"):
            data = await request.json()
        else:
            data = {}
    except Exception:
        data = {}

    # Lấy danh sách dữ liệu từ trường 'data'
    records = data.get("data", [])
    alert_message = "Không có cảnh báo: Chất lượng không khí tốt."
    affected_areas = []

    # Kiểm tra AQI cho từng bản ghi
    for record in records:
        aqi = record.get("aqi", 0)
        city = record.get("city", "Unknown")
        if aqi > 100:  # Ngưỡng cảnh báo: AQI > 100
            affected_areas.append(city)
    if affected_areas:
        alert_message = f"CẢNH BÁO: Chất lượng không khí kém (AQI > 100) tại {', '.join(affected_areas)}."

    return {
        "status": "ok",
        "processed_data": records,
        "metrics": {"processed_records": len(records)},
        "alert_message": alert_message,
        "affected_areas": ", ".join(affected_areas) if affected_areas else "Không có khu vực bị ảnh hưởng"
    }

@app.post("/update")
async def update_dashboard(request: Request):
    """
    Cập nhật dashboard (giả lập endpoint dashboard).
    """
    try:
        if request.headers.get("content-type", "").startswith("application/json"):
            data = await request.json()
        else:
            data = {}
    except Exception:
        data = {}
    return {"status": "ok", "received": data}
