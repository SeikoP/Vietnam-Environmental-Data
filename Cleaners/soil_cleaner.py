import os
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
import io
import numpy as np
from datetime import datetime

# Load env
env_path = Path(__file__).parent.parent / "configs" / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

DB_URL = os.getenv("DATABASE_URL") or \
    "postgresql+psycopg2://postgres:0946932602a@postgres:5432/air_quality_db"

engine = create_engine(DB_URL)
app = FastAPI(title="Soil Cleaner")

def clean_soil_df(df: pd.DataFrame) -> pd.DataFrame:
    """Làm sạch và chuẩn hóa dữ liệu soil."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if "success" in df.columns:
        df = df[df["success"] == True]
    drop_cols = [c for c in ["error_code", "error_message"] if c in df.columns]
    df = df.drop(columns=drop_cols, errors="ignore")
    if "location" in df.columns and "timestamp" in df.columns:
        df = df.drop_duplicates(subset=["location", "timestamp"])
    required_cols = ["location", "province", "lat", "lon", "timestamp"]
    for col in required_cols:
        if col in df.columns:
            df = df[df[col].notnull()]
    float_cols = [
        "lat", "lon", "soil_temperature_0cm", "soil_moisture_0_1cm", "soil_moisture_1_3cm",
        "soil_moisture_3_9cm", "soil_moisture_9_27cm", "soil_moisture_27_81cm",
        "temperature_2m_max", "temperature_2m_min", "precipitation_sum", "et0_fao_evapotranspiration"
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    str_cols = [
        "location", "province", "data_sources", "source", "data_type", "moisture_status",
        "temperature_stress", "irrigation_need", "soil_health_status"
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", None)
            df[col] = df[col].replace({"None": None, "": None})
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "crawl_time" in df.columns:
        df["crawl_time"] = pd.to_datetime(df["crawl_time"], errors="coerce")
    df = df[df["timestamp"].notnull()]
    # Thay thế giá trị null bằng giá trị mô phỏng
    if "lat" in df.columns:
        random_lat = np.random.uniform(8, 23, size=len(df))
        df["lat"] = df["lat"].fillna(pd.Series(random_lat, index=df.index))
    if "lon" in df.columns:
        random_lon = np.random.uniform(102, 110, size=len(df))
        df["lon"] = df["lon"].fillna(pd.Series(random_lon, index=df.index))
    if "province" in df.columns:
        df["province"] = df["province"].fillna("Unknown Province")
    if "moisture_status" in df.columns:
        df["moisture_status"] = df["moisture_status"].fillna("Normal")
    if "soil_health_status" in df.columns:
        df["soil_health_status"] = df["soil_health_status"].fillna("Healthy")
    if "irrigation_need" in df.columns:
        df["irrigation_need"] = df["irrigation_need"].fillna("No Need")
    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"].fillna(datetime.now())
    if "data_type" in df.columns:
        df["data_type"] = df["data_type"].fillna("Unknown Type")
    df = df.reset_index(drop=True)
    return df

def transform_soil_3nf(df: pd.DataFrame) -> dict:
    """
    Chuẩn hóa dữ liệu soil về dạng 3NF: Location, SoilType, SoilRecord.
    """
    for col in ["lat", "lon"]:
        if col not in df.columns:
            df[col] = None
    for col in ["data_type"]:
        if col not in df.columns:
            df[col] = None

    # Bảng Province
    provinces = (
        df[["province"]].drop_duplicates().reset_index(drop=True).assign(province_id=lambda x: x.index + 1)
        if "province" in df.columns
        else pd.DataFrame(columns=["province", "province_id"])
    )
    province_map = provinces.set_index("province")["province_id"].to_dict() if "province" in provinces.columns else {}

    # Bảng SoilHealthStatus
    health_statuses = (
        df[["soil_health_status"]].drop_duplicates().reset_index(drop=True).assign(health_id=lambda x: x.index + 1)
        if "soil_health_status" in df.columns
        else pd.DataFrame(columns=["soil_health_status", "health_id"])
    )
    health_map = health_statuses.set_index("soil_health_status")["health_id"].to_dict() if "soil_health_status" in health_statuses.columns else {}

    # Bảng MoistureStatus
    moisture_statuses = (
        df[["moisture_status"]].drop_duplicates().reset_index(drop=True).assign(moisture_id=lambda x: x.index + 1)
        if "moisture_status" in df.columns
        else pd.DataFrame(columns=["moisture_status", "moisture_id"])
    )
    moisture_map = moisture_statuses.set_index("moisture_status")["moisture_id"].to_dict() if "moisture_status" in moisture_statuses.columns else {}

    # Bảng IrrigationNeed
    irrigation_needs = (
        df[["irrigation_need"]].drop_duplicates().reset_index(drop=True).assign(irrigation_id=lambda x: x.index + 1)
        if "irrigation_need" in df.columns
        else pd.DataFrame(columns=["irrigation_need", "irrigation_id"])
    )
    irrigation_map = irrigation_needs.set_index("irrigation_need")["irrigation_id"].to_dict() if "irrigation_need" in irrigation_needs.columns else {}

    # Location table
    locations = (
        df[["location", "province", "lat", "lon"]].drop_duplicates().reset_index(drop=True).assign(location_id=lambda x: x.index + 1)
        if all(c in df.columns for c in ["location", "province", "lat", "lon"])
        else pd.DataFrame(columns=["location", "province", "lat", "lon", "location_id"])
    )
    loc_map = locations.set_index(["location", "province"])["location_id"].to_dict() if all(c in locations.columns for c in ["location", "province", "location_id"]) else {}

    # SoilType table
    soil_types = (
        df[["data_type"]].drop_duplicates().reset_index(drop=True).assign(soil_type_id=lambda x: x.index + 1)
        if "data_type" in df.columns
        else pd.DataFrame(columns=["data_type", "soil_type_id"])
    )
    soil_type_map = soil_types.set_index("data_type")["soil_type_id"].to_dict() if "data_type" in soil_types.columns else {}

    # SoilRecord table
    soil_records = df.copy()
    if loc_map:
        soil_records["location_id"] = soil_records.apply(lambda x: loc_map.get((x.get("location"), x.get("province"))), axis=1)
    if soil_type_map:
        soil_records["soil_type_id"] = soil_records["data_type"].map(soil_type_map) if "data_type" in soil_records.columns else None
    if province_map:
        soil_records["province_id"] = soil_records["province"].map(province_map) if "province" in soil_records.columns else None
    if health_map:
        soil_records["health_id"] = soil_records["soil_health_status"].map(health_map) if "soil_health_status" in soil_records.columns else None
    if moisture_map:
        soil_records["moisture_id"] = soil_records["moisture_status"].map(moisture_map) if "moisture_status" in soil_records.columns else None
    if irrigation_map:
        soil_records["irrigation_id"] = soil_records["irrigation_need"].map(irrigation_map) if "irrigation_need" in soil_records.columns else None

    soil_fields = [
        "timestamp", "location_id", "province_id", "soil_type_id", "health_id", "moisture_id", "irrigation_id",
        "soil_temperature_0cm", "soil_moisture_0_1cm", "soil_moisture_1_3cm", "soil_moisture_3_9cm",
        "soil_moisture_9_27cm", "soil_moisture_27_81cm", "temperature_2m_max", "temperature_2m_min",
        "precipitation_sum", "et0_fao_evapotranspiration", "temperature_stress", "source"
    ]
    for col in soil_fields:
        if col not in soil_records.columns:
            soil_records[col] = None
    soil_records = soil_records[soil_fields]
    return {
        "Province": provinces,
        "SoilHealthStatus": health_statuses,
        "MoistureStatus": moisture_statuses,
        "IrrigationNeed": irrigation_needs,
        "Location": locations,
        "SoilType": soil_types,
        "SoilRecord": soil_records
    }

@app.post("/clean_soil_data")
async def clean_soil_data(request: Request):
    """
    Nhận nội dung CSV qua body (json/csv), làm sạch, transform, lưu vào Postgres.
    """
    try:
        data = await request.json()
        csv_content = data.get("csv_content")
        if not csv_content:
            raise HTTPException(status_code=400, detail="csv_content is required")
        df = pd.read_csv(io.StringIO(csv_content))
        df_clean = clean_soil_df(df)
        # --- CHUẨN HÓA 3NF ---
        tables = transform_soil_3nf(df_clean)
        # Lưu từng bảng vào Postgres
        tables["Province"].to_sql("soil_province", engine, if_exists="replace", index=False)
        tables["SoilHealthStatus"].to_sql("soil_health_status", engine, if_exists="replace", index=False)
        tables["MoistureStatus"].to_sql("soil_moisture_status", engine, if_exists="replace", index=False)
        tables["IrrigationNeed"].to_sql("soil_irrigation_need", engine, if_exists="replace", index=False)
        tables["Location"].to_sql("soil_location", engine, if_exists="replace", index=False)
        tables["SoilType"].to_sql("soil_type", engine, if_exists="replace", index=False)
        tables["SoilRecord"].to_sql("soil_record", engine, if_exists="append", index=False)
        # Lưu file clean vào thư mục chuẩn
        cleaned_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_cleaner\data\data_cleaned")
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        cleaned_file = cleaned_dir / 'cleaned_soil_quality.csv'
        df_clean.to_csv(cleaned_file, index=False, encoding='utf-8-sig')
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def main():
    pass

if __name__ == "__main__":
    main()

router = app.router