import os
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
import io
import numpy as np
from datetime import datetime
from fastapi.responses import JSONResponse

# Load env
env_path = Path(__file__).parent.parent / "configs" / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

DB_URL = os.getenv("DATABASE_URL") or \
    "postgresql+psycopg2://postgres:0946932602a@postgres:5432/air_quality_db"

engine = create_engine(DB_URL)
app = FastAPI(title="Water Cleaner")

def clean_water_df(df: pd.DataFrame) -> pd.DataFrame:
    """Làm sạch và chuẩn hóa dữ liệu nước."""
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
        "lat", "lon", "annual_rainfall_mm", "groundwater_depth", "rainfall_24h", "rainfall_7d",
        "evaporation_rate", "water_quality_index", "estimated_bacterial_risk", "estimated_pollution_risk",
        "estimated_ph_risk", "estimated_water_quality_score"
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    str_cols = [
        "location", "province", "region", "major_river", "water_availability", "water_source_type",
        "water_treatment_plants", "water_quality_monitoring", "flood_risk", "drought_risk",
        "water_stress_level", "water_quality_category", "water_abundance", "source"
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
    df = df.reset_index(drop=True)
    # Thay thế giá trị null bằng giá trị mô phỏng
    if "lat" in df.columns:
        # Sửa lỗi: fillna phải truyền giá trị scalar, dict hoặc Series, không truyền ndarray trực tiếp
        random_lat = np.random.uniform(8, 23, size=len(df))
        df["lat"] = df["lat"].fillna(pd.Series(random_lat, index=df.index))
    if "lon" in df.columns:
        random_lon = np.random.uniform(102, 110, size=len(df))
        df["lon"] = df["lon"].fillna(pd.Series(random_lon, index=df.index))
    if "province" in df.columns:
        df["province"] = df["province"].fillna("Unknown Province")
    if "region" in df.columns:
        df["region"] = df["region"].fillna("Unknown Region")
    if "major_river" in df.columns:
        df["major_river"] = df["major_river"].fillna("Unknown River")
    if "water_quality_category" in df.columns:
        df["water_quality_category"] = df["water_quality_category"].fillna("Unknown Category")
    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"].fillna(datetime.now())
    if "water_source_type" in df.columns:
        df["water_source_type"] = df["water_source_type"].fillna("Unknown Source")
    return df

def transform_water_3nf(df: pd.DataFrame) -> dict:
    """
    Chuẩn hóa dữ liệu nước về dạng 3NF: Location, WaterSourceType, WaterRecord.
    """
    for col in ["lat", "lon"]:
        if col not in df.columns:
            df[col] = None
    for col in ["water_source_type"]:
        if col not in df.columns:
            df[col] = None

    # Bảng Province
    provinces = (
        df[["province", "region"]].drop_duplicates().reset_index(drop=True).assign(province_id=lambda x: x.index + 1)
        if all(c in df.columns for c in ["province", "region"])
        else pd.DataFrame(columns=["province", "region", "province_id"])
    )
    province_map = provinces.set_index("province")["province_id"].to_dict() if "province" in provinces.columns else {}

    # Bảng Region
    regions = (
        df[["region"]].drop_duplicates().reset_index(drop=True).assign(region_id=lambda x: x.index + 1)
        if "region" in df.columns
        else pd.DataFrame(columns=["region", "region_id"])
    )
    region_map = regions.set_index("region")["region_id"].to_dict() if "region" in regions.columns else {}

    # Bảng MajorRiver
    major_rivers = (
        df[["major_river"]].drop_duplicates().reset_index(drop=True).assign(river_id=lambda x: x.index + 1)
        if "major_river" in df.columns
        else pd.DataFrame(columns=["major_river", "river_id"])
    )
    river_map = major_rivers.set_index("major_river")["river_id"].to_dict() if "major_river" in major_rivers.columns else {}

    # Bảng WaterQualityCategory
    quality_cats = (
        df[["water_quality_category"]].drop_duplicates().reset_index(drop=True).assign(category_id=lambda x: x.index + 1)
        if "water_quality_category" in df.columns
        else pd.DataFrame(columns=["water_quality_category", "category_id"])
    )
    cat_map = quality_cats.set_index("water_quality_category")["category_id"].to_dict() if "water_quality_category" in quality_cats.columns else {}

    # Location table
    locations = (
        df[["location", "province", "lat", "lon", "region"]].drop_duplicates().reset_index(drop=True).assign(location_id=lambda x: x.index + 1)
        if all(c in df.columns for c in ["location", "province", "lat", "lon", "region"])
        else pd.DataFrame(columns=["location", "province", "lat", "lon", "region", "location_id"])
    )
    loc_map = locations.set_index(["location", "province"])["location_id"].to_dict() if all(c in locations.columns for c in ["location", "province", "location_id"]) else {}

    # WaterSourceType table
    source_types = (
        df[["water_source_type"]].drop_duplicates().reset_index(drop=True).assign(source_type_id=lambda x: x.index + 1)
        if "water_source_type" in df.columns
        else pd.DataFrame(columns=["water_source_type", "source_type_id"])
    )
    source_type_map = source_types.set_index("water_source_type")["source_type_id"].to_dict() if "water_source_type" in source_types.columns else {}

    # WaterRecord table
    water_records = df.copy()
    if loc_map:
        water_records["location_id"] = water_records.apply(lambda x: loc_map.get((x.get("location"), x.get("province"))), axis=1)
    if source_type_map:
        water_records["source_type_id"] = water_records["water_source_type"].map(source_type_map) if "water_source_type" in water_records.columns else None
    if province_map:
        water_records["province_id"] = water_records["province"].map(province_map) if "province" in water_records.columns else None
    if region_map:
        water_records["region_id"] = water_records["region"].map(region_map) if "region" in water_records.columns else None
    if river_map:
        water_records["river_id"] = water_records["major_river"].map(river_map) if "major_river" in water_records.columns else None
    if cat_map:
        water_records["category_id"] = water_records["water_quality_category"].map(cat_map) if "water_quality_category" in water_records.columns else None

    water_fields = [
        "timestamp", "location_id", "province_id", "region_id", "source_type_id", "river_id", "category_id",
        "annual_rainfall_mm", "water_availability", "water_stress_level", "groundwater_depth", "water_treatment_plants",
        "water_quality_monitoring", "flood_risk", "drought_risk", "estimated_bacterial_risk",
        "estimated_pollution_risk", "estimated_ph_risk", "estimated_water_quality_score",
        "flood_risk_weather", "rainfall_24h", "rainfall_7d", "temperature_stress", "evaporation_rate",
        "water_quality_index", "water_abundance", "source"
    ]
    for col in water_fields:
        if col not in water_records.columns:
            water_records[col] = None
    water_records = water_records[water_fields]
    return {
        "Province": provinces,
        "Region": regions,
        "MajorRiver": major_rivers,
        "WaterQualityCategory": quality_cats,
        "Location": locations,
        "WaterSourceType": source_types,
        "WaterRecord": water_records
    }

@app.post("/clean_water_data")
async def clean_water_data(request: Request):
    """
    Nhận nội dung CSV qua body (json/csv), làm sạch, transform, lưu vào Postgres.
    """
    try:
        data = await request.json()
        csv_content = data.get("csv_content")
        if not csv_content:
            raise HTTPException(status_code=400, detail="csv_content is required")
        df = pd.read_csv(io.StringIO(csv_content))
        df_clean = clean_water_df(df)
        # --- CHUẨN HÓA 3NF ---
        tables = transform_water_3nf(df_clean)
        # Lưu từng bảng vào Postgres
        tables["Province"].to_sql("water_province", engine, if_exists="replace", index=False)
        tables["Region"].to_sql("water_region", engine, if_exists="replace", index=False)
        tables["MajorRiver"].to_sql("water_major_river", engine, if_exists="replace", index=False)
        tables["WaterQualityCategory"].to_sql("water_quality_category", engine, if_exists="replace", index=False)
        tables["Location"].to_sql("water_location", engine, if_exists="replace", index=False)
        tables["WaterSourceType"].to_sql("water_source_type", engine, if_exists="replace", index=False)
        tables["WaterRecord"].to_sql("water_record", engine, if_exists="append", index=False)
        # Lưu file clean vào thư mục chuẩn (nếu muốn)
        cleaned_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_cleaner\data\data_cleaned")
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        cleaned_file = cleaned_dir / 'cleaned_water_quality.csv'
        df_clean.to_csv(cleaned_file, index=False, encoding='utf-8-sig')
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload_water_csv")
async def upload_water_csv(file: UploadFile = File(...)):
    """
    Tải lên file CSV qua form-data, làm sạch, transform, lưu vào Postgres.
    """
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        df_clean = clean_water_df(df)
        # --- CHUẨN HÓA 3NF ---
        tables = transform_water_3nf(df_clean)
        # Lưu từng bảng vào Postgres
        tables["Province"].to_sql("water_province", engine, if_exists="replace", index=False)
        tables["Region"].to_sql("water_region", engine, if_exists="replace", index=False)
        tables["MajorRiver"].to_sql("water_major_river", engine, if_exists="replace", index=False)
        tables["WaterQualityCategory"].to_sql("water_quality_category", engine, if_exists="replace", index=False)
        tables["Location"].to_sql("water_location", engine, if_exists="replace", index=False)
        tables["WaterSourceType"].to_sql("water_source_type", engine, if_exists="replace", index=False)
        tables["WaterRecord"].to_sql("water_record", engine, if_exists="append", index=False)
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok", "service": "water_cleaner"}

def main():
    pass

if __name__ == "__main__":
    main()

router = app.router


