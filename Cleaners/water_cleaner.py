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
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def simulate_water_row(row):
    """Sinh dữ liệu mô phỏng cho các trường null của một dòng."""
    import random
    lat = row.get("lat", random.uniform(8, 23))
    lon = row.get("lon", random.uniform(102, 110))
    region = row.get("region", "Unknown Region")
    province = row.get("province", "Unknown Province")
    major_river = row.get("major_river", "Unknown River")
    return {
        "lat": lat if pd.notnull(lat) else random.uniform(8, 23),
        "lon": lon if pd.notnull(lon) else random.uniform(102, 110),
        "annual_rainfall_mm": random.uniform(1500, 2500),
        "groundwater_depth": random.uniform(2, 50),
        "rainfall_24h": random.uniform(0, 80),
        "rainfall_7d": random.uniform(0, 300),
        "evaporation_rate": random.uniform(1, 10),
        "water_quality_index": random.uniform(40, 90),
        "estimated_bacterial_risk": random.uniform(0, 1),
        "estimated_pollution_risk": random.uniform(0, 1),
        "estimated_ph_risk": random.uniform(0, 1),
        "estimated_water_quality_score": random.uniform(50, 95),
        "region": region,
        "province": province,
        "major_river": major_river,
        "water_quality_category": "good",
        "water_source_type": "river_groundwater",
        "water_treatment_plants": random.randint(2, 10),
        "water_quality_monitoring": "regular",
        "flood_risk": "medium",
        "drought_risk": "low",
        "water_stress_level": "medium",
        "water_abundance": "high",
        "source": "simulated"
    }

def clean_water_df(df: pd.DataFrame) -> pd.DataFrame:
    """Làm sạch và chuẩn hóa dữ liệu nước. Nếu null quá nhiều, thay thế bằng dữ liệu mô phỏng."""
    logger.info("Starting water data cleaning process")
    # Chuẩn hóa tên cột
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
    # Define default values for float columns (similar to climate_cleaner.py)
    float_defaults = {
        "annual_rainfall_mm": 0.0,
        "groundwater_depth": None,
        "rainfall_24h": 0.0,
        "rainfall_7d": 0.0,
        "evaporation_rate": 0.0,
        "water_quality_index": None,
        "estimated_bacterial_risk": None,
        "estimated_pollution_risk": None,
        "estimated_ph_risk": None,
        "estimated_water_quality_score": None
    }
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Replace inf/-inf with NaN and clip to reasonable ranges
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            if col in ["lat", "lon"]:  # Geographic coordinates
                df[col] = df[col].clip(lower=-180, upper=180)
            elif col in ["annual_rainfall_mm", "rainfall_24h", "rainfall_7d"]:  # Rainfall in mm
                df[col] = df[col].clip(lower=0, upper=1e4)  # Max 10,000 mm
            elif col == "groundwater_depth":  # Depth in meters
                df[col] = df[col].clip(lower=0, upper=1e3)  # Max 1,000 m
            elif col == "evaporation_rate":  # Rate in mm/day
                df[col] = df[col].clip(lower=0, upper=100)  # Max 100 mm/day
            elif col == "water_quality_index":  # Index typically 0-100
                df[col] = df[col].clip(lower=0, upper=100)
            elif col in ["estimated_bacterial_risk", "estimated_pollution_risk", "estimated_ph_risk"]:  # Risk scores 0-1
                df[col] = df[col].clip(lower=0, upper=1)
            elif col == "estimated_water_quality_score":  # Score 0-100
                df[col] = df[col].clip(lower=0, upper=100)
            # Fill NaN with default value
            default = float_defaults.get(col, None)
            if default is not None:
                df[col] = df[col].fillna(default)
            else:
                df[col] = df[col].where(pd.notnull(df[col]), None)
            # Ensure JSON-safe float values
            df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) and -1e308 < float(x) < 1e308 else None)
    str_cols = [
        "location", "province", "region", "major_river", "water_availability", "water_source_type",
        "water_treatment_plants", "water_quality_monitoring", "flood_risk", "drought_risk",
        "water_stress_level", "water_quality_category", "water_abundance", "source"
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", None)
            df[col] = df[col].replace({"None": None, "": None})
    # Handle datetime columns
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "crawl_time" in df.columns:
        df["crawl_time"] = pd.to_datetime(df["crawl_time"], errors="coerce")
    df = df[df["timestamp"].notnull()]
    # Thay thế giá trị null bằng giá trị mô phỏng
    if "lat" in df.columns:
        random_lat = pd.Series(np.random.uniform(8, 23, size=len(df)), index=df.index)
        df["lat"] = df["lat"].fillna(random_lat)
    if "lon" in df.columns:
        random_lon = pd.Series(np.random.uniform(102, 110, size=len(df)), index=df.index)
        df["lon"] = df["lon"].fillna(random_lon)
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
    df = df.reset_index(drop=True)
    # Đếm tỷ lệ null trên các trường quan trọng
    important_cols = ["lat", "lon", "annual_rainfall_mm", "rainfall_24h", "estimated_water_quality_score"]
    important_cols_exist = [col for col in important_cols if col in df.columns]
    if important_cols_exist:
        null_counts = df[important_cols_exist].isnull().sum()
        null_ratio = null_counts / len(df) if len(df) > 0 else 0
        # Nếu bất kỳ trường quan trọng nào có tỷ lệ null > 0.3 (30%), thay thế bằng dữ liệu mô phỏng
        if (null_ratio > 0.3).any():
            logger.warning(f"Too many nulls detected in important columns: {null_counts.to_dict()}. Replacing with simulated data.")
            for idx, row in df.iterrows():
                for col in important_cols_exist:
                    if pd.isnull(row[col]):
                        sim = simulate_water_row(row)
                        df.at[idx, col] = sim[col]
                # Có thể bổ sung các trường khác nếu cần
                for col in ["water_quality_category", "water_source_type", "region", "province", "major_river"]:
                    if col in df.columns and pd.isnull(row[col]):
                        sim = simulate_water_row(row)
                        df.at[idx, col] = sim[col]
    # Log any remaining NaN values in float columns
    for col in float_cols:
        if col in df.columns and df[col].isna().any():
            logger.warning(f"Column {col} contains {df[col].isna().sum()} NaN values after cleaning")
    logger.info("Water data cleaning completed")
    return df

def transform_water_3nf(df: pd.DataFrame) -> dict:
    """
    Chuẩn hóa dữ liệu nước về dạng 3NF: Location, WaterSourceType, WaterRecord.
    """
    logger.info("Starting 3NF transformation for water data")
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
    logger.info("3NF transformation for water data completed")
    return {
        "Province": provinces,
        "Region": regions,
        "MajorRiver": major_rivers,
        "WaterQualityCategory": quality_cats,
        "Location": locations,
        "WaterSourceType": source_types,
        "WaterRecord": water_records
    }

def fill_simulated_for_nan(df):
    """Thay thế tất cả giá trị NaN của các trường float bằng giá trị mô phỏng hợp lý."""
    import random
    float_cols = [
        "lat", "lon", "annual_rainfall_mm", "groundwater_depth", "rainfall_24h", "rainfall_7d",
        "evaporation_rate", "water_quality_index", "estimated_bacterial_risk", "estimated_pollution_risk",
        "estimated_ph_risk", "estimated_water_quality_score"
    ]
    for col in float_cols:
        if col in df.columns:
            nan_idx = df[df[col].isna()].index
            if len(nan_idx) > 0:
                # Sinh giá trị mô phỏng cho từng dòng
                for idx in nan_idx:
                    if col == "lat":
                        df.at[idx, col] = random.uniform(8, 23)
                    elif col == "lon":
                        df.at[idx, col] = random.uniform(102, 110)
                    elif col == "annual_rainfall_mm":
                        df.at[idx, col] = random.uniform(1500, 2500)
                    elif col == "groundwater_depth":
                        df.at[idx, col] = random.uniform(2, 50)
                    elif col == "rainfall_24h":
                        df.at[idx, col] = random.uniform(0, 80)
                    elif col == "rainfall_7d":
                        df.at[idx, col] = random.uniform(0, 300)
                    elif col == "evaporation_rate":
                        df.at[idx, col] = random.uniform(1, 10)
                    elif col == "water_quality_index":
                        df.at[idx, col] = random.uniform(40, 90)
                    elif col == "estimated_bacterial_risk":
                        df.at[idx, col] = random.uniform(0, 1)
                    elif col == "estimated_pollution_risk":
                        df.at[idx, col] = random.uniform(0, 1)
                    elif col == "estimated_ph_risk":
                        df.at[idx, col] = random.uniform(0, 1)
                    elif col == "estimated_water_quality_score":
                        df.at[idx, col] = random.uniform(50, 95)
    return df

@app.post("/clean_water_data")
async def clean_water_data(request: Request):
    """
    Nhận nội dung CSV qua body (json/csv), làm sạch, transform, lưu vào Postgres.
    Trả về dữ liệu sạch mới nhất để workflow có thể truyền sang process-data.
    """
    try:
        logger.info("Received request to /clean_water_data")
        data = await request.json()
        csv_content = data.get("csv_content")
        if not csv_content:
            logger.error("csv_content is missing in request")
            raise HTTPException(status_code=400, detail="csv_content is required")
        df = pd.read_csv(io.StringIO(csv_content))
        logger.info(f"Loaded CSV with {len(df)} rows")
        df_clean = clean_water_df(df)
        logger.info(f"Cleaned DataFrame with {len(df_clean)} rows")
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
        logger.info("Data saved to PostgreSQL")
        # Lưu file clean vào thư mục chuẩn
        cleaned_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_cleaner\data\data_cleaned")
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        cleaned_file = cleaned_dir / 'cleaned_water_quality.csv'
        df_clean.to_csv(cleaned_file, index=False, encoding='utf-8-sig')
        logger.info(f"Cleaned data saved to {cleaned_file}")
        # Ensure JSON-serializable output
        df_clean = df_clean.replace([np.inf, -np.inf], np.nan)
        df_clean = fill_simulated_for_nan(df_clean)
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        # Convert datetime columns to ISO format strings for JSON compatibility
        for col in ["timestamp", "crawl_time"]:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(lambda x: x.isoformat() if pd.notnull(x) and isinstance(x, (pd.Timestamp, datetime)) else None)
        # Ensure all float columns are within safe JSON range
        float_cols = [
            "lat", "lon", "annual_rainfall_mm", "groundwater_depth", "rainfall_24h", "rainfall_7d",
            "evaporation_rate", "water_quality_index", "estimated_bacterial_risk", "estimated_pollution_risk",
            "estimated_ph_risk", "estimated_water_quality_score"
        ]
        for col in float_cols:
            if col in df_clean.columns:
                # Chuyển mọi giá trị không phải số hoặc ngoài khoảng thành None
                df_clean[col] = df_clean[col].apply(
                    lambda x: float(x) if isinstance(x, (int, float)) and pd.notnull(x) and -1e308 < float(x) < 1e308 else None
                )
        # Final check for JSON compatibility (chỉ log, không raise lỗi)
        for col in df_clean.columns:
            if col in float_cols:
                # Chuyển mọi giá trị không phải số hoặc ngoài khoảng thành None (lặp lại để đảm bảo)
                df_clean[col] = df_clean[col].apply(
                    lambda x: float(x) if isinstance(x, (int, float)) and pd.notnull(x) and -1e308 < float(x) < 1e308 else None
                )
                invalid = df_clean[col][df_clean[col].notnull() & ~df_clean[col].apply(lambda x: isinstance(x, (int, float)) and -1e308 < float(x) < 1e308)]
                if not invalid.empty:
                    logger.error(f"Column {col} contains {len(invalid)} non-JSON compliant values: {invalid.tolist()}")
                    df_clean.loc[invalid.index, col] = None
        logger.info("Data prepared for JSON response")
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns),
            "cleaned_file": str(cleaned_file),
            "record_count": len(df_clean)
        }
    except Exception as e:
        logger.error(f"Error in /clean_water_data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload_water_csv")
async def upload_water_csv(file: UploadFile = File(...)):
    """
    Tải lên file CSV qua form-data, làm sạch, transform, lưu vào Postgres.
    """
    try:
        logger.info("Received request to /upload_water_csv")
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        logger.info(f"Loaded CSV with {len(df)} rows")
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
        logger.info("Data saved to PostgreSQL")
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns)
        }
    except Exception as e:
        logger.error(f"Error in /upload_water_csv: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    logger.info("Health check endpoint called")
    return {"status": "ok", "service": "water_cleaner"}

def main():
    logger.info("Main function called (currently a placeholder)")
    pass

if __name__ == "__main__":
    main()

router = app.router