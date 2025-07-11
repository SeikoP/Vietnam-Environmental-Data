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
app = FastAPI(title="Climate Cleaner")

def clean_climate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Làm sạch và chuẩn hóa sâu dữ liệu khí hậu."""
    logger.info("Starting data cleaning process")
    # Chuẩn hóa tên cột
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if "success" in df.columns:
        df = df[df["success"] == True]
    drop_cols = [c for c in ["error_code", "error_message"] if c in df.columns]
    df = df.drop(columns=drop_cols, errors="ignore")
    if "location" in df.columns and "timestamp" in df.columns:
        df = df.drop_duplicates(subset=["location", "timestamp"])
    required_cols = ["location", "province", "lat", "lon", "timestamp", "temperature"]
    for col in required_cols:
        if col in df.columns:
            df = df[df[col].notnull()]
    float_cols = [
        "temperature", "feels_like", "temp_min", "temp_max", "humidity", "pressure",
        "dew_point", "uvi", "rainfall", "wind_speed", "wind_deg", "wind_gust",
        "clouds", "visibility", "lat", "lon"
    ]
    # Define default values for float columns
    float_defaults = {
        "temperature": None,
        "feels_like": None,
        "temp_min": None,
        "temp_max": None,
        "humidity": None,
        "pressure": None,
        "dew_point": None,
        "uvi": 0.0,
        "rainfall": 0.0,
        "wind_speed": None,
        "wind_deg": None,
        "wind_gust": None,
        "clouds": None,
        "visibility": None,
        "lat": None,
        "lon": None
    }
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Replace inf/-inf with NaN and clip to reasonable ranges
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            if col in ["lat", "lon"]:  # Geographic coordinates
                df[col] = df[col].clip(lower=-180, upper=180)
            elif col in ["temperature", "feels_like", "temp_min", "temp_max"]:  # Temperature reasonable range
                df[col] = df[col].clip(lower=-100, upper=100)
            elif col == "humidity":  # Humidity percentage
                df[col] = df[col].clip(lower=0, upper=100)
            elif col in ["pressure", "wind_speed", "wind_gust", "visibility"]:  # Reasonable upper limit
                df[col] = df[col].clip(lower=0, upper=1e6)
            elif col in ["clouds"]:  # Clouds percentage
                df[col] = df[col].clip(lower=0, upper=100)
            elif col in ["uvi"]:  # UV index
                df[col] = df[col].clip(lower=0, upper=20)
            elif col in ["wind_deg"]:  # Wind direction in degrees
                df[col] = df[col].clip(lower=0, upper=360)
            # Fill NaN with default value
            default = float_defaults.get(col, None)
            if default is not None:
                df[col] = df[col].fillna(default)
            else:
                df[col] = df[col].where(pd.notnull(df[col]), None)
            # Ensure JSON-safe float values
            df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) and -1e308 < float(x) < 1e308 else None)
    str_cols = [
        "location", "province", "coord_string", "country", "weather_condition",
        "weather_main", "weather_icon", "source"
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", None)
            df[col] = df[col].replace({"None": None, "": None})
    # Fix datetime conversion
    for col in ["timestamp", "sunrise", "sunset", "crawl_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    # Đảm bảo luôn có cột 'timestamp'
    if "timestamp" not in df.columns:
        logger.warning("Input data missing 'timestamp' column. Adding default timestamp.")
        df["timestamp"] = datetime.now()
    df = df[df["timestamp"].notnull()]
    if "temperature" in df.columns:
        df = df[df["temperature"].notnull()]
    if "humidity" in df.columns:
        df = df[(df["humidity"] >= 0) & (df["humidity"] <= 100)]
    # Thay thế giá trị null bằng giá trị mô phỏng
    if "lat" in df.columns:
        random_lat = pd.Series(np.random.uniform(8, 23, size=len(df)), index=df.index)
        df["lat"] = df["lat"].fillna(random_lat)
    if "lon" in df.columns:
        random_lon = pd.Series(np.random.uniform(102, 110, size=len(df)), index=df.index)
        df["lon"] = df["lon"].fillna(random_lon)
    if "province" in df.columns:
        df["province"] = df["province"].fillna("Unknown Province")
    if "country" in df.columns:
        df["country"] = df["country"].fillna("VN")
    if "weather_condition" in df.columns:
        df["weather_condition"] = df["weather_condition"].fillna("Clear")
    if "weather_main" in df.columns:
        df["weather_main"] = df["weather_main"].fillna("Clear")
    if "weather_icon" in df.columns:
        df["weather_icon"] = df["weather_icon"].fillna("01d")
    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"].fillna(datetime.now())
    df = df.reset_index(drop=True)
    # Log any remaining NaN values in float columns
    for col in float_cols:
        if col in df.columns and df[col].isna().any():
            logger.warning(f"Column {col} contains {df[col].isna().sum()} NaN values after cleaning")
    logger.info("Data cleaning completed")
    return df

def transform_climate_3nf(df: pd.DataFrame) -> dict:
    """
    Chuẩn hóa dữ liệu khí hậu về dạng 3NF: Location, WeatherCondition, ClimateRecord.
    """
    logger.info("Starting 3NF transformation")
    for col in ["lat", "lon", "country", "weather_condition", "weather_main", "weather_icon"]:
        if col not in df.columns:
            df[col] = None
    # Bảng Province
    provinces = (
        df[["province", "country"]].drop_duplicates().reset_index(drop=True).assign(province_id=lambda x: x.index + 1)
        if all(c in df.columns for c in ["province", "country"])
        else pd.DataFrame(columns=["province", "country", "province_id"])
    )
    province_map = provinces.set_index("province")["province_id"].to_dict() if "province" in provinces.columns else {}
    # Bảng Country
    countries = (
        df[["country"]].drop_duplicates().reset_index(drop=True).assign(country_id=lambda x: x.index + 1)
        if "country" in df.columns
        else pd.DataFrame(columns=["country", "country_id"])
    )
    country_map = countries.set_index("country")["country_id"].to_dict() if "country" in countries.columns else {}
    # Bảng WeatherMain
    weather_mains = (
        df[["weather_main"]].drop_duplicates().reset_index(drop=True).assign(main_id=lambda x: x.index + 1)
        if "weather_main" in df.columns
        else pd.DataFrame(columns=["weather_main", "main_id"])
    )
    main_map = weather_mains.set_index("weather_main")["main_id"].to_dict() if "weather_main" in weather_mains.columns else {}
    # Bảng WeatherIcon
    weather_icons = (
        df[["weather_icon"]].drop_duplicates().reset_index(drop=True).assign(icon_id=lambda x: x.index + 1)
        if "weather_icon" in df.columns
        else pd.DataFrame(columns=["weather_icon", "icon_id"])
    )
    icon_map = weather_icons.set_index("weather_icon")["icon_id"].to_dict() if "weather_icon" in weather_icons.columns else {}
    # Location table
    locations = (
        df[["location", "province", "lat", "lon", "country"]].drop_duplicates().reset_index(drop=True).assign(location_id=lambda x: x.index + 1)
        if all(c in df.columns for c in ["location", "province", "lat", "lon", "country"])
        else pd.DataFrame(columns=["location", "province", "lat", "lon", "country", "location_id"])
    )
    loc_map = locations.set_index(["location", "province"])["location_id"].to_dict() if all(c in locations.columns for c in ["location", "province", "location_id"]) else {}
    # WeatherCondition table
    weather_conditions = (
        df[["weather_condition", "weather_main", "weather_icon"]].drop_duplicates().reset_index(drop=True).assign(condition_id=lambda x: x.index + 1)
        if all(c in df.columns for c in ["weather_condition", "weather_main", "weather_icon"])
        else pd.DataFrame(columns=["weather_condition", "weather_main", "weather_icon", "condition_id"])
    )
    cond_map = (
        weather_conditions.set_index(["weather_condition", "weather_main", "weather_icon"])["condition_id"].to_dict()
        if all(c in weather_conditions.columns for c in ["weather_condition", "weather_main", "weather_icon", "condition_id"])
        else {}
    )
    # ClimateRecord table
    climate_records = df.copy()
    if loc_map:
        climate_records["location_id"] = climate_records.apply(lambda x: loc_map.get((x.get("location"), x.get("province"))), axis=1)
    if province_map:
        climate_records["province_id"] = climate_records["province"].map(province_map) if "province" in climate_records.columns else None
    if country_map:
        climate_records["country_id"] = climate_records["country"].map(country_map) if "country" in climate_records.columns else None
    if main_map:
        climate_records["main_id"] = climate_records["weather_main"].map(main_map) if "weather_main" in climate_records.columns else None
    if icon_map:
        climate_records["icon_id"] = climate_records["weather_icon"].map(icon_map) if "weather_icon" in climate_records.columns else None
    if cond_map:
        climate_records["condition_id"] = climate_records.apply(
            lambda x: cond_map.get((x.get("weather_condition"), x.get("weather_main"), x.get("weather_icon"))), axis=1
        )
    climate_fields = [
        "timestamp", "crawl_time", "location_id", "province_id", "country_id", "main_id", "icon_id", "condition_id",
        "coord_string", "timezone", "temperature", "feels_like", "temp_min", "temp_max", "humidity", "pressure",
        "dew_point", "uvi", "rainfall", "wind_speed", "wind_deg", "wind_gust", "clouds", "visibility",
        "sunrise", "sunset", "source"
    ]
    for col in climate_fields:
        if col not in climate_records.columns:
            climate_records[col] = None
    climate_records = climate_records[climate_fields]
    logger.info("3NF transformation completed")
    return {
        "Province": provinces,
        "Country": countries,
        "WeatherMain": weather_mains,
        "WeatherIcon": weather_icons,
        "Location": locations,
        "WeatherCondition": weather_conditions,
        "ClimateRecord": climate_records
    }

@app.post("/clean_climate_data")
async def clean_climate_data(request: Request):
    """
    Nhận nội dung CSV qua body (json/csv), làm sạch, transform, lưu vào Postgres.
    Trả về dữ liệu sạch mới nhất để workflow có thể truyền sang process-data.
    """
    try:
        logger.info("Received request to /clean_climate_data")
        data = await request.json()
        csv_content = data.get("csv_content")
        if not csv_content:
            logger.error("csv_content is missing in request")
            raise HTTPException(status_code=400, detail="csv_content is required")
        df = pd.read_csv(io.StringIO(csv_content))
        logger.info(f"Loaded CSV with {len(df)} rows")
        df_clean = clean_climate_df(df)
        logger.info(f"Cleaned DataFrame with {len(df_clean)} rows")
        # --- CHUẨN HÓA 3NF ---
        tables = transform_climate_3nf(df_clean)
        # Lưu từng bảng vào Postgres
        tables["Province"].to_sql("climate_province", engine, if_exists="replace", index=False)
        tables["Country"].to_sql("climate_country", engine, if_exists="replace", index=False)
        tables["WeatherMain"].to_sql("climate_weather_main", engine, if_exists="replace", index=False)
        tables["WeatherIcon"].to_sql("climate_weather_icon", engine, if_exists="replace", index=False)
        tables["Location"].to_sql("climate_location", engine, if_exists="replace", index=False)
        tables["WeatherCondition"].to_sql("climate_weather_condition", engine, if_exists="replace", index=False)
        tables["ClimateRecord"].to_sql("climate_record", engine, if_exists="append", index=False)
        logger.info("Data saved to PostgreSQL")
        # Lưu file clean vào thư mục chuẩn
        cleaned_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_cleaner\data\data_cleaned")
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        cleaned_file = cleaned_dir / 'cleaned_climate_quality.csv'
        df_clean.to_csv(cleaned_file, index=False, encoding='utf-8-sig')
        logger.info(f"Cleaned data saved to {cleaned_file}")
        # Ensure JSON-serializable output
        df_clean = df_clean.replace([np.inf, -np.inf], np.nan)
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        # Convert datetime columns to ISO format strings for JSON compatibility
        for col in ["timestamp", "sunrise", "sunset", "crawl_time"]:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(lambda x: x.isoformat() if pd.notnull(x) and isinstance(x, (pd.Timestamp, datetime)) else None)
        # Ensure all float columns are within safe JSON range
        float_cols = [
            "temperature", "feels_like", "temp_min", "temp_max", "humidity", "pressure",
            "dew_point", "uvi", "rainfall", "wind_speed", "wind_deg", "wind_gust",
            "clouds", "visibility", "lat", "lon"
        ]
        for col in float_cols:
            if col in df_clean.columns:
                # Log problematic values before final conversion
                invalid = df_clean[col][df_clean[col].notnull() & ~df_clean[col].apply(lambda x: isinstance(x, (int, float)) and -1e308 < float(x) < 1e308)]
                if not invalid.empty:
                    logger.warning(f"Column {col} contains {len(invalid)} out-of-range values: {invalid.tolist()}")
                df_clean[col] = df_clean[col].apply(lambda x: float(x) if isinstance(x, (int, float)) and -1e308 < float(x) < 1e308 else None)
        # Final check for JSON compatibility
        for col in df_clean.columns:
            if df_clean[col].dtype in [float, int] or df_clean[col].apply(lambda x: isinstance(x, (int, float))).any():
                invalid = df_clean[col][df_clean[col].notnull() & ~df_clean[col].apply(lambda x: isinstance(x, (int, float)) and -1e308 < float(x) < 1e308 if pd.notnull(x) else True)]
                if not invalid.empty:
                    logger.error(f"Column {col} contains {len(invalid)} non-JSON compliant values: {invalid.tolist()}")
                    raise ValueError(f"Non-JSON compliant values in {col}: {invalid.tolist()}")
        logger.info("Data prepared for JSON response")
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns),
            "cleaned_file": str(cleaned_file),
            "record_count": len(df_clean)
        }
    except Exception as e:
        logger.error(f"Error in /clean_climate_data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload_climate_csv")
async def upload_climate_csv(file: UploadFile = File(...)):
    """
    Nhận file CSV upload, làm sạch, transform, lưu vào Postgres.
    """
    try:
        logger.info("Received request to /upload_climate_csv")
        df = pd.read_csv(file.file)
        logger.info(f"Loaded CSV with {len(df)} rows")
        df_clean = clean_climate_df(df)
        tables = transform_climate_3nf(df_clean)
        tables["Province"].to_sql("climate_province", engine, if_exists="replace", index=False)
        tables["Country"].to_sql("climate_country", engine, if_exists="replace", index=False)
        tables["WeatherMain"].to_sql("climate_weather_main", engine, if_exists="replace", index=False)
        tables["WeatherIcon"].to_sql("climate_weather_icon", engine, if_exists="replace", index=False)
        tables["Location"].to_sql("climate_location", engine, if_exists="replace", index=False)
        tables["WeatherCondition"].to_sql("climate_weather_condition", engine, if_exists="replace", index=False)
        tables["ClimateRecord"].to_sql("climate_record", engine, if_exists="append", index=False)
        logger.info("Data saved to PostgreSQL")
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns)
        }
    except Exception as e:
        logger.error(f"Error in /upload_climate_csv: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def main():
    """
    Quét tất cả file CSV trong thư mục raw, làm sạch, transform 3NF và đẩy dữ liệu vào Postgres.
    """
    logger.info("Starting main batch processing")
    raw_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\climate\raw")
    if not raw_dir.exists():
        logger.error(f"Directory not found: {raw_dir}")
        print(f"❌ Không tìm thấy thư mục: {raw_dir}")
        return
    csv_files = list(raw_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("No CSV files found in raw directory")
        print("❌ Không tìm thấy file CSV nào trong thư mục raw.")
        return
    total_rows = 0
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            logger.info(f"Processing file {csv_file.name} with {len(df)} rows")
            df_clean = clean_climate_df(df)
            tables = transform_climate_3nf(df_clean)
            tables["Province"].to_sql("climate_province", engine, if_exists="replace", index=False)
            tables["Country"].to_sql("climate_country", engine, if_exists="replace", index=False)
            tables["WeatherMain"].to_sql("climate_weather_main", engine, if_exists="replace", index=False)
            tables["WeatherIcon"].to_sql("climate_weather_icon", engine, if_exists="replace", index=False)
            tables["Location"].to_sql("climate_location", engine, if_exists="replace", index=False)
            tables["WeatherCondition"].to_sql("climate_weather_condition", engine, if_exists="replace", index=False)
            tables["ClimateRecord"].to_sql("climate_record", engine, if_exists="append", index=False)
            logger.info(f"Successfully loaded {len(tables['ClimateRecord'])} records from {csv_file.name}")
            print(f"✅ Đã nạp {len(tables['ClimateRecord'])} bản ghi từ {csv_file.name}")
            total_rows += len(tables['ClimateRecord'])
        except Exception as e:
            logger.error(f"Error processing file {csv_file.name}: {str(e)}")
            print(f"❌ Lỗi với file {csv_file.name}: {e}")
    logger.info(f"Total records loaded: {total_rows}")
    print(f"🎉 Tổng số bản ghi đã nạp: {total_rows}")

if __name__ == "__main__":
    main()

router = app.router