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
app = FastAPI(title="Climate Cleaner")

def clean_climate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Làm sạch và chuẩn hóa sâu dữ liệu khí hậu."""
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
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    str_cols = [
        "location", "province", "coord_string", "country", "weather_condition",
        "weather_main", "weather_icon", "source"
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", None)
            df[col] = df[col].replace({"None": None, "": None})
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "sunrise" in df.columns:
        df["sunrise"] = pd.to_datetime(df["sunrise"], errors="coerce")
    if "sunset" in df.columns:
        df["sunset"] = pd.to_datetime(df["sunset"], errors="coerce")
    if "crawl_time" in df.columns:
        df["crawl_time"] = pd.to_datetime(df["crawl_time"], errors="coerce")
    df = df[df["timestamp"].notnull()]
    if "temperature" in df.columns:
        df = df[df["temperature"].notnull()]
    for col, default in [("rainfall", 0.0), ("uvi", 0.0), ("dew_point", None)]:
        if col in df.columns:
            if default is None:
                df[col] = df[col].fillna(value=pd.NA)
            else:
                df[col] = df[col].fillna(default)
    if "temperature" in df.columns:
        df = df[(df["temperature"] > -50) & (df["temperature"] < 60)]
    if "humidity" in df.columns:
        df = df[(df["humidity"] >= 0) & (df["humidity"] <= 100)]
    # Thay thế giá trị null bằng giá trị mô phỏng
    if "lat" in df.columns:
        random_lat = np.random.uniform(8, 23, size=len(df))
        df["lat"] = df["lat"].fillna(pd.Series(random_lat, index=df.index))
    if "lon" in df.columns:
        random_lon = np.random.uniform(102, 110, size=len(df))
        df["lon"] = df["lon"].fillna(pd.Series(random_lon, index=df.index))
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
    return df

def transform_climate_3nf(df: pd.DataFrame) -> dict:
    """
    Chuẩn hóa dữ liệu khí hậu về dạng 3NF: Location, WeatherCondition, ClimateRecord.
    Tự động kiểm tra và thêm cột thiếu với giá trị None nếu không có trong file.
    """
    # Đảm bảo các cột cần thiết đều có trong DataFrame
    for col in ["lat", "lon", "country"]:
        if col not in df.columns:
            df[col] = None
    for col in ["weather_condition", "weather_main", "weather_icon"]:
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
    # Thêm các trường còn thiếu với giá trị None
    for col in climate_fields:
        if col not in climate_records.columns:
            climate_records[col] = None
    climate_records = climate_records[climate_fields]
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
    """
    try:
        data = await request.json()
        csv_content = data.get("csv_content")
        if not csv_content:
            raise HTTPException(status_code=400, detail="csv_content is required")
        df = pd.read_csv(io.StringIO(csv_content))
        df_clean = clean_climate_df(df)
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
        # Lưu file clean vào thư mục chuẩn
        cleaned_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_cleaner\data\data_cleaned")
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        cleaned_file = cleaned_dir / 'cleaned_climate_quality.csv'
        df_clean.to_csv(cleaned_file, index=False, encoding='utf-8-sig')
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload_climate_csv")
async def upload_climate_csv(file: UploadFile = File(...)):
    """
    Nhận file CSV upload, làm sạch, transform, lưu vào Postgres.
    """
    try:
        df = pd.read_csv(file.file)
        df_clean = clean_climate_df(df)
        df_clean.to_sql("climate_data", engine, if_exists="append", index=False)
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def main():
    """
    Quét tất cả file CSV trong thư mục raw, làm sạch, transform 3NF và đẩy dữ liệu vào Postgres.
    """
    raw_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\climate\raw")
    if not raw_dir.exists():
        print(f"❌ Không tìm thấy thư mục: {raw_dir}")
        return

    csv_files = list(raw_dir.glob("*.csv"))
    if not csv_files:
        print("❌ Không tìm thấy file CSV nào trong thư mục raw.")
        return

    total_rows = 0
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            df_clean = clean_climate_df(df)
            tables = transform_climate_3nf(df_clean)
            # Lưu từng bảng vào Postgres
            tables["Province"].to_sql("climate_province", engine, if_exists="replace", index=False)
            tables["Country"].to_sql("climate_country", engine, if_exists="replace", index=False)
            tables["WeatherMain"].to_sql("climate_weather_main", engine, if_exists="replace", index=False)
            tables["WeatherIcon"].to_sql("climate_weather_icon", engine, if_exists="replace", index=False)
            tables["Location"].to_sql("climate_location", engine, if_exists="replace", index=False)
            tables["WeatherCondition"].to_sql("climate_weather_condition", engine, if_exists="replace", index=False)
            tables["ClimateRecord"].to_sql("climate_record", engine, if_exists="append", index=False)
            print(f"✅ Đã nạp {len(tables['ClimateRecord'])} bản ghi từ {csv_file.name}")
            total_rows += len(tables["ClimateRecord"])
        except Exception as e:
            print(f"❌ Lỗi với file {csv_file.name}: {e}")

    print(f"🎉 Tổng số bản ghi đã nạp: {total_rows}")

if __name__ == "__main__":
    main()

router = app.router