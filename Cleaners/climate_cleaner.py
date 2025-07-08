import os
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
import io

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

    # Location table
    locations = (
        df[["location", "province", "lat", "lon", "country"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(location_id=lambda x: x.index + 1)
    )
    # WeatherCondition table
    weather_conditions = (
        df[["weather_condition", "weather_main", "weather_icon"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(condition_id=lambda x: x.index + 1)
    )
    # Mapping for foreign keys
    loc_map = locations.set_index(["location", "province"])["location_id"].to_dict()
    cond_map = weather_conditions.set_index(["weather_condition", "weather_main", "weather_icon"])["condition_id"].to_dict()
    # ClimateRecord table
    climate_records = df.copy()
    climate_records["location_id"] = climate_records.apply(lambda x: loc_map.get((x["location"], x["province"])), axis=1)
    climate_records["condition_id"] = climate_records.apply(
        lambda x: cond_map.get((x.get("weather_condition"), x.get("weather_main"), x.get("weather_icon"))), axis=1
    )
    # Chọn các trường cần thiết cho ClimateRecord
    climate_fields = [
        "timestamp", "crawl_time", "location_id", "condition_id", "coord_string", "timezone",
        "temperature", "feels_like", "temp_min", "temp_max", "humidity", "pressure", "dew_point",
        "uvi", "rainfall", "wind_speed", "wind_deg", "wind_gust", "clouds", "visibility",
        "sunrise", "sunset", "source"
    ]
    # Thêm các trường còn thiếu với giá trị None
    for col in climate_fields:
        if col not in climate_records.columns:
            climate_records[col] = None
    climate_records = climate_records[climate_fields]
    return {
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
        # Lưu vào bảng climate_data
        df_clean.to_sql("climate_data", engine, if_exists="append", index=False)
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

@app.get("/health")
def health():
    return {"status": "ok", "service": "climate_cleaner"}

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