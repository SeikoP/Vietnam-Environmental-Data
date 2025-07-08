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

    # Location table
    locations = (
        df[["location", "province", "lat", "lon", "region"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(location_id=lambda x: x.index + 1)
    )
    # WaterSourceType table
    source_types = (
        df[["water_source_type"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(source_type_id=lambda x: x.index + 1)
    )
    loc_map = locations.set_index(["location", "province"])["location_id"].to_dict()
    source_type_map = source_types.set_index("water_source_type")["source_type_id"].to_dict()
    # WaterRecord table
    water_records = df.copy()
    water_records["location_id"] = water_records.apply(lambda x: loc_map.get((x["location"], x["province"])), axis=1)
    water_records["source_type_id"] = water_records["water_source_type"].map(source_type_map)
    water_fields = [
        "timestamp", "location_id", "source_type_id", "major_river", "annual_rainfall_mm",
        "water_availability", "water_stress_level", "groundwater_depth", "water_treatment_plants",
        "water_quality_monitoring", "flood_risk", "drought_risk", "estimated_bacterial_risk",
        "estimated_pollution_risk", "estimated_ph_risk", "estimated_water_quality_score",
        "water_quality_category", "flood_risk_weather", "rainfall_24h", "rainfall_7d",
        "temperature_stress", "evaporation_rate", "water_quality_index", "water_abundance", "source"
    ]
    for col in water_fields:
        if col not in water_records.columns:
            water_records[col] = None
    water_records = water_records[water_fields]
    return {
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
        # Lưu vào bảng water_data
        df_clean.to_sql("water_data", engine, if_exists="append", index=False)
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
    Nhận file CSV upload, làm sạch, transform, lưu vào Postgres.
    """
    try:
        df = pd.read_csv(file.file)
        df_clean = clean_water_df(df)
        df_clean.to_sql("water_data", engine, if_exists="append", index=False)
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
    """
    Quét tất cả file CSV trong thư mục raw, làm sạch, transform 3NF và đẩy dữ liệu vào Postgres.
    """
    raw_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\water\raw")
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
            df_clean = clean_water_df(df)
            tables = transform_water_3nf(df_clean)
            # Lưu từng bảng vào Postgres
            tables["Location"].to_sql("water_location", engine, if_exists="replace", index=False)
            tables["WaterSourceType"].to_sql("water_source_type", engine, if_exists="replace", index=False)
            tables["WaterRecord"].to_sql("water_record", engine, if_exists="append", index=False)
            print(f"✅ Đã nạp {len(tables['WaterRecord'])} bản ghi từ {csv_file.name}")
            total_rows += len(tables["WaterRecord"])
        except Exception as e:
            print(f"❌ Lỗi với file {csv_file.name}: {e}")

    print(f"🎉 Tổng số bản ghi đã nạp: {total_rows}")

if __name__ == "__main__":
    main()

router = app.router