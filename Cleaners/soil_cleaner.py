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

    # Location table
    locations = (
        df[["location", "province", "lat", "lon"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(location_id=lambda x: x.index + 1)
    )
    # SoilType table
    soil_types = (
        df[["data_type"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(soil_type_id=lambda x: x.index + 1)
    )
    loc_map = locations.set_index(["location", "province"])["location_id"].to_dict()
    soil_type_map = soil_types.set_index("data_type")["soil_type_id"].to_dict()
    # SoilRecord table
    soil_records = df.copy()
    soil_records["location_id"] = soil_records.apply(lambda x: loc_map.get((x["location"], x["province"])), axis=1)
    soil_records["soil_type_id"] = soil_records["data_type"].map(soil_type_map)
    soil_fields = [
        "timestamp", "location_id", "soil_type_id", "soil_temperature_0cm", "soil_moisture_0_1cm",
        "soil_moisture_1_3cm", "soil_moisture_3_9cm", "soil_moisture_9_27cm", "soil_moisture_27_81cm",
        "temperature_2m_max", "temperature_2m_min", "precipitation_sum", "et0_fao_evapotranspiration",
        "moisture_status", "temperature_stress", "irrigation_need", "soil_health_status", "source"
    ]
    for col in soil_fields:
        if col not in soil_records.columns:
            soil_records[col] = None
    soil_records = soil_records[soil_fields]
    return {
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
        # Lưu vào bảng soil_data
        df_clean.to_sql("soil_data", engine, if_exists="append", index=False)
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload_soil_csv")
async def upload_soil_csv(file: UploadFile = File(...)):
    """
    Nhận file CSV upload, làm sạch, transform, lưu vào Postgres.
    """
    try:
        df = pd.read_csv(file.file)
        df_clean = clean_soil_df(df)
        df_clean.to_sql("soil_data", engine, if_exists="append", index=False)
        return {
            "success": True,
            "total_rows": len(df_clean),
            "columns": list(df_clean.columns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok", "service": "soil_cleaner"}

def main():
    """
    Quét tất cả file CSV trong thư mục raw, làm sạch, transform 3NF và đẩy dữ liệu vào Postgres.
    """
    raw_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\soil\raw")
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
            df_clean = clean_soil_df(df)
            tables = transform_soil_3nf(df_clean)
            # Lưu từng bảng vào Postgres
            tables["Location"].to_sql("soil_location", engine, if_exists="replace", index=False)
            tables["SoilType"].to_sql("soil_type", engine, if_exists="replace", index=False)
            tables["SoilRecord"].to_sql("soil_record", engine, if_exists="append", index=False)
            print(f"✅ Đã nạp {len(tables['SoilRecord'])} bản ghi từ {csv_file.name}")
            total_rows += len(tables["SoilRecord"])
        except Exception as e:
            print(f"❌ Lỗi với file {csv_file.name}: {e}")

    print(f"🎉 Tổng số bản ghi đã nạp: {total_rows}")

if __name__ == "__main__":
    main()

router = app.router