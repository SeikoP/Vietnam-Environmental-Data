import pandas as pd
import numpy as np
from pathlib import Path
import io
import logging
import datetime
import os
from sqlalchemy import create_engine
import dotenv
from typing import Dict
from time import sleep
from fastapi import FastAPI, Request

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('converted.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
dotenv.load_dotenv()

# --- MOVE CONFIGURATION & DIR SETUP HERE ---
DOCKER_DATA_DIR = Path("/app/data")
if DOCKER_DATA_DIR.exists():
    BASE_DIR = Path("/app")
    CRAWL_DATA_DIR = DOCKER_DATA_DIR / 'data_export'
    CLEANER_DIR = DOCKER_DATA_DIR
else:
    BASE_DIR = Path(r'D:\Project_Dp-15')
    # Sửa lại đường dẫn local cho đúng
    CRAWL_DATA_DIR = Path(r"D:\Project_Dp-15\Air_Quality\data_crawler\data_export")
    CLEANER_DIR = Path(r"D:\Project_Dp-15\Air_Quality\data_cleaner\data")

CLEANED_DIR = CLEANER_DIR / 'data_cleaned'
TRANFORM_DIR = CLEANER_DIR / 'data_tranform'

# Tạo thư mục nếu chưa có
for folder in [CLEANED_DIR,TRANFORM_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

TOKEN_FILE = BASE_DIR / 'clean' / 'token_drive.pkl'
CREDENTIALS_FILE = 'credentials_oauth.json'
DATABASE_URL = (
    os.getenv('DATABASE_URL')
    or os.getenv('POSTGRES_URL')
    # fallback: only use a valid default, not a template!
    or 'postgresql+psycopg2://postgres:0946932602a@postgres:5432/air_quality_db'
)

# Initialize SQLAlchemy engine
engine = create_engine(DATABASE_URL)
# --- END CONFIGURATION & DIR SETUP ---

app = FastAPI()

@app.post("/air_cleaner")
async def air_cleaner_api(request: Request):
    """
    Nhận body JSON hợp lệ từ n8n hoặc client khác, trả về lỗi nếu không phải JSON.
    """
    try:
        if request.headers.get("content-type", "").startswith("application/json"):
            data = await request.json()
        else:
            data = {}
    except Exception as ex:
        logger.error(f"Failed to parse JSON body: {ex}")
        return {"status": "error", "message": f"Failed to parse JSON body: {ex}"}

    csv_file = data.get("csv_file")
    csv_content = data.get("csv_content")
    
    try:
        source_folder = CLEANER_DIR  # Ensure always defined
        if csv_content:
            df = pd.read_csv(io.StringIO(csv_content))
            source_folder = CLEANER_DIR
        elif csv_file:
            file_path = Path(csv_file)
            # Nếu truyền đường dẫn tương đối, hiểu là file trong CRAWL_DATA_DIR
            if not file_path.is_absolute():
                file_path = CRAWL_DATA_DIR / file_path
            if file_path.exists() and file_path.is_file():
                df = pd.read_csv(file_path)
                # Lưu file clean vào cùng tháng với file gốc
                try:
                    month_folder = file_path.parent.name
                    source_folder = CLEANER_DIR / month_folder
                    source_folder.mkdir(parents=True, exist_ok=True)
                except Exception as ex:
                    logger.warning(f"Could not create month folder: {ex}")
                    source_folder = CLEANER_DIR
            else:
                logger.error("CSV file not found or outside allowed directory")
                return {"status": "error", "message": "CSV file not found or outside allowed directory"}
        else:
            logger.error("No valid CSV data provided")
            return {"status": "error", "message": "No valid CSV data provided"}
    except Exception as ex:
        logger.error(f"Error loading CSV: {ex}", exc_info=True)
        return {"status": "error", "message": f"Error loading CSV: {ex}"}
    
    try:
        df = clean_data(df)
        mapping = transform_data(df)
    except Exception as ex:
        logger.error(f"Error cleaning/transforming data: {ex}", exc_info=True)
        return {"status": "error", "message": f"Error cleaning/transforming data: {ex}"}

    # Lưu file clean vào đúng thư mục
    try:
        cleaned_file = source_folder / 'cleaned_air_quality.csv'
        df.to_csv(cleaned_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved cleaned data to {cleaned_file}")
    except Exception as ex:
        logger.error(f"Error saving cleaned file: {ex}", exc_info=True)
        return {"status": "error", "message": f"Error saving cleaned file: {ex}"}

    # Lưu các bảng transform vào DATA_TRANFORM_DIR
    try:
        for table_name, table_data in mapping.items():
            table_path = TRANFORM_DIR / f"{table_name}.csv"
            table_data.to_csv(table_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved {table_name} to {table_path}")
    except Exception as ex:
        logger.error(f"Error saving transformed tables: {ex}", exc_info=True)
        return {"status": "error", "message": f"Error saving transformed tables: {ex}"}

    # Đẩy dữ liệu vào PostgreSQL bằng Python, đúng thứ tự khóa ngoại
    try:
        logger.info(f"City records: {len(mapping['City'])}")
        logger.info(f"Source records: {len(mapping['Source'])}")
        logger.info(f"WeatherCondition records: {len(mapping['WeatherCondition'])}")
        logger.info(f"AirQualityRecord records: {len(mapping['AirQualityRecord'])}")

        logger.info(f"DATABASE_URL: {DATABASE_URL}")
        logger.info(f"Engine: {engine}")

        try:
            with engine.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("SELECT 1"))
            logger.info("Database connection test: SUCCESS")
        except Exception as db_test_err:
            logger.error(f"Database connection test: FAILED - {db_test_err}", exc_info=True)
            return {
                "status": "error",
                "message": f"Database connection failed: {db_test_err}"
            }

        save_to_postgres(mapping['City'], 'City', engine, if_exists='replace')
        save_to_postgres(mapping['Source'], 'Source', engine, if_exists='replace')
        save_to_postgres(mapping['WeatherCondition'], 'WeatherCondition', engine, if_exists='replace')
        save_to_postgres(mapping['AirQualityRecord'], 'AirQualityRecord', engine, if_exists='append')
        logger.info("Inserted all tables to PostgreSQL successfully")
    except Exception as e:
        logger.error(f"Error inserting to PostgreSQL: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Error inserting to PostgreSQL: {e}"
        }

    # --- FIX: Convert NaN/inf to None for JSON ---
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.where(pd.notnull(df), None)

    # Trả về dữ liệu sạch mới nhất để workflow có thể truyền sang process-data
    return {
        "status": "success",
        "message": f"Processed {len(df)} records and inserted to PostgreSQL",
        "cleaned_file": str(cleaned_file),
        "data": df.to_dict(orient="records"),
        "record_count": len(df)
    }

# Configuration
DOCKER_DATA_DIR = Path("/app/data")
if DOCKER_DATA_DIR.exists():
    BASE_DIR = Path("/app")
    CRAWL_DATA_DIR = DOCKER_DATA_DIR / 'data_export'
    CLEANER_DIR = DOCKER_DATA_DIR
else:
    BASE_DIR = Path(r'D:\Project_Dp-15')
    # Sửa lại đường dẫn local cho đúng
    CRAWL_DATA_DIR = Path(r"D:\Project_Dp-15\Air_Quality\data_crawler\data_export")
    CLEANER_DIR = Path(r"D:\Project_Dp-15\Air_Quality\data_cleaner\data")

CLEANED_DIR = CLEANER_DIR / 'data_cleaned'
TRANFORM_DIR = CLEANER_DIR / 'data_tranform'

# Tạo thư mục nếu chưa có
for folder in [CLEANED_DIR,TRANFORM_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

TOKEN_FILE = BASE_DIR / 'clean' / 'token_drive.pkl'
CREDENTIALS_FILE = 'credentials_oauth.json'
DATABASE_URL = (
    os.getenv('DATABASE_URL')
    or os.getenv('POSTGRES_URL')
    # fallback: only use a valid default, not a template!
    or 'postgresql+psycopg2://postgres:0946932602a@postgres:5432/air_quality_db'
)

# Initialize SQLAlchemy engine
engine = create_engine(DATABASE_URL)

def setup_directories():
    """Create data directory if it doesn't exist."""
    global DATA_DIR 
    DATA_DIR = CLEANED_DIR
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_data() -> pd.DataFrame:
    """Load và nối tất cả các file CSV crawl được trong data_export."""
    try:
        csv_files = list(CRAWL_DATA_DIR.glob('*.csv'))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {CRAWL_DATA_DIR}")
        df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
        logger.info(f"Loaded {len(df)} records from {len(csv_files)} files in {CRAWL_DATA_DIR}")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def save_to_csv(df: pd.DataFrame, mapping: Dict[str, pd.DataFrame]):
    """Save cleaned DataFrame and transformed tables to CSV in the data directory."""
    try:
        # Save cleaned data
        output_path = DATA_DIR / 'cleaned_air_quality.csv'
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved cleaned data to {output_path}")

        # Save transformed tables
        for table_name, table_data in mapping.items():
            file_path = DATA_DIR / f"{table_name}.csv"
            table_data.to_csv(file_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved {table_name} to {file_path}")
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        raise

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the DataFrame."""
    try:
        # Define columns
        numeric_cols = ['aqi', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'nh3', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction', 'visibility']
        categorical_cols = ['city', 'province', 'city_source', 'source', 'status', 'weather_condition']

        # Lọc các cột thực sự có trong DataFrame
        numeric_cols_exist = [col for col in numeric_cols if col in df.columns]
        categorical_cols_exist = [col for col in categorical_cols if col in df.columns]

        # Fill missing values for numeric columns: dùng median thay vì mean để tránh bị kéo xuống bởi outlier thấp
        df.fillna({col: df[col].median() for col in numeric_cols_exist}, inplace=True)
        df.fillna({col: 'unknown' for col in categorical_cols_exist}, inplace=True)

        # Xử lý giá trị bất thường thấp cho các chất khí (bao gồm cả trường hợp giá trị = 0 hoặc rất nhỏ)
        min_thresholds = {
            'pm25': 5,
            'pm10': 5,
            'o3': 3,
            'no2': 3,
            'so2': 3,
            'co': 0.2,
            'nh3': 0.2,
        }
        for col, min_val in min_thresholds.items():
            if col in df.columns:
                # Nếu dữ liệu nhỏ hơn hoặc bằng ngưỡng (<=), sẽ bị thay thế bằng giá trị ngưỡng tối thiểu
                df[col] = df[col].apply(lambda x: min_val if pd.notnull(x) and x <= min_val else x)

        # Xử lý giá trị bất thường cho AQI
        # AQI ngoài trời hợp lý thường từ 10 đến 500, loại bỏ giá trị < 10 và > 500
        min_aqi = 10
        max_aqi = 500
        if 'aqi' in df.columns:
            df['aqi'] = df['aqi'].apply(lambda x: np.nan if pd.notnull(x) and (x < min_aqi or x > max_aqi) else x)
            median_aqi = df['aqi'].median()
            df['aqi'] = df['aqi'].fillna(median_aqi)

        # Handle numeric columns: remove negative values and cap outliers
        for col in numeric_cols_exist:
            df[col] = df[col].clip(lower=0, upper=df[col].quantile(0.999)).round(2)

        # Process timestamp
        import pytz
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df['timestamp'] = df['timestamp'].dt.tz_localize('Asia/Ho_Chi_Minh', ambiguous='NaT', nonexistent='NaT')
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Normalize weather condition
        weather_mapping = {
            'clouds': 'cloudy', 'clear': 'clear', 'rain': 'rain',
            'snow': 'snow', 'mist': 'mist', 'fog': 'fog'
        }
        if 'weather_condition' in df.columns:
            df['weather_condition'] = (
                df['weather_condition'].str.lower().str.strip()
                .replace('', np.nan).fillna('unknown')
                .replace(weather_mapping)
            )

        # Normalize categorical columns
        for col in categorical_cols_exist:
            df[col] = df[col].str.lower().str.strip()

        # Remove duplicates
        initial_rows = df.shape[0]
        df = df.drop_duplicates()
        logger.info(f"Removed {initial_rows - df.shape[0]} duplicate rows")

        # Validate coordinates
        if 'longitude' in df.columns:
            df['longitude'] = df['longitude'].apply(lambda x: x if -180 <= x <= 180 else np.nan)
            df['longitude'] = df.groupby('city')['longitude'].transform(lambda x: x.fillna(x.mean()))
        if 'latitude' in df.columns:
            df['latitude'] = df['latitude'].apply(lambda x: x if -90 <= x <= 90 else np.nan)
            df['latitude'] = df.groupby('city')['latitude'].transform(lambda x: x.fillna(x.mean()))

        # Drop unnecessary columns
        df = df.drop(columns=[col for col in ['uv_index', 'aqi_cn'] if col in df.columns], errors='ignore')

        return df
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        raise

def transform_data(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Transform data into normalized tables."""
    try:
        # Create cities table
        cities = (
            df[['city', 'province', 'latitude', 'longitude']]
            .drop_duplicates()
            .reset_index(drop=True)
            .assign(city_id=lambda x: x.index + 1)
            [['city_id', 'city', 'province', 'latitude', 'longitude']]
            .rename(columns={'city': 'city_name'})
        )

        # Create sources table
        sources = (
            df[['source']]
            .drop_duplicates()
            .reset_index(drop=True)
            .assign(source_id=lambda x: x.index + 1)
            [['source_id', 'source']]
            .rename(columns={'source': 'source_name'})
        )

        # Create weather conditions table
        conditions = (
            df[['weather_condition']]
            .drop_duplicates()
            .reset_index(drop=True)
            .assign(condition_id=lambda x: x.index + 1)
            [['condition_id', 'weather_condition']]
            .rename(columns={'weather_condition': 'condition_name'})
        )

        # Create mappings
        city_map = cities.set_index(['city_name', 'province'])['city_id'].to_dict()
        source_map = sources.set_index('source_name')['source_id'].to_dict()
        condition_map = conditions.set_index('condition_name')['condition_id'].to_dict()

        # Map IDs to main DataFrame
        df['city_id'] = df.apply(lambda x: city_map.get((x['city'], x['province'])), axis=1)
        df['source_id'] = df['source'].map(source_map)
        df['condition_id'] = df['weather_condition'].map(condition_map)

        # Create air quality table
        air_quality = (
            df[[
                'timestamp', 'city_id', 'source_id', 'condition_id', 'aqi', 'pm25', 'pm10', 'o3',
                'no2', 'so2', 'co', 'nh3', 'temperature', 'humidity', 'pressure', 'wind_speed',
                'wind_direction', 'visibility', 'status'
            ]]
            .reset_index(drop=True)
            .assign(record_id=lambda x: x.index + 1)
        )

        return {
            'AirQualityRecord': air_quality,
            'City': cities,
            'Source': sources,
            'WeatherCondition': conditions
        }
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def save_to_postgres(df: pd.DataFrame, table_name: str, engine, if_exists: str = 'append', chunksize: int = 1000):
    """Save DataFrame to PostgreSQL table."""
    try:
        # Nếu là bảng AirQualityRecord và if_exists='replace', drop CASCADE view phụ thuộc trước khi to_sql
        if table_name == 'AirQualityRecord' and if_exists == 'replace':
            with engine.connect() as conn:
                from sqlalchemy import text
                # Xoá view phụ thuộc trước (nếu có) bằng CASCADE
                try:
                    conn.execute(text('DROP VIEW IF EXISTS airqualityrecord CASCADE;'))
                except Exception as e:
                    logger.warning(f"Could not drop dependent view airqualityrecord: {e}")
                # Xoá bảng chính bằng CASCADE để chắc chắn không còn phụ thuộc
                try:
                    conn.execute(text('DROP TABLE IF EXISTS "AirQualityRecord" CASCADE;'))
                except Exception as e:
                    logger.warning(f"Could not drop table AirQualityRecord with CASCADE: {e}")
        # Đảm bảo timestamp là kiểu datetime khi lưu vào DB
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, chunksize=chunksize, method='multi')
        logger.info(f"Saved {len(df)} records to PostgreSQL table {table_name}")
    except Exception as e:
        logger.error(f"Error saving to PostgreSQL table {table_name}: {e}")
        raise

@app.get("/air-quality")
def get_air_quality():
    """FastAPI endpoint to retrieve recent air quality records."""
    try:
        query = 'SELECT * FROM "AirQualityRecord" ORDER BY timestamp DESC LIMIT 100'
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error retrieving air quality data: {e}")
        return {"error": str(e)}

@app.get("/health")
def health_check():
    """Health check endpoint cho service data_cleaner."""
    return {"status": "ok", "service": "data_cleaner"}

def main():
    """
    Quét tất cả file CSV trong thư mục data_storage/air/raw, làm sạch, transform, và đẩy lên Postgres.
    """
    data_export_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\air\raw")
    csv_files = list(data_export_dir.glob("*.csv"))
    if not csv_files:
        logger.error(f"Không tìm thấy file CSV nào trong {data_export_dir}")
        return
    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
    logger.info(f"Loaded {len(df)} records from {len(csv_files)} files in {data_export_dir}")
    df = clean_data(df)
    mapping = transform_data(df)

    # Lưu file cleaned và các bảng transform ra CSV (backup)
    CLEANED_DIR = Path(r"D:\Project_Dp-15\Air_Quality\data_cleaner\data\data_cleaned")
    TRANFORM_DIR = Path(r"D:\Project_Dp-15\Air_Quality\data_cleaner\data\data_tranform")
    CLEANED_DIR.mkdir(parents=True, exist_ok=True)
    TRANFORM_DIR.mkdir(parents=True, exist_ok=True)
    cleaned_file = CLEANED_DIR / 'cleaned_air_quality.csv'
    df.to_csv(cleaned_file, index=False, encoding='utf-8-sig')
    logger.info(f"Saved cleaned data to {cleaned_file}")
    for table_name, table_data in mapping.items():
        table_path = TRANFORM_DIR / f"{table_name}.csv"
        table_data.to_csv(table_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved {table_name} to {table_path}")

    # Kiểm tra kết nối DB trước khi ghi vào DB
    try:
        with engine.connect() as conn:
            from sqlalchemy import text
            conn.execute(text("SELECT 1"))
        logger.info("Database connection test: SUCCESS")
    except Exception as db_test_err:
        logger.error(f"Database connection test: FAILED - {db_test_err}")
        return

    # Ghi lần lượt các bảng
    for table in ['City', 'Source', 'WeatherCondition', 'AirQualityRecord']:
        if table in mapping:
            try:
                # Nếu là bảng AirQualityRecord và if_exists='replace', dùng DROP CASCADE để tránh lỗi view phụ thuộc
                if table == 'AirQualityRecord':
                    with engine.connect() as conn:
                        from sqlalchemy import text
                        conn.execute(text('DROP TABLE IF EXISTS "AirQualityRecord" CASCADE;'))
                save_to_postgres(mapping[table], table, engine, if_exists='replace')
                logger.info(f"Saved {table} to PostgreSQL")
            except Exception as e:
                logger.error(f"Error saving to PostgreSQL table {table}: {e}")
    logger.info("Đã load dữ liệu sạch vào PostgreSQL thành công.")

if __name__ == '__main__':
    main()
