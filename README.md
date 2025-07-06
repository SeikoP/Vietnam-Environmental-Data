# 🌏 Vietnam Environmental Data Platform

## 📌 Giới thiệu

Hệ thống nền tảng dữ liệu môi trường Việt Nam, hỗ trợ đa nguồn dữ liệu và workflow hiện đại:
- **Crawler**: Thu thập dữ liệu không khí, nước, đất, khí hậu từ nhiều API (IQAir, WAQI, OpenWeatherMap, SoilGrids, Open-Meteo, NASA POWER, World Bank, ...).
- **Cleaner**: Làm sạch, chuẩn hóa, phân tách bảng, lưu vào PostgreSQL, hỗ trợ chuẩn hóa dữ liệu đa miền.
- **API**: Cung cấp REST API cho dashboard, phân tích, cảnh báo, truy vấn dữ liệu sạch.
- **Workflow tự động**: n8n điều phối, cảnh báo Discord, log, Power BI, hỗ trợ Docker Compose.

## 🧱 Cấu trúc dự án

```
├── crawlers/
│   ├── air/
│   │   └── air_crawler.py
│   ├── water/
│   │   └── water_crawler.py
│   ├── soil/
│   │   └── soil_crawler.py
│   └── climate/
│       └── climate_crawler.py
├── cleaners/
│   ├── air_cleaner.py
│   ├── water_cleaner.py
│   ├── soil_cleaner.py
│   └── climate_cleaner.py
├── api/
│   └── api.py
├── data_storage/
│   ├── air/
│   │   └── raw/
│   ├── water/
│   │   └── raw/
│   ├── soil/
│   │   └── raw/
│   └── climate/
│       └── raw/
├── workflows/
│   └── n8n/
├── configs/
│   ├── .env.example
│   └── requirements.txt
├── docker/
│   ├── Dockerfile.air_crawler
│   ├── Dockerfile.water_crawler
│   ├── Dockerfile.soil_crawler
│   ├── Dockerfile.climate_crawler
│   ├── Dockerfile.cleaner
│   ├── Dockerfile.api
│   └── docker-compose.yml
└── README.md
```

## ⚙️ Quy trình hệ thống

1. **Thu thập dữ liệu (Crawler)**
   - Gọi API `/run_crawl` cho từng loại: không khí, nước, đất, khí hậu.
   - Crawl đa nguồn, trả về file CSV và nội dung CSV, lưu vào `data_storage/<type>/raw/`.
   - Hỗ trợ crawl nâng cao: crawl song song, cache, retry, log chi tiết, crawl nhiều API cho cùng 1 loại dữ liệu.

2. **Làm sạch & chuẩn hóa (Cleaner)**
   - Nhận CSV qua API `/clean_<type>_data`.
   - Làm sạch, chuẩn hóa, phân tách bảng (City, Source, WeatherCondition, ...), lưu vào PostgreSQL.
   - Chuẩn hóa dữ liệu đa miền, kiểm tra ngoại lệ, mapping ID, log chi tiết.

3. **Phân tích & cảnh báo (API)**
   - API `/process-data` nhận dữ liệu sạch, phân tích, sinh cảnh báo nếu vượt ngưỡng.
   - Trả về insight, cảnh báo, khu vực ảnh hưởng, hỗ trợ truy vấn dữ liệu mới nhất.

4. **Tự động hóa & cảnh báo (Workflow)**
   - n8n workflow: Lên lịch, kiểm tra, gọi các API trên, gửi cảnh báo Discord khi phát hiện vượt ngưỡng, log thực thi, trigger Power BI.
   - Hỗ trợ tích hợp với các hệ thống BI, cảnh báo real-time.

## 🚦 Các endpoint chính

- **Crawler**:  
  - `POST /run_crawl` (air, water, soil, climate) → Trả về file CSV, nội dung CSV, tổng số bản ghi, các trường dữ liệu.
  - `GET /health` → Kiểm tra trạng thái từng crawler.
  - `GET /locations` → Danh sách địa điểm crawl được.

- **Cleaner**:  
  - `POST /clean_<type>_data` → Nhận CSV, làm sạch, chuẩn hóa, lưu DB.
  - `GET /<type>-quality` → Lấy 100 bản ghi sạch mới nhất.
  - `GET /health` → Kiểm tra trạng thái cleaner.

- **API**:  
  - `GET /air-quality`, `GET /water-quality`, ... → 100 bản ghi mới nhất.
  - `POST /process-data` → Phân tích, cảnh báo, insight cho workflow.
  - `GET /health` → Kiểm tra trạng thái API.

## 🏁 Hướng dẫn chạy

### 1. Cài đặt thư viện
```bash
pip install -r configs/requirements.txt
```

### 2. Cấu hình biến môi trường
- Tạo file `.env` từ `configs/.env.example`.
- Thiết lập các API key cần thiết (OpenWeather, IQAir, SoilGrids, ...).

### 3. Chạy từng service (có thể chạy độc lập hoặc Docker Compose)
```bash
uvicorn crawlers.air.air_crawler:app --reload --port 8081
uvicorn crawlers.water.water_crawler:app --reload --port 8082
uvicorn crawlers.soil.soil_crawler:app --reload --port 8083
uvicorn crawlers.climate.climate_crawler:app --reload --port 8084
uvicorn cleaners.air_cleaner:app --reload --port 8091
uvicorn cleaners.water_cleaner:app --reload --port 8092
uvicorn cleaners.soil_cleaner:app --reload --port 8093
uvicorn cleaners.climate_cleaner:app --reload --port 8094
uvicorn api.api:app --reload --port 8000
```

### 4. (Tùy chọn) Chạy workflow tự động với n8n
- Import workflow mẫu, cấu hình endpoint phù hợp cho từng loại dữ liệu.
- Tích hợp cảnh báo Discord, log, Power BI...

## 🐳 Docker Compose (khuyến nghị)

```yaml
version: '3.8'
services:
  air_crawler:
    build:
      context: .
      dockerfile: docker/Dockerfile.air_crawler
    env_file: configs/.env
    ports:
      - "8081:8081"
    restart: unless-stopped

  water_crawler:
    build:
      context: .
      dockerfile: docker/Dockerfile.water_crawler
    env_file: configs/.env
    ports:
      - "8082:8082"
    restart: unless-stopped

  soil_crawler:
    build:
      context: .
      dockerfile: docker/Dockerfile.soil_crawler
    env_file: configs/.env
    ports:
      - "8083:8083"
    restart: unless-stopped

  climate_crawler:
    build:
      context: .
      dockerfile: docker/Dockerfile.climate_crawler
    env_file: configs/.env
    ports:
      - "8084:8084"
    restart: unless-stopped

  air_cleaner:
    build:
      context: .
      dockerfile: docker/Dockerfile.cleaner
    env_file: configs/.env
    ports:
      - "8091:8091"
    restart: unless-stopped

  water_cleaner:
    build:
      context: .
      dockerfile: docker/Dockerfile.cleaner
    env_file: configs/.env
    ports:
      - "8092:8092"
    restart: unless-stopped

  soil_cleaner:
    build:
      context: .
      dockerfile: docker/Dockerfile.cleaner
    env_file: configs/.env
    ports:
      - "8093:8093"
    restart: unless-stopped

  climate_cleaner:
    build:
      context: .
      dockerfile: docker/Dockerfile.cleaner
    env_file: configs/.env
    ports:
      - "8094:8094"
    restart: unless-stopped

  api:
    build:
      context: .
      dockerfile: docker/Dockerfile.api
    env_file: configs/.env
    ports:
      - "8000:8000"
    restart: unless-stopped

  db:
    image: postgres:14
    environment:
      POSTGRES_DB: air_quality_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: pass
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

## ✅ Yêu cầu hệ thống

- Python 3.9+
- PostgreSQL 14+
- FastAPI, SQLAlchemy, Pandas, Uvicorn, requests, dotenv, geopandas, shapely
- (Tùy chọn) Google API Client, Docker, n8n, Power BI
- (Tùy chọn) Các API key cho nước, đất, khí hậu nếu có

## 📊 Ứng dụng & mở rộng

- Kết nối Power BI lấy dữ liệu real-time thông qua các endpoint JSON.
- Triển khai trên cloud (Heroku, Railway, EC2, Azure).
- Lên lịch tự động bằng n8n, Airflow hoặc `schedule`.
- Mở rộng thêm các nguồn dữ liệu môi trường khác, tích hợp AI phân tích dự báo.

## 🧑‍💻 Tác giả
Nguyễn Hữu Cường  
Dự án tốt nghiệp - Phân tích dữ liệu 2025
