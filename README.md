# 🌏 Vietnam Environmental Data Platform

## 📌 Giới thiệu

Nền tảng dữ liệu môi trường Việt Nam thu thập, làm sạch, chuẩn hóa, lưu trữ và cung cấp API dữ liệu không khí, nước, đất, khí hậu từ nhiều nguồn (OpenWeather, SoilGrids, NASA POWER, World Bank, ...). Hệ thống hỗ trợ workflow tự động, cảnh báo, tích hợp BI, triển khai đa môi trường (local, cloud, Docker).

---

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

---

## ⚙️ Quy trình hệ thống

1. **Crawler**  
   - Thu thập dữ liệu từ nhiều API (không khí, nước, đất, khí hậu).
   - Lưu file CSV vào `data_storage/<type>/raw/`.
   - Hỗ trợ crawl song song, cache, retry, log chi tiết.

2. **Cleaner**  
   - Nhận file CSV, làm sạch, chuẩn hóa, phân tách bảng (City, Source, WeatherCondition, ...).
   - Lưu dữ liệu sạch vào PostgreSQL.
   - Mapping ID, kiểm tra ngoại lệ, log chi tiết.

3. **API**  
   - Cung cấp REST API cho dashboard, phân tích, cảnh báo, truy vấn dữ liệu sạch.
   - Endpoint phân tích, sinh cảnh báo nếu vượt ngưỡng.

4. **Workflow tự động**  
   - n8n điều phối, lên lịch, gọi các API, gửi cảnh báo Discord, log thực thi, trigger Power BI.

---

## 🚦 Các endpoint chính

- **Crawler**  
  - `POST /run_crawl` (air, water, soil, climate) → Trả về file CSV, nội dung CSV, tổng số bản ghi, các trường dữ liệu.
  - `GET /health` → Kiểm tra trạng thái crawler.
  - `GET /locations` → Danh sách địa điểm crawl được.

- **Cleaner**  
  - `POST /clean_<type>_data` → Nhận CSV, làm sạch, chuẩn hóa, lưu DB.
  - `GET /<type>-quality` → Lấy 100 bản ghi sạch mới nhất.
  - `GET /health` → Kiểm tra trạng thái cleaner.

- **API**  
  - `GET /air-quality`, `GET /water-quality`, ... → 100 bản ghi mới nhất.
  - `POST /process-data` → Phân tích, cảnh báo, insight cho workflow.
  - `GET /health` → Kiểm tra trạng thái API.

---

## 🏁 Hướng dẫn cài đặt & chạy

### 1. Cài đặt thư viện Python

```bash
pip install -r configs/requirements.txt
```

### 2. Cấu hình biến môi trường

- Tạo file `.env` từ `configs/.env.example`.
- Thiết lập các API key cần thiết (OpenWeather, SoilGrids, ...).

### 3. Tạo cấu trúc thư mục (nếu chưa có)

**Windows PowerShell:**
```powershell
New-Item -ItemType Directory -Force -Path crawlers\air
New-Item -ItemType Directory -Force -Path crawlers\water
New-Item -ItemType Directory -Force -Path crawlers\soil
New-Item -ItemType Directory -Force -Path crawlers\climate
New-Item -ItemType Directory -Force -Path cleaners
New-Item -ItemType Directory -Force -Path api
New-Item -ItemType Directory -Force -Path data_storage\air\raw
New-Item -ItemType Directory -Force -Path data_storage\water\raw
New-Item -ItemType Directory -Force -Path data_storage\soil\raw
New-Item -ItemType Directory -Force -Path data_storage\climate\raw
New-Item -ItemType Directory -Force -Path workflows\n8n
New-Item -ItemType Directory -Force -Path configs
New-Item -ItemType Directory -Force -Path docker
```

**Linux/macOS:**
```bash
mkdir -p crawlers/air crawlers/water crawlers/soil crawlers/climate
mkdir -p cleaners api
mkdir -p data_storage/air/raw data_storage/water/raw data_storage/soil/raw data_storage/climate/raw
mkdir -p workflows/n8n configs docker
```

### 4. Chạy từng service (local)

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

### 5. (Tùy chọn) Chạy workflow tự động với n8n

- Import workflow mẫu, cấu hình endpoint phù hợp cho từng loại dữ liệu.
- Tích hợp cảnh báo Discord, log, Power BI...

---

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

---

## ✅ Yêu cầu hệ thống

- Python 3.9+
- PostgreSQL 14+
- FastAPI, SQLAlchemy, Pandas, Uvicorn, requests, dotenv, geopandas, shapely
- (Tùy chọn) Google API Client, Docker, n8n, Power BI
- (Tùy chọn) Các API key cho nước, đất, khí hậu nếu có

---

## 📊 Ứng dụng & mở rộng

- Kết nối Power BI lấy dữ liệu real-time thông qua các endpoint JSON.
- Triển khai trên cloud (Heroku, Railway, EC2, Azure).
- Lên lịch tự động bằng n8n, Airflow hoặc `schedule`.
- Mở rộng thêm các nguồn dữ liệu môi trường khác, tích hợp AI phân tích dự báo.

---

## 📝 Lưu ý triển khai thực tế

- Đảm bảo cấu hình `.env` đúng, bảo mật các API key.
- Kiểm tra quyền ghi thư mục `data_storage/`, `data_crawler/`, `data_cleaner/`.
- Khi chạy Docker, mount volume nếu muốn giữ dữ liệu ngoài container.
- Đảm bảo PostgreSQL đã khởi động trước khi cleaner hoặc API ghi dữ liệu.
- Đọc kỹ log khi gặp lỗi, kiểm tra kết nối mạng tới các API nguồn.

---

## 🧑‍💻 Tác giả

Nguyễn Hữu Cường  
Dự án tốt nghiệp - Phân tích dữ liệu 2025
