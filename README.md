# 🌏 Vietnam Environmental Data Platform

## 📌 Giới thiệu

Nền tảng dữ liệu môi trường Việt Nam thu thập, làm sạch, chuẩn hóa, lưu trữ và cung cấp API dữ liệu không khí, nước, đất, khí hậu từ nhiều nguồn (OpenWeather, SoilGrids, NASA POWER, World Bank, ...). Hệ thống hỗ trợ workflow tự động (n8n), cảnh báo Discord, tích hợp BI, triển khai đa môi trường (local, cloud, Docker).

---

## 🧱 Cấu trúc dự án

```
├── crawlers/
│   ├── air/
│   ├── water/
│   ├── soil/
│   └── climate/
├── cleaners/
│   ├── air_cleaner.py
│   ├── water_cleaner.py
│   ├── soil_cleaner.py
│   └── climate_cleaner.py
├── api/
│   └── api.py
├── data_storage/
│   ├── air/raw/
│   ├── water/raw/
│   ├── soil/raw/
│   └── climate/raw/
├── data_cleaner/
│   └── data/
├── workflows/
│   └── n8n/
├── configs/
│   ├── .env.example
│   └── requirements.txt
├── docker/
│   ├── Dockerfile.*
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

4. **Workflow tự động (n8n)**  
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
New-Item -ItemType Directory -Force -Path data_cleaner\data
New-Item -ItemType Directory -Force -Path workflows\n8n
New-Item -ItemType Directory -Force -Path configs
New-Item -ItemType Directory -Force -Path docker
```

**Linux/macOS:**
```bash
mkdir -p crawlers/air crawlers/water crawlers/soil crawlers/climate
mkdir -p cleaners api
mkdir -p data_storage/air/raw data_storage/water/raw data_storage/soil/raw data_storage/climate/raw
mkdir -p data_cleaner/data workflows/n8n configs docker
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
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5433:5432"
    env_file:
      - ../configs/.env
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped
    networks:
      - air_quality_network

  api:
    build:
      context: ..
      dockerfile: docker/Dockerfile.api
    ports:
      - "8000:8000"
    env_file:
      - .env
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped
    networks:
      - air_quality_network
    command: ["uvicorn", "api.api:app", "--host", "0.0.0.0", "--port", "8000"]

  air_crawler:
    build:
      context: ..
      dockerfile: docker/Dockerfile.air_crawler
    env_file: ../configs/.env
    ports:
      - "8081:8081"
    restart: unless-stopped
    networks:
      - air_quality_network
    volumes:
      - ../data_storage/air/raw:/app/data_export

  water_crawler:
    build:
      context: ..
      dockerfile: docker/Dockerfile.water_crawler
    env_file: ../configs/.env
    ports:
      - "18082:8082"
    restart: unless-stopped
    networks:
      - air_quality_network
    volumes:
      - ../data_storage/water/raw:/app/data_export

  soil_crawler:
    build:
      context: ..
      dockerfile: docker/Dockerfile.soil_crawler
    env_file: ../configs/.env
    ports:
      - "8083:8083"
    restart: unless-stopped
    networks:
      - air_quality_network
    volumes:
      - ../data_storage/soil/raw:/app/data_export

  climate_crawler:
    build:
      context: ..
      dockerfile: docker/Dockerfile.climate_crawler
    env_file: ../configs/.env
    ports:
      - "8084:8084"
    restart: unless-stopped
    networks:
      - air_quality_network
    volumes:
      - ../data_storage/climate/raw:/app/data_export

  n8n:
    image: n8nio/n8n
    ports:
      - "5678:5678"
    environment:
      - N8N_HOST=${N8N_HOST}
      - N8N_PORT=${N8N_PORT}
      - N8N_PROTOCOL=http
      - N8N_EMAIL=${N8N_EMAIL}
      - N8N_PASSWORD=${N8N_PASSWORD}
      - N8N_BASIC_AUTH_ACTIVE=true
      - N8N_USER_MANAGEMENT_DISABLED=false
    user: "1000:1000"
    volumes:
      - D:\Project_Dp-15\Air_Quality\workflow\n8n_data:/home/node/.n8n
    depends_on:
      postgres:
        condition: service_healthy
      api:
        condition: service_started
    restart: unless-stopped
    networks:
      - air_quality_network

  unified_cleaner:
    build:
      context: ..
      dockerfile: docker/Dockerfile.cleaner
    env_file: ../configs/.env
    ports:
      - "8090:8090"
    restart: unless-stopped
    depends_on:
      - postgres
    volumes:
      - ../data_cleaner/data:/app/data
    networks:
      - air_quality_network

volumes:
  postgres_data:
    name: air_quality_postgres_data
    external: true
  pgdata:

networks:
  air_quality_network:
    name: air_quality_network
    external: true
```

**Lưu ý:**  
- Mount các thư mục `data_storage/*/raw` từ host vào container `/app/data_export` để backup dữ liệu crawl.
- Mount `data_cleaner/data` từ host vào `/app/data` để backup dữ liệu sạch và bảng transform từ cleaner.
- Khi cần backup, chỉ cần copy các thư mục này trên máy host.

---

## ✅ Yêu cầu hệ thống

- Python 3.9+
- PostgreSQL 13+
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
