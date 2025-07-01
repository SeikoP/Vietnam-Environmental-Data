# 🌍 Air Quality Monitoring System

## 📌 Giới thiệu

Hệ thống giám sát chất lượng không khí tự động cho Việt Nam, gồm các thành phần:
- **Crawler**: Thu thập dữ liệu từ nhiều nguồn (IQAir, WAQI, OpenWeatherMap).
- **Cleaner**: Làm sạch, chuẩn hóa, phân tách bảng, lưu vào PostgreSQL.
- **API**: Cung cấp REST API cho dashboard, phân tích, cảnh báo.
- **Workflow tự động**: n8n điều phối, cảnh báo Discord, log, Power BI.

## 🧱 Cấu trúc dự án

```
├── data_crawler/           # Thu thập dữ liệu (FastAPI)
│   └── data_crawler.py
├── data_cleaner/           # Làm sạch, chuẩn hóa, lưu DB (FastAPI)
│   └── clean_data.py
├── api/                    # API truy xuất/phân tích/cảnh báo (FastAPI)
│   └── api.py
├── workflows/              # (Tùy chọn) Workflow n8n mẫu
├── .env.example            # Mẫu biến môi trường
├── requirements.txt        # Thư viện Python
└── README.md               # Tài liệu này
```

## ⚙️ Quy trình hệ thống

1. **Thu thập dữ liệu**
   - Gọi API `/run_optimized_crawl` (data_crawler).
   - Crawl đa nguồn, trả về file CSV và nội dung CSV.

2. **Làm sạch & chuẩn hóa**
   - Nhận CSV qua API `/main` (data_cleaner).
   - Làm sạch, chuẩn hóa, phân tách bảng, lưu vào PostgreSQL.

3. **Phân tích & cảnh báo**
   - API `/process-data` (api) nhận dữ liệu sạch, phân tích, sinh cảnh báo nếu AQI cao.
   - Trả về insight, cảnh báo, khu vực ảnh hưởng.

4. **Tự động hóa & cảnh báo**
   - n8n workflow: Lên lịch, kiểm tra, gọi các API trên, gửi cảnh báo Discord khi phát hiện AQI cao, log thực thi, trigger Power BI.

## 🚦 Các endpoint chính

- **Crawler**:  
  - `POST /run_optimized_crawl` → Trả về file CSV, nội dung CSV, tổng số bản ghi.
  - `GET /health` → Kiểm tra trạng thái crawler.

- **Cleaner**:  
  - `POST /main` → Nhận CSV, làm sạch, chuẩn hóa, lưu DB.
  - `GET /air-quality` → Lấy 100 bản ghi sạch mới nhất.
  - `GET /health` → Kiểm tra trạng thái cleaner.

- **API**:  
  - `GET /air-quality` → 100 bản ghi mới nhất.
  - `GET /kpi-summary` → KPI tổng hợp.
  - `GET /province-summary` → AQI trung bình theo tỉnh.
  - `GET /time-series?city_id=X` → Chuỗi thời gian AQI.
  - `GET /map-data` → Dữ liệu bản đồ.
  - `GET /source-breakdown` → Thống kê nguồn dữ liệu.
  - `GET /filter?...` → Lọc theo thành phố, nguồn, thời gian.
  - `GET /calculation-tab` → Trung bình/ngày theo thành phố.
  - `GET /latest-by-city` → Bản ghi mới nhất từng thành phố.
  - `POST /process-data` → Phân tích, cảnh báo, insight cho workflow.
  - `GET /health` → Kiểm tra trạng thái API.

## 🏁 Hướng dẫn chạy

### 1. Cài đặt thư viện
```bash
pip install -r requirements.txt
```

### 2. Cấu hình biến môi trường
Tạo file `.env` từ `.env.example`:
```env
DATABASE_URL=postgresql+psycopg2://user:pass@host:port/db
OPENWEATHER_API_KEY=your_key
WAQI_TOKEN=your_token
IQAIR_API_KEY=your_key
```

### 3. Chạy từng service (có thể chạy độc lập hoặc Docker Compose)
```bash
uvicorn data_crawler.data_crawler:app --reload --port 8081
uvicorn data_cleaner.clean_data:app --reload --port 8080
uvicorn api.api:app --reload --port 8000
```

### 4. (Tùy chọn) Chạy workflow tự động với n8n
- Import workflow mẫu, cấu hình endpoint phù hợp.
- Tích hợp cảnh báo Discord, log, Power BI...

## 🐳 Docker Compose (khuyến nghị)

```yaml
version: '3.8'
services:
  crawler:
    build:
      context: .
      dockerfile: Dockerfile.crawler
    env_file: .env
    ports:
      - "8081:8081"
    restart: unless-stopped

  cleaner:
    build:
      context: .
      dockerfile: Dockerfile.cleaner
    env_file: .env
    ports:
      - "8080:8080"
    restart: unless-stopped

  api:
    build:
      context: .
      dockerfile: Dockerfile.api
    env_file: .env
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
- PostgreSQL
- FastAPI, SQLAlchemy, Pandas, Uvicorn
- (Tùy chọn) Google API Client, Docker, n8n

## 📊 Ứng dụng & mở rộng

- Kết nối Power BI lấy dữ liệu real-time thông qua các endpoint JSON.
- Triển khai trên cloud (Heroku, Railway, EC2).
- Lên lịch tự động bằng Airflow hoặc `schedule`.

## 🧑‍💻 Tác giả
Nguyễn Hữu Cường  
Dự án tốt nghiệp - Phân tích dữ liệu 2025
