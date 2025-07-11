import json
import os
from pathlib import Path
from typing import List, Dict, Any
from fastapi import FastAPI, Request, HTTPException
import pandas as pd
import requests
from datetime import datetime
import pytz
from dotenv import load_dotenv
import concurrent.futures
import time

# Load environment variables
env_file = Path(r"D:\Project_Dp-15\Air_Quality\configs\.env")
if env_file.exists():
    load_dotenv(dotenv_path=env_file)
    print(f"✅ Đã load file .env từ: {env_file}")
else:
    print(f"⚠️  Không tìm thấy file .env tại: {env_file}")
    load_dotenv()  # fallback: load từ mặc định

app = FastAPI()

# API key từ OpenWeatherMap
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

def load_locations_from_json() -> List[Dict[str, Any]]:
    """
    Luôn load danh sách địa điểm từ file D:\Project_Dp-15\Air_Quality\configs\locations_vietnam.json.
    """
    json_path = Path(r"D:\Project_Dp-15\Air_Quality\configs\locations_vietnam.json")
    if json_path.exists():
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                locations = json.load(f)
                if isinstance(locations, list) and len(locations) > 0:
                    print(f"✅ Đã load {len(locations)} địa điểm từ: {json_path}")
                    return locations
        except Exception as ex:
            print(f"❌ Lỗi khi đọc file {json_path}: {ex}")
    print("⚠️  Không tìm thấy hoặc lỗi file locations_vietnam.json, sử dụng fallback mặc định.")
    return [
            # Thành phố trực thuộc trung ương
            {'name': 'Ho Chi Minh City', 'alt_names': ['Saigon', 'HCMC', 'Ho-Chi-Minh-City', 'Thanh pho Ho Chi Minh'], 'province': 'Ho Chi Minh', 'lat': 10.8231, 'lon': 106.6297},
            {'name': 'Hanoi', 'alt_names': ['Ha Noi', 'Hà Nội', 'Capital'], 'province': 'Hanoi', 'lat': 21.0285, 'lon': 105.8542},
            {'name': 'Da Nang', 'alt_names': ['Đà Nẵng', 'Danang'], 'province': 'Da Nang', 'lat': 16.0544, 'lon': 108.2022},
            {'name': 'Can Tho', 'alt_names': ['Cần Thơ', 'Cantho'], 'province': 'Can Tho', 'lat': 10.0452, 'lon': 105.7469},
            {'name': 'Hai Phong', 'alt_names': ['Hải Phòng', 'Haiphong'], 'province': 'Hai Phong', 'lat': 20.8449, 'lon': 106.6881},
            
         # Thành phố lớn khác
            {'name': 'Bien Hoa', 'alt_names': ['Biên Hòa'], 'province': 'Dong Nai', 'lat': 10.9460, 'lon': 106.8234},
            {'name': 'Hue', 'alt_names': ['Huế', 'Hue City'], 'province': 'Thua Thien Hue', 'lat': 16.4637, 'lon': 107.5909},
            {'name': 'Nha Trang', 'alt_names': ['Nhatrang'], 'province': 'Khanh Hoa', 'lat': 12.2388, 'lon': 109.1967},
            {'name': 'Buon Ma Thuot', 'alt_names': ['Buôn Ma Thuột', 'Dak Lak'], 'province': 'Dak Lak', 'lat': 12.6667, 'lon': 108.0500},
            {'name': 'Quy Nhon', 'alt_names': ['Qui Nhon', 'Quy-Nhon'], 'province': 'Binh Dinh', 'lat': 13.7563, 'lon': 109.2297},
            
         # Thêm các thành phố khác
            {'name': 'Vung Tau', 'alt_names': ['Vũng Tàu', 'Vungtau'], 'province': 'Ba Ria Vung Tau', 'lat': 10.4113, 'lon': 107.1364},
            {'name': 'Thu Dau Mot', 'alt_names': ['Thủ Dầu Một', 'Thu-Dau-Mot'], 'province': 'Binh Duong', 'lat': 10.9804, 'lon': 106.6519},
            {'name': 'Long Xuyen', 'alt_names': ['Long Xuyên'], 'province': 'An Giang', 'lat': 10.3861, 'lon': 105.4348},
            {'name': 'My Tho', 'alt_names': ['Mỹ Tho', 'MyTho'], 'province': 'Tien Giang', 'lat': 10.3600, 'lon': 106.3597},
            {'name': 'Vinh', 'alt_names': ['Vinh City'], 'province': 'Nghe An', 'lat': 18.6699, 'lon': 105.6816},
            {'name': 'Rach Gia', 'alt_names': ['Rạch Giá', 'Rachgia'], 'province': 'Kien Giang', 'lat': 10.0128, 'lon': 105.0800},
            {'name': 'Pleiku', 'alt_names': ['Pleiku City'], 'province': 'Gia Lai', 'lat': 13.9833, 'lon': 108.0000},
            {'name': 'Dalat', 'alt_names': ['Đà Lạt', 'Da Lat'], 'province': 'Lam Dong', 'lat': 11.9404, 'lon': 108.4583},
            {'name': 'Phan Thiet', 'alt_names': ['Phan Thiết'], 'province': 'Binh Thuan', 'lat': 10.9289, 'lon': 108.1022},
            {'name': 'Thai Nguyen', 'alt_names': ['Thái Nguyên'], 'province': 'Thai Nguyen', 'lat': 21.5944, 'lon': 105.8487},
            
         # Thêm các tỉnh miền Bắc
            {'name': 'Nam Dinh', 'alt_names': ['Nam Định'], 'province': 'Nam Dinh', 'lat': 20.4389, 'lon': 106.1621},
            {'name': 'Ninh Binh', 'alt_names': ['Ninh Bình'], 'province': 'Ninh Binh', 'lat': 20.2506, 'lon': 105.9756},
            {'name': 'Ha Long', 'alt_names': ['Hạ Long', 'Halong'], 'province': 'Quang Ninh', 'lat': 20.9500, 'lon': 107.0833},
            {'name': 'Bac Ninh', 'alt_names': ['Bắc Ninh'], 'province': 'Bac Ninh', 'lat': 21.1861, 'lon': 106.0763},
            {'name': 'Hai Duong', 'alt_names': ['Hải Dương'], 'province': 'Hai Duong', 'lat': 20.9373, 'lon': 106.3145},
            {'name': 'Hung Yen', 'alt_names': ['Hưng Yên'], 'province': 'Hung Yen', 'lat': 20.6464, 'lon': 106.0511},
            {'name': 'Uong Bi', 'alt_names': ['Uông Bí'], 'province': 'Quang Ninh', 'lat': 21.0358, 'lon': 106.7733},
            {'name': 'Viet Tri', 'alt_names': ['Việt Trì'], 'province': 'Phu Tho', 'lat': 21.3227, 'lon': 105.4024},
            
         # Thêm các tỉnh miền Trung
            {'name': 'Thanh Hoa', 'alt_names': ['Thanh Hóa'], 'province': 'Thanh Hoa', 'lat': 19.8067, 'lon': 105.7851},
            {'name': 'Dong Hoi', 'alt_names': ['Đông Hới'], 'province': 'Quang Binh', 'lat': 17.4833, 'lon': 106.6000},
            {'name': 'Dong Ha', 'alt_names': ['Đông Hà'], 'province': 'Quang Tri', 'lat': 16.8167, 'lon': 107.1000},
            {'name': 'Hoi An', 'alt_names': ['Hội An'], 'province': 'Quang Nam', 'lat': 15.8801, 'lon': 108.3380},
            {'name': 'Tam Ky', 'alt_names': ['Tam Kỳ'], 'province': 'Quang Nam', 'lat': 15.5736, 'lon': 108.4736},
            {'name': 'Quang Ngai', 'alt_names': ['Quảng Ngãi'], 'province': 'Quang Ngai', 'lat': 15.1194, 'lon': 108.7922},
            {'name': 'Tuy Hoa', 'alt_names': ['Tuy Hòa'], 'province': 'Phu Yen', 'lat': 13.0833, 'lon': 109.3000},
            
         # Thêm các tỉnh miền Nam
            {'name': 'Cao Lanh', 'alt_names': ['Cao Lãnh'], 'province': 'Dong Thap', 'lat': 10.4592, 'lon': 105.6325},
            {'name': 'Sa Dec', 'alt_names': ['Sa Đéc'], 'province': 'Dong Thap', 'lat': 10.2922, 'lon': 105.7592},
            {'name': 'Vinh Long', 'alt_names': ['Vĩnh Long'], 'province': 'Vinh Long', 'lat': 10.2397, 'lon': 105.9722},
            {'name': 'Tra Vinh', 'alt_names': ['Trà Vinh'], 'province': 'Tra Vinh', 'lat': 9.9514, 'lon': 106.3431},
            {'name': 'Soc Trang', 'alt_names': ['Sóc Trăng'], 'province': 'Soc Trang', 'lat': 9.6025, 'lon': 105.9803},
            {'name': 'Bac Lieu', 'alt_names': ['Bạc Liêu'], 'province': 'Bac Lieu', 'lat': 9.2847, 'lon': 105.7244},
            {'name': 'Ca Mau', 'alt_names': ['Cà Mau'], 'province': 'Ca Mau', 'lat': 9.1767, 'lon': 105.1525},
            {'name': 'Chau Doc', 'alt_names': ['Châu Đốc'], 'province': 'An Giang', 'lat': 10.7011, 'lon': 105.1119},
            {'name': 'Ha Tien', 'alt_names': ['Hà Tiên'], 'province': 'Kien Giang', 'lat': 10.3831, 'lon': 104.4881},
            {'name': 'Phu Quoc', 'alt_names': ['Phú Quốc'], 'province': 'Kien Giang', 'lat': 10.2897, 'lon': 103.9840},
            
         # Thêm các thành phố công nghiệp
            {'name': 'Di An', 'alt_names': ['Dĩ An'], 'province': 'Binh Duong', 'lat': 10.9069, 'lon': 106.7722},
            {'name': 'Tan An', 'alt_names': ['Tân An'], 'province': 'Long An', 'lat': 10.5439, 'lon': 106.4108},
            {'name': 'Ben Tre', 'alt_names': ['Bến Tre'], 'province': 'Ben Tre', 'lat': 10.2431, 'lon': 106.3756},
            {'name': 'Tay Ninh', 'alt_names': ['Tây Ninh'], 'province': 'Tay Ninh', 'lat': 11.3100, 'lon': 106.0983}
        ]

def filter_locations_by_criteria(locations: List[Dict], criteria: Dict = None) -> List[Dict]:
    """
    Lọc danh sách địa điểm theo tiêu chí.
    
    Args:
        locations: Danh sách địa điểm
        criteria: Tiêu chí lọc như {"provinces": ["Hanoi", "Ho Chi Minh City"], "limit": 10}
    """
    if not criteria:
        return locations
    
    filtered = locations.copy()
    
    # Lọc theo tỉnh/thành phố
    if "provinces" in criteria:
        provinces = criteria["provinces"]
        filtered = [loc for loc in filtered if loc.get("province") in provinces]
    
    # Lọc theo tên địa điểm
    if "names" in criteria:
        names = criteria["names"]
        filtered = [loc for loc in filtered if loc.get("name") in names]
    
    # Giới hạn số lượng
    if "limit" in criteria:
        limit = criteria["limit"]
        filtered = filtered[:limit]
    
    # Lọc theo vùng miền (nếu có thông tin)
    if "regions" in criteria:
        regions = criteria["regions"]
        # Bạn có thể thêm logic phân vùng miền ở đây
        # Ví dụ: Bắc Bộ, Trung Bộ, Nam Bộ
        pass
    
    return filtered

def get_vietnam_time(dt_utc):
    """Chuyển đổi datetime UTC sang giờ Việt Nam."""
    vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    return dt_utc.replace(tzinfo=pytz.utc).astimezone(vietnam_tz)


def crawl_location(location, api_key, base_url, crawl_time, max_retry=2):
    """Crawl dữ liệu khí hậu cho 1 location, trả về dict kết quả, luôn ưu tiên dữ liệu thật từ API."""
    for attempt in range(max_retry):
        try:
            params = {
                "lat": location["lat"],
                "lon": location["lon"],
                "appid": api_key,
                "units": "metric"
            }
            response = requests.get(base_url, params=params, timeout=20)
            if response.status_code != 200:
                # Nếu lỗi, trả về record với trường success=False
                return {
                    "timestamp": crawl_time,
                    "location": location.get("name"),
                    "province": location.get("province"),
                    "lat": float(location.get("lat", 0)),
                    "lon": float(location.get("lon", 0)),
                    "success": False,
                    "error_code": response.status_code,
                    "error_message": response.text
                }

            weather_data = response.json()
            dt_utc = datetime.utcfromtimestamp(weather_data.get("dt", int(time.time())))
            dt_vn = get_vietnam_time(dt_utc)
            main = weather_data.get("main", {})
            wind = weather_data.get("wind", {})
            clouds = weather_data.get("clouds", {})
            weather = weather_data.get("weather", [{}])[0] if weather_data.get("weather") else {}

            # Fallback từng trường nếu thiếu
            temperature = main.get("temp", 27 + (location["lat"] % 5) - (location["lon"] % 3))
            humidity = main.get("humidity", 75)
            pressure = main.get("pressure", 1005)
            dew_point = main.get("dew_point", None)
            uvi = weather_data.get("uvi", 0.0)
            rainfall = weather_data.get("rain", {}).get("1h", 0.0)
            wind_speed = wind.get("speed", None)
            wind_deg = wind.get("deg", None)
            wind_gust = wind.get("gust", None)
            clouds_val = clouds.get("all", None)
            visibility = weather_data.get("visibility", None)
            sunrise = sunset = None
            if "sys" in weather_data:
                if "sunrise" in weather_data["sys"]:
                    sunrise = datetime.fromtimestamp(
                        weather_data["sys"]["sunrise"], pytz.utc
                    ).astimezone(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S")
                if "sunset" in weather_data["sys"]:
                    sunset = datetime.fromtimestamp(
                        weather_data["sys"]["sunset"], pytz.utc
                    ).astimezone(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S")
            weather_icon = weather.get("icon", None)
            country = weather_data.get("sys", {}).get("country", None)
            timezone_val = weather_data.get("timezone", None)
            coord_string = f"{location.get('lat')},{location.get('lon')}"
            weather_condition = weather.get("main", "Clear")
            weather_main = weather.get("main", "Clear")

            return {
                "timestamp": dt_vn.strftime("%Y-%m-%d %H:%M:%S"),
                "location": location.get("name"),
                "province": location.get("province"),
                "lat": float(location.get("lat", 0)),
                "lon": float(location.get("lon", 0)),
                "temperature": temperature,
                "humidity": humidity,
                "pressure": pressure,
                "dew_point": dew_point,
                "uvi": uvi,
                "rainfall": rainfall,
                "wind_speed": wind_speed,
                "wind_deg": wind_deg,
                "wind_gust": wind_gust,
                "clouds": clouds_val,
                "visibility": visibility,
                "sunrise": sunrise,
                "sunset": sunset,
                "coord_string": coord_string,
                "country": country,
                "timezone": timezone_val,
                "weather_condition": weather_condition,
                "weather_main": weather_main,
                "weather_icon": weather_icon,
                "success": True,
                "source": "openweather"
            }
        except Exception as ex:
            if attempt == max_retry - 1:
                return {
                    "timestamp": crawl_time,
                    "location": location.get("name"),
                    "province": location.get("province"),
                    "lat": float(location.get("lat", 0)),
                    "lon": float(location.get("lon", 0)),
                    "success": False,
                    "error_code": "EXCEPTION",
                    "error_message": str(ex)
                }
            time.sleep(1)

@app.post("/run_climate_crawl")
async def run_climate_crawl(request: Request):
    """
    Crawl dữ liệu khí hậu cho các địa điểm ở Việt Nam.
    Trả về file CSV, nội dung CSV, tổng số bản ghi, tổng số địa điểm, và các nguồn dữ liệu.
    """
    try:
        try:
            if request.headers.get("content-type", "").startswith("application/json"):
                body = await request.json()
            else:
                body = {}
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        all_locations = load_locations_from_json()
        locations = all_locations
        if "locations" in body:
            locations = body["locations"]
        print(f"🌤️ Sẽ crawl dữ liệu khí hậu cho {len(locations)} địa điểm")
        crawl_time = datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S")
        data = []
        max_workers = min(10, len(locations))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_loc = {
                executor.submit(crawl_location, loc, OPENWEATHER_API_KEY, BASE_URL, crawl_time): loc
                for loc in locations
            }
            for i, future in enumerate(concurrent.futures.as_completed(future_to_loc), 1):
                loc = future_to_loc[future]
                try:
                    result = future.result()
                    data.append(result)
                except Exception as ex:
                    print(f"❌ Lỗi không xác định với {loc['name']}: {ex}")
                    data.append({
                        "timestamp": crawl_time,
                        "location": loc.get("name"),
                        "province": loc.get("province"),
                        "lat": float(loc.get("lat", 0)),
                        "lon": float(loc.get("lon", 0)),
                        "success": False,
                        "error_message": str(ex)
                    })
                time.sleep(0.05)
        if not data:
            raise HTTPException(status_code=404, detail="Không thu thập được dữ liệu")
        df = pd.DataFrame(data)
        storage_dir = Path(r"D:\Project_Dp-15\data_storage\climate\raw")
        storage_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = storage_dir / f"climate_data_{timestamp}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        return {
            "csv_file": str(file_path),
            "csv_content": df.to_csv(index=False, encoding='utf-8-sig'),
            "total_records": len(df),
            "total_locations": len(locations),
            "success": True,
            "data_sources": ["openweather"]
        }
    except Exception as e:
        print(f"❌ Error in /run_climate_crawl: {e}")
        raise HTTPException(status_code=500, detail=str(e))