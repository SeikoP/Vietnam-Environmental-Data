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

# Load environment variables
config_dir = Path(__file__).parent.parent.parent / "config"
env_file = config_dir / ".env"

if env_file.exists():
    load_dotenv(env_file)
    print(f"✅ Đã load file .env từ: {env_file}")
else:
    print(f"⚠️  Không tìm thấy file .env tại: {env_file}")
    load_dotenv()

app = FastAPI()

# API key từ OpenWeatherMap
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

def load_locations_from_json() -> List[Dict[str, Any]]:
    """
    Load danh sách địa điểm từ file JSON.
    Ưu tiên: locations.json > locations_vietnam.json > fallback default
    """
    # Thử các đường dẫn file JSON
    possible_paths = [
        config_dir / "locations.json",
        config_dir / "locations_vietnam.json", 
        Path(__file__).parent / "locations.json",
        Path(__file__).parent / "config" / "locations.json"
    ]
    
    for json_path in possible_paths:
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    locations = json.load(f)
                    if isinstance(locations, list) and len(locations) > 0:
                        print(f"✅ Đã load {len(locations)} địa điểm từ: {json_path}")
                        return locations
            except Exception as ex:
                print(f"❌ Lỗi khi đọc file {json_path}: {ex}")
                continue
    
    # Fallback: thử load từ biến môi trường
    print("⚠️  Không tìm thấy file JSON, thử load từ biến môi trường...")
    return load_locations_from_env()

def load_locations_from_env() -> List[Dict[str, Any]]:
    """
    Load danh sách địa điểm từ biến môi trường (phương pháp cũ).
    """
    env_val = os.getenv("VIETNAM_LOCATIONS")
    if env_val:
        try:
            # Làm sạch và chuyển đổi format
            env_val_clean = env_val.replace("'", '"')
            env_val_clean = "".join([line.strip() for line in env_val_clean.splitlines()])
            locations = json.loads(env_val_clean)
            if isinstance(locations, list):
                print(f"✅ Đã load {len(locations)} địa điểm từ biến môi trường")
                return locations
        except Exception as ex:
            print(f"❌ Không parse được VIETNAM_LOCATIONS từ env: {ex}")
    
    # Fallback cuối cùng
    print("⚠️  Sử dụng danh sách địa điểm mặc định")
    return [
        {"name": "Hanoi", "lat": 21.0285, "lon": 105.8542, "province": "Hanoi"},
        {"name": "Ho Chi Minh City", "lat": 10.7769, "lon": 106.7009, "province": "Ho Chi Minh City"},
        {"name": "Da Nang", "lat": 16.0544, "lon": 108.2022, "province": "Da Nang"},
        {"name": "Can Tho", "lat": 10.0452, "lon": 105.7469, "province": "Can Tho"},
        {"name": "Hue", "lat": 16.4637, "lon": 107.5909, "province": "Thua Thien Hue"},
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

def create_locations_json_file():
    """
    Tạo file locations.json mẫu từ dữ liệu trong paste.txt
    """
    # Đây là dữ liệu từ paste.txt của bạn
    vietnam_locations = [
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
    
    # Tạo file JSON
    config_dir.mkdir(parents=True, exist_ok=True)
    json_path = config_dir / "locations_vietnam.json"
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(vietnam_locations, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Đã tạo file cấu hình: {json_path}")
    return json_path

@app.post("/run_climate_crawl")
async def run_climate_crawl(request: Request):
    """
    Crawl dữ liệu khí hậu từ OpenWeatherMap cho các địa điểm ở Việt Nam.
    """
    try:
        if not OPENWEATHER_API_KEY:
            raise HTTPException(status_code=400, detail="OPENWEATHER_API_KEY không được thiết lập")
        
        # Lấy body request
        try:
            body = await request.json()
        except:
            body = {}
        
        # Load locations với tùy chọn lọc
        all_locations = load_locations_from_json()
        
        # Áp dụng filter nếu có
        filter_criteria = body.get("filter", {})
        locations = filter_locations_by_criteria(all_locations, filter_criteria)
        
        # Override locations nếu được cung cấp trong request
        if "locations" in body:
            locations = body["locations"]
        
        print(f"🌍 Sẽ crawl dữ liệu cho {len(locations)} địa điểm")
        
        data = []
        for i, location in enumerate(locations, 1):
            print(f"🔄 [{i}/{len(locations)}] Đang crawl {location['name']}...")
            params = {
                "lat": location["lat"],
                "lon": location["lon"],
                "appid": OPENWEATHER_API_KEY,
                "units": "metric"
            }
            crawl_time = datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S")
            try:
                response = requests.get(BASE_URL, params=params, timeout=20)
                if response.status_code != 200:
                    print(f"❌ Lỗi API cho {location['name']}: {response.status_code} - {response.text}")
                    data.append({
                        "timestamp": crawl_time,
                        "location": location["name"],
                        "province": location["province"],
                        "lat": location["lat"],
                        "lon": location["lon"],
                        "success": False,
                        "error_code": response.status_code,
                        "error_message": response.text
                    })
                    continue

                weather_data = response.json()
                dt_utc = datetime.utcfromtimestamp(weather_data["dt"])
                dt_vn = get_vietnam_time(dt_utc)
                
                # Xử lý dữ liệu thời gian
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
                
                # Thêm các trường nâng cao
                dew_point = weather_data.get("main", {}).get("dew_point")
                uvi = weather_data.get("uvi")
                weather_icon = weather_data["weather"][0].get("icon") if weather_data.get("weather") else None
                country = weather_data.get("sys", {}).get("country")
                timezone_val = weather_data.get("timezone")
                coord_string = f"{location['lat']},{location['lon']}"
                
                data.append({
                    "timestamp": dt_vn.strftime("%Y-%m-%d %H:%M:%S"),
                    "crawl_time": crawl_time,
                    "location": location["name"],
                    "province": location["province"],
                    "lat": location["lat"],
                    "lon": location["lon"],
                    "coord_string": coord_string,
                    "country": country,
                    "timezone": timezone_val,
                    "temperature": weather_data["main"]["temp"],
                    "feels_like": weather_data["main"].get("feels_like"),
                    "temp_min": weather_data["main"].get("temp_min"),
                    "temp_max": weather_data["main"].get("temp_max"),
                    "humidity": weather_data["main"]["humidity"],
                    "pressure": weather_data["main"].get("pressure"),
                    "dew_point": dew_point,
                    "uvi": uvi,
                    "rainfall": weather_data.get("rain", {}).get("1h", 0.0),
                    "wind_speed": weather_data["wind"].get("speed"),
                    "wind_deg": weather_data["wind"].get("deg"),
                    "wind_gust": weather_data["wind"].get("gust"),
                    "clouds": weather_data.get("clouds", {}).get("all"),
                    "visibility": weather_data.get("visibility"),
                    "weather_condition": weather_data["weather"][0]["description"] if weather_data.get("weather") else None,
                    "weather_main": weather_data["weather"][0].get("main") if weather_data.get("weather") else None,
                    "weather_icon": weather_icon,
                    "sunrise": sunrise,
                    "sunset": sunset,
                    "source": "openweather",
                    "success": True,
                    "error_code": None,
                    "error_message": None
                })
                
            except Exception as ex:
                print(f"❌ Lỗi khi crawl {location['name']}: {ex}")
                data.append({
                    "timestamp": crawl_time,
                    "location": location["name"],
                    "province": location["province"],
                    "lat": location["lat"],
                    "lon": location["lon"],
                    "success": False,
                    "error_code": None,
                    "error_message": str(ex)
                })
                continue

        if not data:
            raise HTTPException(status_code=404, detail="Không thu thập được dữ liệu")

        df = pd.DataFrame(data)
        
        # Lưu file
        storage_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\climate\raw")
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
            "fields": list(df.columns)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi: {str(e)}")

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "climate_crawler"}

@app.get("/locations")
def get_locations():
    """API để lấy danh sách tất cả địa điểm có thể crawl"""
    locations = load_locations_from_json()
    return {
        "locations": locations,
        "total": len(locations),
        "provinces": list(set(loc["province"] for loc in locations))
    }

def main():
    """Hàm main để chạy crawl từ command line"""
    
    # Tạo file JSON config nếu chưa có
    if not (config_dir / "locations_vietnam.json").exists():
        print("📁 Tạo file cấu hình locations...")
        create_locations_json_file()
    
    if not OPENWEATHER_API_KEY:
        print("❌ OPENWEATHER_API_KEY không được thiết lập!")
        return

    print(f"✅ API Key: {OPENWEATHER_API_KEY[:8]}...")
    
    # Load locations
    locations = load_locations_from_json()
    
    # Có thể giới hạn số lượng để test
    # locations = locations[:5]  # Chỉ lấy 5 địa điểm đầu tiên
    
    print(f"🌍 Sẽ crawl {len(locations)} địa điểm")
    
    data = []
    for i, location in enumerate(locations, 1):
        print(f"🔄 [{i}/{len(locations)}] Crawling {location['name']}...")
        params = {
            "lat": location["lat"],
            "lon": location["lon"],
            "appid": OPENWEATHER_API_KEY,
            "units": "metric"
        }
        crawl_time = datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S")
        try:
            response = requests.get(BASE_URL, params=params, timeout=20)
            if response.status_code != 200:
                print(f"❌ Lỗi {location['name']}: {response.status_code} - {response.text}")
                data.append({
                    "timestamp": crawl_time,
                    "location": location["name"],
                    "province": location["province"],
                    "lat": location["lat"],
                    "lon": location["lon"],
                    "success": False,
                    "error_code": response.status_code,
                    "error_message": response.text
                })
                continue

            weather_data = response.json()
            dt_utc = datetime.utcfromtimestamp(weather_data["dt"])
            dt_vn = get_vietnam_time(dt_utc)
            
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
            
            dew_point = weather_data.get("main", {}).get("dew_point")
            uvi = weather_data.get("uvi")
            weather_icon = weather_data["weather"][0].get("icon") if weather_data.get("weather") else None
            country = weather_data.get("sys", {}).get("country")
            timezone_val = weather_data.get("timezone")
            coord_string = f"{location['lat']},{location['lon']}"
            
            data.append({
                "timestamp": dt_vn.strftime("%Y-%m-%d %H:%M:%S"),
                "crawl_time": crawl_time,
                "location": location["name"],
                "province": location["province"],
                "lat": location["lat"],
                "lon": location["lon"],
                "coord_string": coord_string,
                "country": country,
                "timezone": timezone_val,
                "temperature": weather_data["main"]["temp"],
                "feels_like": weather_data["main"].get("feels_like"),
                "temp_min": weather_data["main"].get("temp_min"),
                "temp_max": weather_data["main"].get("temp_max"),
                "humidity": weather_data["main"]["humidity"],
                "pressure": weather_data["main"].get("pressure"),
                "dew_point": dew_point,
                "uvi": uvi,
                "rainfall": weather_data.get("rain", {}).get("1h", 0.0),
                "wind_speed": weather_data["wind"].get("speed"),
                "wind_deg": weather_data["wind"].get("deg"),
                "wind_gust": weather_data["wind"].get("gust"),
                "clouds": weather_data.get("clouds", {}).get("all"),
                "visibility": weather_data.get("visibility"),
                "weather_condition": weather_data["weather"][0]["description"] if weather_data.get("weather") else None,
                "weather_main": weather_data["weather"][0].get("main") if weather_data.get("weather") else None,
                "weather_icon": weather_icon,
                "sunrise": sunrise,
                "sunset": sunset,
                "source": "openweather",
                "success": True,
                "error_code": None,
                "error_message": None
            })
            print(f"✅ OK: {location['name']}")
        except Exception as ex:
            print(f"❌ Lỗi {location['name']}: {ex}")
            data.append({
                "timestamp": crawl_time,
                "location": location["name"],
                "province": location["province"],
                "lat": location["lat"],
                "lon": location["lon"],
                "success": False,
                "error_code": None,
                "error_message": str(ex)
            })

    if data:
        df = pd.DataFrame(data)
        storage_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\climate\raw")
        storage_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = storage_dir / f"climate_data_{timestamp}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ Đã lưu: {file_path}")
        print(f"📊 Tổng: {len(df)} bản ghi")
        print("\n📋 Dữ liệu mẫu:")
        print(df.head())
        print("\n📋 Các trường dữ liệu:")
        print(list(df.columns))
    else:
        print("❌ Không thu thập được dữ liệu nào")

if __name__ == "__main__":
    main()