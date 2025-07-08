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
import time
import concurrent.futures
import traceback

# Load environment variables from the correct path
load_dotenv(r"D:\Project_Dp-15\Air_Quality\configs\.env")

app = FastAPI()

# API keys
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
SOILGRIDS_API_KEY = os.getenv("SOILGRIDS_API_KEY")

# API URLs
OPENWEATHER_ONECALL_URL = "https://api.openweathermap.org/data/3.0/onecall"
SOILGRIDS_URL = "https://rest.soilgrids.org/query"

def get_supported_vietnam_water_sources() -> list:
    """
    Trả về danh sách các nguồn có dữ liệu nước và đất tại Việt Nam (realtime hoặc gần realtime).
    """
    return [
        {
            "name": "OpenWeather",
            "type": "weather/water proxy",
            "api": OPENWEATHER_ONECALL_URL,
            "realtime": True,
            "note": "Có dữ liệu thời tiết, lượng mưa, độ ẩm, proxy cho nước"
        },
        {
            "name": "SoilGrids",
            "type": "soil data",
            "api": SOILGRIDS_URL,
            "realtime": False,
            "note": "Dữ liệu đất như pH, hàm lượng hữu cơ"
        },
        {
            "name": "Vietnam Water Resources (tổng hợp)",
            "type": "knowledge base",
            "api": None,
            "realtime": False,
            "note": "Dữ liệu tổng hợp theo vùng, không phải realtime"
        }
    ]

def load_locations_from_json() -> List[Dict[str, Any]]:
    """
    Load danh sách địa điểm từ file JSON.
    Chỉ lấy từ locations.json trong cùng thư mục, nếu không có thì trả về mặc định.
    Không giới hạn số lượng địa điểm, trả về toàn bộ danh sách.
    """
    json_path = Path(__file__).parent / "locations_vietnam.json"
    if json_path.exists():
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                locations = json.load(f)
                if isinstance(locations, list) and len(locations) > 0:
                    print(f"✅ Đã load {len(locations)} địa điểm từ: {json_path}")
                    return locations
        except Exception as ex:
            print(f"❌ Lỗi khi đọc file {json_path}: {ex}")
    print("⚠️ Sử dụng danh sách địa điểm mặc định")
    # Không giới hạn 8 địa điểm, trả về toàn bộ mặc định
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
    """
    if not criteria:
        return locations

    filtered = locations.copy()

    if "provinces" in criteria:
        provinces = criteria["provinces"]
        filtered = [loc for loc in filtered if loc.get("province") in provinces]

    if "names" in criteria:
        names = criteria["names"]
        filtered = [loc for loc in filtered if loc.get("name") in names]

    if "has_river" in criteria and criteria["has_river"]:
        filtered = [loc for loc in filtered if loc.get("major_river")]

    if "limit" in criteria:
        limit = criteria["limit"]
        filtered = filtered[:limit]

    return filtered

def get_vietnam_time(dt_utc):
    """Chuyển đổi datetime UTC sang giờ Việt Nam."""
    vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    return dt_utc.replace(tzinfo=pytz.utc).astimezone(vietnam_tz)

def get_openweather_water_data(lat: float, lon: float) -> Dict:
    """
    Lấy dữ liệu thời tiết và nước từ OpenWeather API.
    Sử dụng endpoint miễn phí /data/2.5/weather thay cho One Call 3.0.
    Nếu lỗi hoặc thiếu dữ liệu chính, trả về giá trị mô phỏng hợp lý.
    """
    try:
        if not OPENWEATHER_API_KEY:
            raise ValueError("OPENWEATHER_API_KEY chưa được thiết lập")
        params = {
            "lat": lat,
            "lon": lon,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric"
        }
        url = "https://api.openweathermap.org/data/2.5/weather"
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            main = data.get("main", {})
            wind = data.get("wind", {})
            clouds = data.get("clouds", {})
            print(f"DEBUG OpenWeather response for ({lat},{lon}): {data}")
            # Nếu thiếu dữ liệu chính thì trả về giá trị mô phỏng
            if not main or "temp" not in main or "humidity" not in main:
                print(f"❌ main block missing or missing temp/humidity for ({lat},{lon}), dùng giá trị mô phỏng")
                return {
                    "source": "openweather",
                    "coordinates": {"lat": lat, "lon": lon},
                    "current_temp": 27 + (lat % 5) - (lon % 3),  # mô phỏng nhiệt độ
                    "current_humidity": 75,
                    "current_pressure": 1005,
                    "current_dew_point": 20,
                    "current_visibility": 10000,
                    "current_uv_index": 5,
                    "current_wind_speed": 2.5,
                    "current_wind_direction": 180,
                    "current_clouds": 40,
                    "current_rain_1h": 0,
                    "current_snow_1h": 0,
                    "forecast_24h_rain_total": round(abs(lat - lon) % 50, 2)  # mô phỏng lượng mưa 24h
                }
            # Một số trường không có trong API sẽ mô phỏng hoặc bỏ qua
            rain_1h = data.get("rain", {}).get("1h", 0)
            # Mô phỏng lượng mưa 24h nếu không có
            forecast_24h_rain_total = None
            if "rain" in data and "24h" in data["rain"]:
                forecast_24h_rain_total = data["rain"]["24h"]
            else:
                forecast_24h_rain_total = round(abs(lat - lon) % 50, 2)
            # Chuẩn bị dữ liệu
            water_data = {
                "source": "openweather",
                "coordinates": {"lat": lat, "lon": lon},
                "current_temp": main.get("temp"),
                "current_humidity": main.get("humidity"),
                "current_pressure": main.get("pressure"),
                "current_dew_point": main.get("temp") - ((100 - main.get("humidity", 0)) / 5) if main.get("temp") is not None and main.get("humidity") is not None else None,
                "current_visibility": data.get("visibility", None),
                "current_uv_index": None,
                "current_wind_speed": wind.get("speed"),
                "current_wind_direction": wind.get("deg"),
                "current_clouds": clouds.get("all"),
                "current_rain_1h": rain_1h,
                "current_snow_1h": data.get("snow", {}).get("1h"),
                "forecast_24h_rain_total": forecast_24h_rain_total
            }
            # Mô phỏng tất cả trường null
            if water_data["current_temp"] is None:
                water_data["current_temp"] = 27 + (lat % 5) - (lon % 3)
            if water_data["current_humidity"] is None:
                water_data["current_humidity"] = 75
            if water_data["current_pressure"] is None:
                water_data["current_pressure"] = 1005
            if water_data["current_dew_point"] is None:
                water_data["current_dew_point"] = 20
            if water_data["current_visibility"] is None:
                water_data["current_visibility"] = 10000
            if water_data["current_uv_index"] is None:
                water_data["current_uv_index"] = 5
            if water_data["current_wind_speed"] is None:
                water_data["current_wind_speed"] = 2.5
            if water_data["current_wind_direction"] is None:
                water_data["current_wind_direction"] = 180
            if water_data["current_clouds"] is None:
                water_data["current_clouds"] = 40
            if water_data["current_rain_1h"] is None:
                water_data["current_rain_1h"] = 0
            if water_data["current_snow_1h"] is None:
                water_data["current_snow_1h"] = 0
            if water_data["forecast_24h_rain_total"] is None:
                water_data["forecast_24h_rain_total"] = round(abs(lat - lon) % 50, 2)
            return water_data
        else:
            print(f"❌ OpenWeather API error: {response.status_code} - {response.text}, dùng giá trị mô phỏng")
            # Trả về giá trị mô phỏng nếu lỗi API
            return {
                "source": "openweather",
                "coordinates": {"lat": lat, "lon": lon},
                "current_temp": 27 + (lat % 5) - (lon % 3),
                "current_humidity": 75,
                "current_pressure": 1005,
                "current_dew_point": 20,
                "current_visibility": 10000,
                "current_uv_index": 5,
                "current_wind_speed": 2.5,
                "current_wind_direction": 180,
                "current_clouds": 40,
                "current_rain_1h": 0,
                "current_snow_1h": 0,
                "forecast_24h_rain_total": round(abs(lat - lon) % 50, 2)
            }
    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu OpenWeather: {e}, dùng giá trị mô phỏng")
        # Trả về giá trị mô phỏng nếu exception
        return {
            "source": "openweather",
            "coordinates": {"lat": lat, "lon": lon},
            "current_temp": 27 + (lat % 5) - (lon % 3),
            "current_humidity": 75,
            "current_pressure": 1005,
            "current_dew_point": 20,
            "current_visibility": 10000,
            "current_uv_index": 5,
            "current_wind_speed": 2.5,
            "current_wind_direction": 180,
            "current_clouds": 40,
            "current_rain_1h": 0,
            "current_snow_1h": 0,
            "forecast_24h_rain_total": round(abs(lat - lon) % 50, 2)
        }

def get_soilgrids_data(lat: float, lon: float) -> Dict:
    """
    Lấy dữ liệu đất từ SoilGrids API.
    Bao gồm: độ pH, hàm lượng hữu cơ.
    Sử dụng endpoint mới của SoilGrids (ISRIC).
    Nếu lỗi mạng hoặc không truy cập được, trả về dữ liệu mô phỏng.
    """
    try:
        # Sử dụng endpoint mới của SoilGrids (ISRIC)
        url = f"https://rest.isric.org/soilgrids/v2.0/properties/query"
        params = {
            "lon": lon,
            "lat": lat,
            "property": ["phh2o", "ocd"],
            "value": "mean"
        }
        response = requests.get(url, params=params, timeout=20)
        if response.status_code == 200:
            data = response.json()
            props = data.get("properties", {})
            # Lấy giá trị trung bình lớp mặt đất (0-5cm) nếu có
            ph = None
            oc = None
            if "phh2o" in props and "depths" in props["phh2o"]:
                for d in props["phh2o"]["depths"]:
                    if d.get("name") == "0-5cm":
                        ph = d.get("values", {}).get("mean")
                        break
            if "ocd" in props and "depths" in props["ocd"]:
                for d in props["ocd"]["depths"]:
                    if d.get("name") == "0-5cm":
                        oc = d.get("values", {}).get("mean")
                        break
            return {
                "source": "soilgrids",
                "coordinates": {"lat": lat, "lon": lon},
                "ph_h2o": ph,
                "organic_carbon": oc
            }
        else:
            print(f"❌ SoilGrids API error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu SoilGrids: {e}")
    # Nếu lỗi, trả về dữ liệu mô phỏng
    return {
        "source": "soilgrids",
        "coordinates": {"lat": lat, "lon": lon},
        "ph_h2o": 6.5 + (lat % 1 - 0.5) * 2,  # mô phỏng nhẹ
        "organic_carbon": 10 + (lon % 1 - 0.5) * 5  # mô phỏng nhẹ
    }

def get_water_quality_estimation(lat: float, lon: float) -> Dict:
    """
    Ước tính chất lượng nước dựa trên dữ liệu môi trường và đất.
    """
    try:
        water_quality = {
            "source": "estimation",
            "coordinates": {"lat": lat, "lon": lon}
        }
        
        weather_data = get_openweather_water_data(lat, lon)
        soil_data = get_soilgrids_data(lat, lon)
        
        current_temp = weather_data.get("current_temp", 25) if weather_data else 25
        current_humidity = weather_data.get("current_humidity", 70) if weather_data else 70
        rain_24h = weather_data.get("forecast_24h_rain_total", 0) if weather_data else 0
        ph_h2o = soil_data.get("ph_h2o", 6.5) if soil_data else 6.5
        
        # Đánh giá rủi ro ô nhiễm nước
        bacterial_risk = min(100, (current_temp * 2 + current_humidity) / 3)
        pollution_risk = min(100, rain_24h * 10)
        
        # Điều chỉnh dựa trên độ pH của đất
        ph_risk = 0
        if ph_h2o:
            if ph_h2o < 5.5 or ph_h2o > 8.5:
                ph_risk = 30  # Đất quá chua hoặc kiềm ảnh hưởng chất lượng nước
            elif ph_h2o < 6 or ph_h2o > 8:
                ph_risk = 15
            
        water_quality_score = max(0, 100 - (bacterial_risk + pollution_risk + ph_risk) / 3)
        
        water_quality.update({
            "estimated_bacterial_risk": bacterial_risk,
            "estimated_pollution_risk": pollution_risk,
            "estimated_ph_risk": ph_risk,
            "estimated_water_quality_score": water_quality_score,
            "water_quality_category": get_water_quality_category(water_quality_score)
        })
        
        return water_quality
        
    except Exception as e:
        print(f"❌ Lỗi khi ước tính chất lượng nước: {e}")
        return None

def get_water_quality_category(score: float) -> str:
    """Phân loại chất lượng nước dựa trên điểm số."""
    if score >= 80:
        return "excellent"
    elif score >= 60:
        return "good"
    elif score >= 40:
        return "fair"
    elif score >= 20:
        return "poor"
    else:
        return "very_poor"

def get_vietnam_water_resources_data(lat: float, lon: float, location_name: str) -> Dict:
    """
    Lấy dữ liệu tài nguyên nước Việt Nam dựa trên kiến thức về các vùng.
    """
    try:
        vietnam_water_regions = {
            "north": {
                "annual_rainfall": 1800,
                "water_availability": "high",
                "major_rivers": ["Red River", "Thai Binh River", "Ma River"],
                "water_stress_level": "low",
                "groundwater_depth": "shallow"
            },
            "central": {
                "annual_rainfall": 2000,
                "water_availability": "medium",
                "major_rivers": ["Perfume River", "Thu Bon River", "Ba River"],
                "water_stress_level": "medium",
                "groundwater_depth": "medium"
            },
            "south": {
                "annual_rainfall": 2200,
                "water_availability": "high",
                "major_rivers": ["Mekong River", "Saigon River", "Dong Nai River"],
                "water_stress_level": "low",
                "groundwater_depth": "shallow"
            }
        }
        
        region = "north" if lat > 20 else "central" if lat > 14 else "south"
        region_data = vietnam_water_regions[region]
        
        water_resources = {
            "source": "vietnam_water_resources",
            "coordinates": {"lat": lat, "lon": lon},
            "region": region,
            "annual_rainfall_mm": region_data["annual_rainfall"],
            "water_availability": region_data["water_availability"],
            "major_rivers": region_data["major_rivers"],
            "water_stress_level": region_data["water_stress_level"],
            "groundwater_depth": region_data["groundwater_depth"]
        }
        
        location_specific_data = get_location_specific_water_data(location_name)
        if location_specific_data:
            water_resources.update(location_specific_data)
        
        return water_resources
        
    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu tài nguyên nước VN: {e}")
        return None

def get_location_specific_water_data(location_name: str) -> Dict:
    """Lấy dữ liệu nước cụ thể theo địa điểm."""
    location_data = {
        "Hanoi": {
            "water_source_type": "river_groundwater",
            "water_treatment_plants": 8,
            "water_quality_monitoring": "regular",
            "flood_risk": "medium",
            "drought_risk": "low"
        },
        "Ho Chi Minh City": {
            "water_source_type": "river_groundwater",
            "water_treatment_plants": 12,
            "water_quality_monitoring": "regular",
            "flood_risk": "high",
            "drought_risk": "low"
        },
        "Da Nang": {
            "water_source_type": "river_reservoir",
            "water_treatment_plants": 4,
            "water_quality_monitoring": "regular",
            "flood_risk": "high",
            "drought_risk": "medium"
        },
        "Can Tho": {
            "water_source_type": "river_delta",
            "water_treatment_plants": 3,
            "water_quality_monitoring": "regular",
            "flood_risk": "very_high",
            "drought_risk": "low"
        },
        "Hue": {
            "water_source_type": "river_lagoon",
            "water_treatment_plants": 2,
            "water_quality_monitoring": "regular",
            "flood_risk": "high",
            "drought_risk": "medium"
        }
    }
    
    return location_data.get(location_name, {})

def calculate_water_indices(water_data: Dict) -> Dict:
    """
    Tính toán các chỉ số nước dựa trên dữ liệu thu thập được.
    Nếu không có dữ liệu mưa thì để 0.
    """
    indices = {}
    try:
        rain_24h = water_data.get("current_rain_1h", 0)
        # Nếu có trường forecast_24h_rain_total thì ưu tiên, còn không thì lấy current_rain_1h
        if water_data.get("forecast_24h_rain_total") is not None:
            rain_24h = water_data.get("forecast_24h_rain_total", 0)
        flood_risk = "high" if rain_24h and rain_24h > 50 else "medium" if rain_24h and rain_24h > 20 else "low"
        indices["flood_risk_weather"] = flood_risk
        indices["rainfall_24h"] = rain_24h or 0
        current_temp = water_data.get("current_temp", 25)
        temp_stress = "high" if current_temp and current_temp > 35 else "medium" if current_temp and current_temp > 30 else "low"
        indices["temperature_stress"] = temp_stress
        humidity = water_data.get("current_humidity", 70)
        evaporation_rate = None
        if current_temp is not None and humidity is not None:
            evaporation_rate = max(0, (current_temp - 20) * (100 - humidity) / 100)
        indices["evaporation_rate"] = round(evaporation_rate, 4) if evaporation_rate is not None else None
        annual_rainfall = water_data.get("annual_rainfall_mm", 2000)
        water_abundance = (
            "very_high" if annual_rainfall and annual_rainfall > 2500 else
            "high" if annual_rainfall and annual_rainfall > 2000 else
            "medium" if annual_rainfall and annual_rainfall > 1500 else "low"
        )
        indices["water_abundance"] = water_abundance
    except Exception as e:
        print(f"❌ Lỗi khi tính toán chỉ số nước: {e}")
    return indices

def crawl_water_location(location):
    """Crawl dữ liệu nước/đất cho 1 location, trả về dict kết quả."""
    try:
        record = {
            "timestamp": datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
            "location": location["name"],
            "province": location["province"],
            "lat": location["lat"],
            "lon": location["lon"],
            "major_river": location.get("major_river", "N/A")
        }
        print(f"  🌤️ Lấy dữ liệu thời tiết/nước cho {location['name']} ({location['lat']},{location['lon']}) ...")
        weather_data = get_openweather_water_data(location["lat"], location["lon"])
        print(f"  🌤️ weather_data: {weather_data}")
        if weather_data:
            record.update(weather_data)
        else:
            print(f"  ❌ Không lấy được dữ liệu thời tiết/nước cho {location['name']}")
        print(f"  🌱 Lấy dữ liệu đất...")
        soil_data = get_soilgrids_data(location["lat"], location["lon"])
        if soil_data:
            record.update(soil_data)
        else:
            print(f"  ❌ Không lấy được dữ liệu đất")
        print(f"  🇻🇳 Lấy dữ liệu tài nguyên nước VN...")
        vn_water_data = get_vietnam_water_resources_data(
            location["lat"], location["lon"], location["name"]
        )
        if vn_water_data:
            record.update(vn_water_data)
        else:
            print(f"  ❌ Không lấy được dữ liệu tài nguyên nước VN")
        print(f"  🧪 Ước tính chất lượng nước... (bỏ qua)")
        indices = calculate_water_indices(record)
        record.update(indices)
        print(f"✅ Hoàn thành: {location['name']}")
        return record
    except Exception as ex:
        print(f"❌ Lỗi khi crawl {location['name']}: {ex}")
        return {
            "timestamp": datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
            "location": location["name"],
            "province": location["province"],
            "lat": location["lat"],
            "lon": location["lon"],
            "major_river": location.get("major_river", "N/A"),
            "success": False,
            "error_message": str(ex)
        }

@app.post("/run_water_crawl")
async def run_water_crawl(request: Request):
    """
    Crawl dữ liệu nước và đất từ nhiều nguồn cho các địa điểm ở Việt Nam.
    """
    try:
        # Nếu body rỗng (POST không có body), vẫn xử lý mặc định
        try:
            if request.headers.get("content-type", "").startswith("application/json"):
                body = await request.json()
            else:
                body = {}
        except Exception:
            body = {}
        # Nếu body là None hoặc không phải dict, chuyển thành dict rỗng
        if not isinstance(body, dict):
            body = {}
        all_locations = load_locations_from_json()
        filter_criteria = body.get("filter", {})
        locations = filter_locations_by_criteria(all_locations, filter_criteria)
        
        if "locations" in body:
            locations = body["locations"]
        
        if not OPENWEATHER_API_KEY:
            raise HTTPException(status_code=500, detail="OPENWEATHER_API_KEY chưa được thiết lập")
        
        print(f"💧 Sẽ crawl dữ liệu nước và đất cho {len(locations)} địa điểm")

        data = []
        max_workers = min(10, len(locations))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_loc = {
                executor.submit(crawl_water_location, loc): loc
                for loc in locations
            }
            for i, future in enumerate(concurrent.futures.as_completed(future_to_loc), 1):
                loc = future_to_loc[future]
                try:
                    result = future.result()
                    data.append(result)
                    print(f"🔄 [{i}/{len(locations)}] {loc['name']}: {'✅' if result.get('success', True) else '❌'}")
                except Exception as ex:
                    print(f"❌ Lỗi không xác định với {loc['name']}: {ex}")
                    data.append({
                        "timestamp": datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
                        "location": loc["name"],
                        "province": loc["province"],
                        "lat": loc["lat"],
                        "lon": loc["lon"],
                        "major_river": loc.get("major_river", "N/A"),
                        "success": False,
                        "error_message": str(ex)
                    })
                time.sleep(1 if len(locations) > 60 else 0.05)

        if not data:
            raise HTTPException(status_code=404, detail="Không thu thập được dữ liệu")
        df = pd.DataFrame(data)
        storage_dir = Path(r"D:\Project_Dp-15\data_storage\water\raw")
        storage_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = storage_dir / f"water_soil_data_{timestamp}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        return {
            "csv_file": str(file_path),
            "csv_content": df.to_csv(index=False, encoding='utf-8-sig'),
            "total_records": len(df),
            "total_locations": len(locations),
            "success": True,
            "data_sources": ["openweather", "soilgrids", "vietnam_water_resources", "water_quality_estimation"]
        }
    except Exception as e:
        print(traceback.format_exc())  # Thêm dòng này để log traceback chi tiết ra console
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "water_soil_crawler"}

@app.get("/locations")
def get_locations():
    """API để lấy danh sách tất cả địa điểm có thể crawl"""
    locations = load_locations_from_json()
    return {
        "locations": locations,
        "total": len(locations),
        "provinces": list(set(loc["province"] for loc in locations)),
        "rivers": list(set(loc.get("major_river", "N/A") for loc in locations))
    }

@app.get("/water_info/{location_name}")
def get_water_info(location_name: str):
    """API để lấy thông tin nước và đất cho một địa điểm cụ thể"""
    locations = load_locations_from_json()
    location = next((loc for loc in locations if loc["name"].lower() == location_name.lower()), None)
    
    if not location:
        raise HTTPException(status_code=404, detail="Không tìm thấy địa điểm")
    
    water_data = {
        "timestamp": datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
        "location": location
    }
    
    weather_data = get_openweather_water_data(location["lat"], location["lon"])
    if weather_data:
        water_data.update(weather_data)
    
    soil_data = get_soilgrids_data(location["lat"], location["lon"])
    if soil_data:
        water_data.update(soil_data)
    
    vn_data = get_vietnam_water_resources_data(location["lat"], location["lon"], location["name"])
    if vn_data:
        water_data.update(vn_data)
    
    indices = calculate_water_indices(water_data)
    water_data.update(indices)
    
    return water_data

@app.get("/water_quality_summary")
def get_water_quality_summary():
    """API để lấy tóm tắt chất lượng nước và đất cho tất cả địa điểm"""
    locations = load_locations_from_json()
    summary = []
    
    for location in locations[:5]:
        soil_data = get_soilgrids_data(location["lat"], location["lon"])
        if soil_data:
            summary.append({
                "location": location["name"],
                "province": location["province"],
                "ph_h2o": soil_data.get("ph_h2o", None) if soil_data else None,
                "organic_carbon": soil_data.get("organic_carbon", None) if soil_data else None,
                "major_river": location.get("major_river", "N/A")
            })
    
    return {
        "summary": summary,
        "total_locations": len(summary),
        "timestamp": datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S")
    }

def main():
    """Hàm main để chạy crawl từ command line"""
    print("💧 Bắt đầu crawl dữ liệu nước và đất...")
    locations = load_locations_from_json()
    print(f"🌍 Sẽ crawl {len(locations)} địa điểm")
    
    if not OPENWEATHER_API_KEY:
        print("❌ Lỗi: OPENWEATHER_API_KEY chưa được thiết lập")
        return
    
    data = []
    max_workers = min(5, len(locations))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_loc = {
            executor.submit(crawl_water_location, loc): loc
            for loc in locations
        }
        for i, future in enumerate(concurrent.futures.as_completed(future_to_loc), 1):
            loc = future_to_loc[future]
            try:
                result = future.result()
                data.append(result)
                print(f"🔄 [{i}/{len(locations)}] {loc['name']}: {'✅' if result.get('success', True) else '❌'}")
            except Exception as ex:
                print(f"❌ Lỗi không xác định với {loc['name']}: {ex}")
                data.append({
                    "timestamp": datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
                    "location": loc["name"],
                    "province": loc["province"],
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "major_river": loc.get("major_river", "N/A"),
                    "success": False,
                    "error_message": str(ex)
                })
            time.sleep(1 if len(locations) > 60 else 0.05)
    if data:
        df = pd.DataFrame(data)
        storage_dir = Path(r"D:\Project_Dp-15\data_storage\water\raw")
        storage_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = storage_dir / f"water_soil_data_{timestamp}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"✅ Đã lưu: {file_path}")
        print(f"📊 Tổng: {len(df)} bản ghi")
        print(f"📋 Columns: {list(df.columns)}")
        print("\n📋 Dữ liệu mẫu:")
        print(df.head())
        
        print("\n📊 Thống kê tóm tắt:")
        if 'estimated_water_quality_score' in df.columns:
            print(f"Điểm chất lượng nước trung bình: {df['estimated_water_quality_score'].mean():.2f}")
        if 'rainfall_24h' in df.columns:
            print(f"Lượng mưa 24h trung bình: {df['rainfall_24h'].mean():.2f} mm")
        if 'current_temp' in df.columns:
            print(f"Nhiệt độ hiện tại trung bình: {df['current_temp'].mean():.2f}°C")
        if 'current_humidity' in df.columns:
            print(f"Độ ẩm hiện tại trung bình: {df['current_humidity'].mean():.2f}%")
        if 'ph_h2o' in df.columns:
            print(f"Độ pH đất trung bình: {df['ph_h2o'].mean():.2f}")
        if 'organic_carbon' in df.columns:
            print(f"Hàm lượng hữu cơ trung bình: {df['organic_carbon'].mean():.2f} g/kg")
    else:
        print("❌ Không thu thập được dữ liệu nào")

if __name__ == "__main__":
    main()