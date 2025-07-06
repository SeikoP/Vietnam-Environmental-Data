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

# Load environment variables
config_dir = Path(__file__).parent.parent.parent / "config"
env_file = config_dir / ".env"

if env_file.exists():
    load_dotenv(env_file)
    print(f"✅ Đã load file .env từ: {env_file}")
else:
    print(f"⚠️ Không tìm thấy file .env tại: {env_file}")
    load_dotenv()

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
    Ưu tiên: locations.json > locations_vietnam.json > fallback default
    """
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
    
    print("⚠️ Không tìm thấy file JSON, thử load từ biến môi trường...")
    return load_locations_from_env()

def load_locations_from_env() -> List[Dict[str, Any]]:
    """Load danh sách địa điểm từ biến môi trường."""
    env_val = os.getenv("VIETNAM_LOCATIONS")
    if env_val:
        try:
            env_val_clean = env_val.replace("'", '"')
            env_val_clean = "".join([line.strip() for line in env_val_clean.splitlines()])
            locations = json.loads(env_val_clean)
            if isinstance(locations, list):
                print(f"✅ Đã load {len(locations)} địa điểm từ biến môi trường")
                return locations
        except Exception as ex:
            print(f"❌ Không parse được VIETNAM_LOCATIONS từ env: {ex}")
    
    print("⚠️ Sử dụng danh sách địa điểm mặc định")
    return [
        {"name": "Hanoi", "lat": 21.0285, "lon": 105.8542, "province": "Hanoi", "major_river": "Red River"},
        {"name": "Ho Chi Minh City", "lat": 10.7769, "lon": 106.7009, "province": "Ho Chi Minh City", "major_river": "Saigon River"},
        {"name": "Da Nang", "lat": 16.0544, "lon": 108.2022, "province": "Da Nang", "major_river": "Han River"},
        {"name": "Can Tho", "lat": 10.0452, "lon": 105.7469, "province": "Can Tho", "major_river": "Mekong River"},
        {"name": "Hue", "lat": 16.4637, "lon": 107.5909, "province": "Thua Thien Hue", "major_river": "Perfume River"},
        {"name": "Hai Phong", "lat": 20.8449, "lon": 106.6881, "province": "Hai Phong", "major_river": "Cam River"},
        {"name": "Dong Hoi", "lat": 17.4833, "lon": 106.5833, "province": "Quang Binh", "major_river": "Nhat Le River"},
        {"name": "Vinh Long", "lat": 10.2397, "lon": 105.9571, "province": "Vinh Long", "major_river": "Mekong River"},
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
    Bao gồm: lượng mưa, độ ẩm, nhiệt độ, áp suất.
    """
    try:
        if not OPENWEATHER_API_KEY:
            raise ValueError("OPENWEATHER_API_KEY chưa được thiết lập")
            
        params = {
            "lat": lat,
            "lon": lon,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
            "exclude": "minutely,alerts"
        }
        
        response = requests.get(OPENWEATHER_ONECALL_URL, params=params, timeout=30)
        print(f"===> OpenWeather API URL: {response.url}")
        print(f"===> OpenWeather API status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            water_data = {
                "source": "openweather",
                "coordinates": {"lat": lat, "lon": lon}
            }
            
            # Dữ liệu hiện tại
            if "current" in data:
                current = data["current"]
                water_data.update({
                    "current_temp": current.get("temp"),
                    "current_humidity": current.get("humidity"),
                    "current_pressure": current.get("pressure"),
                    "current_dew_point": current.get("dew_point"),
                    "current_visibility": current.get("visibility"),
                    "current_uv_index": current.get("uvi"),
                    "current_wind_speed": current.get("wind_speed"),
                    "current_wind_direction": current.get("wind_deg"),
                    "current_clouds": current.get("clouds")
                })
                
                # Lượng mưa hiện tại
                if "rain" in current:
                    water_data["current_rain_1h"] = current["rain"].get("1h", 0)
                if "snow" in current:
                    water_data["current_snow_1h"] = current["snow"].get("1h", 0)
            
            # Dữ liệu hàng giờ (24 giờ tới)
            if "hourly" in data:
                hourly = data["hourly"][:24]
                rain_forecast = []
                temp_forecast = []
                humidity_forecast = []
                
                for hour in hourly:
                    temp_forecast.append(hour.get("temp"))
                    humidity_forecast.append(hour.get("humidity"))
                    rain_amount = 0
                    if "rain" in hour:
                        rain_amount = hour["rain"].get("1h", 0)
                    rain_forecast.append(rain_amount)
                
                water_data.update({
                    "forecast_24h_rain_total": sum(rain_forecast),
                    "forecast_24h_rain_avg": sum(rain_forecast) / len(rain_forecast) if rain_forecast else 0,
                    "forecast_24h_temp_avg": sum(temp_forecast) / len(temp_forecast) if temp_forecast else 0,
                    "forecast_24h_humidity_avg": sum(humidity_forecast) / len(humidity_forecast) if humidity_forecast else 0,
                    "forecast_24h_max_temp": max(temp_forecast) if temp_forecast else None,
                    "forecast_24h_min_temp": min(temp_forecast) if temp_forecast else None
                })
            
            # Dữ liệu hàng ngày (7 ngày tới)
            if "daily" in data:
                daily = data["daily"][:7]
                weekly_rain = []
                weekly_temp_max = []
                weekly_temp_min = []
                weekly_humidity = []
                
                for day in daily:
                    rain_amount = 0
                    if "rain" in day:
                        rain_amount = day["rain"]
                    weekly_rain.append(rain_amount)
                    
                    if "temp" in day:
                        weekly_temp_max.append(day["temp"].get("max"))
                        weekly_temp_min.append(day["temp"].get("min"))
                    
                    weekly_humidity.append(day.get("humidity"))
                
                water_data.update({
                    "forecast_7d_rain_total": sum(weekly_rain),
                    "forecast_7d_rain_avg": sum(weekly_rain) / len(weekly_rain) if weekly_rain else 0,
                    "forecast_7d_temp_max_avg": sum(weekly_temp_max) / len(weekly_temp_max) if weekly_temp_max else 0,
                    "forecast_7d_temp_min_avg": sum(weekly_temp_min) / len(weekly_temp_min) if weekly_temp_min else 0,
                    "forecast_7d_humidity_avg": sum(weekly_humidity) / len(weekly_humidity) if weekly_humidity else 0
                })
            
            return water_data
        else:
            print(f"❌ OpenWeather API error: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu OpenWeather: {e}")
        return None

def get_soilgrids_data(lat: float, lon: float) -> Dict:
    """
    Lấy dữ liệu đất từ SoilGrids API.
    Bao gồm: độ pH, hàm lượng hữu cơ.
    """
    try:
        url = f"{SOILGRIDS_URL}?lon={lon}&lat={lat}&attributes=phh2o,orgc"
        headers = {"Authorization": f"Bearer {SOILGRIDS_API_KEY}"} if SOILGRIDS_API_KEY else {}
        
        response = requests.get(url, headers=headers, timeout=30)
        print(f"===> SoilGrids API URL: {response.url}")
        print(f"===> SoilGrids API status: {response.status_code}")
        
        if response.status_code == 200:
            soil_data = response.json()
            properties = soil_data.get("properties", {})
            
            return {
                "source": "soilgrids",
                "coordinates": {"lat": lat, "lon": lon},
                "ph_h2o": properties.get("phh2o", {}).get("mean", [None])[0],
                "organic_carbon": properties.get("orgc", {}).get("mean", [None])[0]
            }
        else:
            print(f"❌ SoilGrids API error: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu SoilGrids: {e}")
        return None

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
    """
    indices = {}
    
    try:
        rain_24h = water_data.get("forecast_24h_rain_total", 0)
        rain_7d = water_data.get("forecast_7d_rain_total", 0)
        
        flood_risk = "high" if rain_24h > 50 else "medium" if rain_24h > 20 else "low"
        indices["flood_risk_weather"] = flood_risk
        indices["rainfall_24h"] = rain_24h
        indices["rainfall_7d"] = rain_7d
        
        current_temp = water_data.get("current_temp", 25)
        temp_stress = "high" if current_temp > 35 else "medium" if current_temp > 30 else "low"
        indices["temperature_stress"] = temp_stress
        
        humidity = water_data.get("current_humidity", 70)
        evaporation_rate = max(0, (current_temp - 20) * (100 - humidity) / 100)
        indices["evaporation_rate"] = evaporation_rate
        
        quality_score = water_data.get("estimated_water_quality_score", 50)
        indices["water_quality_index"] = quality_score
        
        annual_rainfall = water_data.get("annual_rainfall_mm", 2000)
        water_abundance = (
            "very_high" if annual_rainfall > 2500 else
            "high" if annual_rainfall > 2000 else
            "medium" if annual_rainfall > 1500 else "low"
        )
        indices["water_abundance"] = water_abundance
        
    except Exception as e:
        print(f"❌ Lỗi khi tính toán chỉ số nước: {e}")
    
    return indices

@app.post("/run_water_crawl")
async def run_water_crawl(request: Request):
    """
    Crawl dữ liệu nước và đất từ nhiều nguồn cho các địa điểm ở Việt Nam.
    """
    try:
        body = await request.json() or {}
        all_locations = load_locations_from_json()
        filter_criteria = body.get("filter", {})
        locations = filter_locations_by_criteria(all_locations, filter_criteria)
        
        if "locations" in body:
            locations = body["locations"]
        
        if not OPENWEATHER_API_KEY:
            raise HTTPException(status_code=500, detail="OPENWEATHER_API_KEY chưa được thiết lập")
        
        print(f"💧 Sẽ crawl dữ liệu nước và đất cho {len(locations)} địa điểm")
        
        data = []
        for i, location in enumerate(locations, 1):
            print(f"🔄 [{i}/{len(locations)}] Đang crawl dữ liệu cho {location['name']}...")
            
            record = {
                "timestamp": datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
                "location": location["name"],
                "province": location["province"],
                "lat": location["lat"],
                "lon": location["lon"],
                "major_river": location.get("major_river", "N/A")
            }
            
            print(f"  🌤️ Lấy dữ liệu thời tiết/nước...")
            weather_data = get_openweather_water_data(location["lat"], location["lon"])
            if weather_data:
                record.update(weather_data)
            else:
                print(f"  ❌ Không lấy được dữ liệu thời tiết/nước")
            
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
            
            print(f"  🧪 Ước tính chất lượng nước...")
            quality_data = get_water_quality_estimation(location["lat"], location["lon"])
            if quality_data:
                record.update(quality_data)
            else:
                print(f"  ❌ Không ước tính được chất lượng nước")
            
            indices = calculate_water_indices(record)
            record.update(indices)
            
            data.append(record)
            print(f"✅ Hoàn thành: {location['name']}")
            time.sleep(1)  # Delay để tránh rate limiting

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
        raise HTTPException(status_code=500, detail=f"Lỗi: {str(e)}")

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
    
    quality_data = get_water_quality_estimation(location["lat"], location["lon"])
    if quality_data:
        water_data.update(quality_data)
    
    indices = calculate_water_indices(water_data)
    water_data.update(indices)
    
    return water_data

@app.get("/water_quality_summary")
def get_water_quality_summary():
    """API để lấy tóm tắt chất lượng nước và đất cho tất cả địa điểm"""
    locations = load_locations_from_json()
    summary = []
    
    for location in locations[:5]:
        quality_data = get_water_quality_estimation(location["lat"], location["lon"])
        soil_data = get_soilgrids_data(location["lat"], location["lon"])
        if quality_data or soil_data:
            summary.append({
                "location": location["name"],
                "province": location["province"],
                "water_quality_score": quality_data.get("estimated_water_quality_score", 0) if quality_data else 0,
                "water_quality_category": quality_data.get("water_quality_category", "unknown") if quality_data else "unknown",
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
    locations = locations[:3]  # Chỉ lấy 3 địa điểm khi test
    print(f"🌍 Sẽ crawl {len(locations)} địa điểm")
    
    if not OPENWEATHER_API_KEY:
        print("❌ Lỗi: OPENWEATHER_API_KEY chưa được thiết lập")
        return
    
    data = []
    for i, location in enumerate(locations, 1):
        print(f"🔄 [{i}/{len(locations)}] Crawling data for {location['name']}...")
        
        record = {
            "timestamp": datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
            "location": location["name"],
            "province": location["province"],
            "lat": location["lat"],
            "lon": location["lon"],
            "major_river": location.get("major_river", "N/A")
        }
        
        print(f"  🌤️ Getting weather/water data...")
        weather_data = get_openweather_water_data(location["lat"], location["lon"])
        if weather_data:
            record.update(weather_data)
            print(f"  ✅ Weather/water data: OK")
        else:
            print(f"  ❌ Weather/water data: Failed")
        
        print(f"  🌱 Getting soil data...")
        soil_data = get_soilgrids_data(location["lat"], location["lon"])
        if soil_data:
            record.update(soil_data)
            print(f"  ✅ Soil data: OK")
        else:
            print(f"  ❌ Soil data: Failed")
        
        print(f"  🇻🇳 Getting Vietnam water resources data...")
        vn_water_data = get_vietnam_water_resources_data(
            location["lat"], location["lon"], location["name"]
        )
        if vn_water_data:
            record.update(vn_water_data)
            print(f"  ✅ Vietnam water resources: OK")
        else:
            print(f"  ❌ Vietnam water resources: Failed")
        
        print(f"  🧪 Estimating water quality...")
        quality_data = get_water_quality_estimation(location["lat"], location["lon"])
        if quality_data:
            record.update(quality_data)
            print(f"  ✅ Water quality estimation: OK")
        else:
            print(f"  ❌ Water quality estimation: Failed")
        
        indices = calculate_water_indices(record)
        record.update(indices)
        
        data.append(record)
        print(f"✅ Completed: {location['name']}")
        time.sleep(1)
    
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