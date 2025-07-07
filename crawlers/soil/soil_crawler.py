import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, Request, HTTPException
import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import time
import geopandas as gpd
from shapely.geometry import Point
import logging
import concurrent.futures

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
config_dir = Path(__file__).parent.parent.parent / "config"
env_file = config_dir / ".env"

if env_file.exists():
    load_dotenv(env_file)
    print(f"✅ Loaded .env file from: {env_file}")
else:
    print(f"⚠️ .env file not found at: {env_file}")
    load_dotenv()

app = FastAPI(title="Enhanced Soil Data Crawler", version="2.0")

# API keys (optional for premium features)
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
AGROMONITORING_API_KEY = os.getenv("AGROMONITORING_API_KEY")
AMBEE_API_KEY = os.getenv("AMBEE_API_KEY")
STORMGLASS_API_KEY = os.getenv("STORMGLASS_API_KEY")

# Free API URLs
SOILGRIDS_BASE_URL = "https://rest.isric.org/soilgrids/v2.0/properties/query"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
NASA_POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
WORLD_BANK_CLIMATE_URL = "https://climateknowledgeportal.worldbank.org/api/v1/country"

# Premium API URLs
AGROMONITORING_BASE_URL = "http://api.agromonitoring.com/agro/1.0/soil"
OPENWEATHER_ONECALL_URL = "https://api.openweathermap.org/data/3.0/onecall"
AMBEE_SOIL_URL = "https://api.getambee.com/v1/soil"
STORMGLASS_SOIL_URL = "https://api.stormglass.io/v2/soil/point"

# Cache directory
cache_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\soil\cache")
cache_dir.mkdir(parents=True, exist_ok=True)

def load_locations_from_json() -> List[Dict[str, Any]]:
    """Load list of locations from JSON file with priority order."""
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
                        logger.info(f"✅ Loaded {len(locations)} locations from: {json_path}")
                        return locations
            except Exception as ex:
                logger.error(f"❌ Error reading {json_path}: {ex}")
                continue
    
    logger.warning("⚠️ No JSON file found, using default Vietnam locations...")
    return get_default_vietnam_locations()

def get_default_vietnam_locations() -> List[Dict[str, Any]]:
    """Return default Vietnam locations for testing."""
    return [
        {"name": "Hanoi", "lat": 21.0285, "lon": 105.8542, "province": "Hanoi"},
        {"name": "Ho Chi Minh City", "lat": 10.7769, "lon": 106.7009, "province": "Ho Chi Minh City"},
        {"name": "Da Nang", "lat": 16.0544, "lon": 108.2022, "province": "Da Nang"},
        {"name": "Can Tho", "lat": 10.0452, "lon": 105.7469, "province": "Can Tho"},
        {"name": "Hue", "lat": 16.4637, "lon": 107.5909, "province": "Thua Thien Hue"},
        {"name": "Hai Phong", "lat": 20.8449, "lon": 106.6881, "province": "Hai Phong"},
        {"name": "Nha Trang", "lat": 12.2388, "lon": 109.1967, "province": "Khanh Hoa"},
        {"name": "Dong Nai", "lat": 10.9804, "lon": 106.8468, "province": "Dong Nai"},
        {"name": "Binh Duong", "lat": 11.1255, "lon": 106.6363, "province": "Binh Duong"},
        {"name": "Vung Tau", "lat": 10.4113, "lon": 107.1369, "province": "Ba Ria-Vung Tau"},
    ]

def get_vietnam_time(dt_utc=None):
    """Convert UTC datetime to Vietnam time."""
    vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    if dt_utc is None:
        return datetime.now(vietnam_tz)
    return dt_utc.replace(tzinfo=pytz.utc).astimezone(vietnam_tz)

def load_cached_data(lat: float, lon: float, source: str, max_age_hours: int = 24) -> Optional[Dict]:
    """Load cached API response if it's still fresh."""
    cache_file = cache_dir / f"{source}_{lat}_{lon}.json"
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Check cache age
            if 'cached_at' in data:
                cached_time = datetime.fromisoformat(data['cached_at'])
                if datetime.now() - cached_time < timedelta(hours=max_age_hours):
                    logger.info(f"✅ Using cached {source} data for lat={lat}, lon={lon}")
                    return data
                else:
                    logger.info(f"⏰ Cache expired for {source} data")
            
            return data
        except Exception as e:
            logger.error(f"❌ Error loading cached {source} data: {e}")
    return None

def save_to_cache(lat: float, lon: float, source: str, data: Dict):
    """Save API response to cache with timestamp."""
    cache_file = cache_dir / f"{source}_{lat}_{lon}.json"
    try:
        data['cached_at'] = datetime.now().isoformat()
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Cached {source} data for lat={lat}, lon={lon}")
    except Exception as e:
        logger.error(f"❌ Error saving {source} cache: {e}")

def get_soilgrids_data(lat: float, lon: float) -> Optional[Dict]:
    """Fetch comprehensive soil data from SoilGrids API (ISRIC) - FREE."""
    cached_data = load_cached_data(lat, lon, "soilgrids", max_age_hours=168)  # 1 week cache
    if cached_data:
        return cached_data

    try:
        properties = [
            "bdod",    # Bulk density
            "cec",     # Cation exchange capacity
            "cfvo",    # Coarse fragments
            "clay",    # Clay content
            "nitrogen", # Nitrogen content
            "ocd",     # Organic carbon density
            "ocs",     # Organic carbon stock
            "phh2o",   # pH in water
            "sand",    # Sand content
            "silt",    # Silt content
            "soc"      # Soil organic carbon
        ]
        depths = ["0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm"]
        
        params = {
            "lon": lon,
            "lat": lat,
            "property": properties,
            "depth": depths,
            "value": "mean"
        }
        
        response = requests.get(SOILGRIDS_BASE_URL, params=params, timeout=30)
        logger.info(f"📊 SoilGrids API status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            soil_data = {
                "source": "soilgrids",
                "coordinates": {"lat": lat, "lon": lon},
                "data_type": "static_soil_properties"
            }
            
            if "properties" in data and data["properties"]:
                for prop_name, prop_data in data["properties"].items():
                    if "depths" in prop_data and prop_data["depths"]:
                        for depth_data in prop_data["depths"]:
                            depth_name = depth_data.get("name", "")
                            depth_values = depth_data.get("values", {})
                            key = f"{prop_name}_{depth_name}"
                            if depth_values and "mean" in depth_values:
                                soil_data[key] = depth_values["mean"]
            
            save_to_cache(lat, lon, "soilgrids", soil_data)
            return soil_data
        else:
            logger.error(f"❌ SoilGrids API error: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error fetching SoilGrids data: {e}")
        return None

def get_open_meteo_soil_data(lat: float, lon: float) -> Optional[Dict]:
    """Fetch real-time soil data from Open-Meteo API - FREE."""
    cached_data = load_cached_data(lat, lon, "open_meteo", max_age_hours=1)  # 1 hour cache
    if cached_data:
        return cached_data

    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": [
                "soil_temperature_0cm",
                "soil_temperature_6cm", 
                "soil_temperature_18cm",
                "soil_temperature_54cm",
                "soil_moisture_0_1cm",
                "soil_moisture_1_3cm",
                "soil_moisture_3_9cm",
                "soil_moisture_9_27cm",
                "soil_moisture_27_81cm"
            ],
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "et0_fao_evapotranspiration"
            ],
            "timezone": "Asia/Bangkok",
            "forecast_days": 1
        }
        
        response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        logger.info(f"🌡️ Open-Meteo API status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            soil_data = {
                "source": "open_meteo",
                "coordinates": {"lat": lat, "lon": lon},
                "data_type": "realtime_soil_weather"
            }
            
            # Process hourly data (current values)
            if "hourly" in data and data["hourly"]:
                hourly = data["hourly"]
                current_index = 0  # Get current hour data
                
                for param in params["hourly"]:
                    if param in hourly and len(hourly[param]) > current_index:
                        soil_data[param] = hourly[param][current_index]
            
            # Process daily data
            if "daily" in data and data["daily"]:
                daily = data["daily"]
                for param in params["daily"]:
                    if param in daily and len(daily[param]) > 0:
                        soil_data[param] = daily[param][0]
            
            save_to_cache(lat, lon, "open_meteo", soil_data)
            return soil_data
        else:
            logger.error(f"❌ Open-Meteo API error: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error fetching Open-Meteo data: {e}")
        return None

def get_nasa_power_data(lat: float, lon: float) -> Optional[Dict]:
    """Fetch agricultural weather data from NASA POWER API - FREE."""
    cached_data = load_cached_data(lat, lon, "nasa_power", max_age_hours=24)  # 24 hour cache
    if cached_data:
        return cached_data

    try:
        # Get data for the last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        params = {
            "parameters": "T2M,T2M_MAX,T2M_MIN,RH2M,PRECTOTCORR,ALLSKY_SFC_SW_DWN,WS2M",
            "community": "AG",  # Agricultural community
            "longitude": lon,
            "latitude": lat,
            "start": start_date.strftime("%Y%m%d"),
            "end": end_date.strftime("%Y%m%d"),
            "format": "JSON"
        }
        
        response = requests.get(NASA_POWER_URL, params=params, timeout=45)
        logger.info(f"🛰️ NASA POWER API status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            soil_data = {
                "source": "nasa_power",
                "coordinates": {"lat": lat, "lon": lon},
                "data_type": "agricultural_weather"
            }
            
            if "properties" in data and "parameter" in data["properties"]:
                parameters = data["properties"]["parameter"]
                
                # Calculate averages for the last 30 days
                for param_name, param_data in parameters.items():
                    if isinstance(param_data, dict):
                        values = list(param_data.values())
                        if values:
                            soil_data[f"{param_name}_avg"] = sum(values) / len(values)
                            soil_data[f"{param_name}_max"] = max(values)
                            soil_data[f"{param_name}_min"] = min(values)
            
            save_to_cache(lat, lon, "nasa_power", soil_data)
            return soil_data
        else:
            logger.error(f"❌ NASA POWER API error: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error fetching NASA POWER data: {e}")
        return None

def get_world_bank_climate_data() -> Optional[Dict]:
    """Fetch Vietnam climate data from World Bank API - FREE."""
    cached_data = load_cached_data(0, 0, "world_bank_vn", max_age_hours=720)  # 30 days cache
    if cached_data:
        return cached_data

    try:
        # Get Vietnam climate data
        url = f"{WORLD_BANK_CLIMATE_URL}/VNM/climatology/1991/2020"
        response = requests.get(url, timeout=30)
        logger.info(f"🌍 World Bank API status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            climate_data = {
                "source": "world_bank",
                "country": "Vietnam",
                "data_type": "national_climate",
                "period": "1991-2020"
            }
            
            if isinstance(data, list) and len(data) > 0:
                climate_data.update(data[0])
            
            save_to_cache(0, 0, "world_bank_vn", climate_data)
            return climate_data
        else:
            logger.error(f"❌ World Bank API error: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error fetching World Bank data: {e}")
        return None

def get_agromonitoring_data(lat: float, lon: float) -> Optional[Dict]:
    """Fetch soil data from AgroMonitoring API - PREMIUM."""
    if not AGROMONITORING_API_KEY:
        logger.warning("⚠️ AGROMONITORING_API_KEY not set, skipping")
        return None
        
    cached_data = load_cached_data(lat, lon, "agromonitoring", max_age_hours=6)
    if cached_data:
        return cached_data

    try:
        params = {
            "lat": lat,
            "lon": lon,
            "appid": AGROMONITORING_API_KEY
        }
        response = requests.get(AGROMONITORING_BASE_URL, params=params, timeout=30)
        logger.info(f"🌾 AgroMonitoring API status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            soil_data = {
                "source": "agromonitoring",
                "soil_moisture": data.get("moisture"),
                "soil_temperature": data.get("t10", data.get("t0")),
                "coordinates": {"lat": lat, "lon": lon},
                "data_type": "realtime_soil"
            }
            save_to_cache(lat, lon, "agromonitoring", soil_data)
            return soil_data
        else:
            logger.error(f"❌ AgroMonitoring API error: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"❌ Error fetching AgroMonitoring data: {e}")
        return None

def get_openweather_soil_data(lat: float, lon: float) -> Optional[Dict]:
    """Fetch weather data from OpenWeather API - PREMIUM."""
    if not OPENWEATHER_API_KEY:
        logger.warning("⚠️ OPENWEATHER_API_KEY not set, skipping")
        return None
        
    cached_data = load_cached_data(lat, lon, "openweather", max_age_hours=2)
    if cached_data:
        return cached_data

    try:
        params = {
            "lat": lat,
            "lon": lon,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
            "exclude": "minutely,alerts"
        }
        response = requests.get(OPENWEATHER_ONECALL_URL, params=params, timeout=30)
        logger.info(f"🌤️ OpenWeather API status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            soil_data = {
                "source": "openweather",
                "data_type": "current_weather"
            }
            
            if "current" in data:
                current = data["current"]
                soil_data.update({
                    "surface_temperature": current.get("temp"),
                    "humidity": current.get("humidity"),
                    "pressure": current.get("pressure"),
                    "uv_index": current.get("uvi"),
                    "dew_point": current.get("dew_point"),
                    "wind_speed": current.get("wind_speed")
                })
            
            save_to_cache(lat, lon, "openweather", soil_data)
            return soil_data
        else:
            logger.error(f"❌ OpenWeather API error: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"❌ Error fetching OpenWeather data: {e}")
        return None

def calculate_enhanced_soil_indices(soil_data: Dict) -> Dict:
    """Calculate comprehensive soil indices from collected data."""
    indices = {}
    
    try:
        # Soil texture classification from SoilGrids
        if "clay_0-5cm" in soil_data and "sand_0-5cm" in soil_data and "silt_0-5cm" in soil_data:
            clay = soil_data["clay_0-5cm"]
            sand = soil_data["sand_0-5cm"]
            silt = soil_data["silt_0-5cm"]
            
            # USDA soil texture triangle
            if clay >= 40:
                soil_type = "clay"
            elif sand >= 85:
                soil_type = "sand"
            elif silt >= 80:
                soil_type = "silt"
            elif clay >= 35 and sand <= 45:
                soil_type = "clay_loam"
            elif clay >= 27 and sand <= 20:
                soil_type = "silty_clay"
            elif sand >= 45 and clay <= 27:
                soil_type = "sandy_loam"
            else:
                soil_type = "loam"
            
            indices.update({
                "soil_texture": soil_type,
                "clay_content": clay,
                "sand_content": sand,
                "silt_content": silt
            })
        
        # pH classification
        if "phh2o_0-5cm" in soil_data:
            ph = soil_data["phh2o_0-5cm"] / 10.0
            indices["soil_ph"] = ph
            
            if ph < 4.5:
                ph_category = "extremely_acidic"
            elif ph < 5.5:
                ph_category = "strongly_acidic"
            elif ph < 6.5:
                ph_category = "moderately_acidic"
            elif ph < 7.5:
                ph_category = "neutral"
            elif ph < 8.5:
                ph_category = "moderately_alkaline"
            else:
                ph_category = "strongly_alkaline"
            
            indices["ph_category"] = ph_category
        
        # Organic carbon classification
        if "soc_0-5cm" in soil_data:
            soc = soil_data["soc_0-5cm"] / 10.0
            indices["organic_carbon"] = soc
            
            if soc < 6:
                carbon_level = "very_low"
            elif soc < 12:
                carbon_level = "low"
            elif soc < 18:
                carbon_level = "medium"
            elif soc < 25:
                carbon_level = "high"
            else:
                carbon_level = "very_high"
            
            indices["carbon_level"] = carbon_level
        
        # Bulk density and compaction assessment
        if "bdod_0-5cm" in soil_data:
            bulk_density = soil_data["bdod_0-5cm"] / 100.0
            indices["bulk_density"] = bulk_density
            
            if bulk_density < 1.0:
                compaction_risk = "very_low"
            elif bulk_density < 1.2:
                compaction_risk = "low"
            elif bulk_density < 1.4:
                compaction_risk = "medium"
            elif bulk_density < 1.6:
                compaction_risk = "high"
            else:
                compaction_risk = "very_high"
            
            indices["compaction_risk"] = compaction_risk
        
        # Soil moisture assessment from Open-Meteo
        if "soil_moisture_0_1cm" in soil_data:
            surface_moisture = soil_data["soil_moisture_0_1cm"]
            indices["surface_soil_moisture"] = surface_moisture
            
            if surface_moisture < 0.1:
                moisture_status = "very_dry"
            elif surface_moisture < 0.2:
                moisture_status = "dry"
            elif surface_moisture < 0.3:
                moisture_status = "adequate"
            elif surface_moisture < 0.4:
                moisture_status = "moist"
            else:
                moisture_status = "wet"
            
            indices["moisture_status"] = moisture_status
        
        # Temperature stress assessment
        if "soil_temperature_0cm" in soil_data:
            soil_temp = soil_data["soil_temperature_0cm"]
            indices["surface_soil_temperature"] = soil_temp
            
            if soil_temp < 10:
                temp_stress = "cold_stress"
            elif soil_temp < 15:
                temp_stress = "cool"
            elif soil_temp < 25:
                temp_stress = "optimal"
            elif soil_temp < 35:
                temp_stress = "warm"
            else:
                temp_stress = "heat_stress"
            
            indices["temperature_stress"] = temp_stress
        
        # Evapotranspiration-based irrigation need
        if "et0_fao_evapotranspiration" in soil_data:
            et0 = soil_data["et0_fao_evapotranspiration"]
            indices["evapotranspiration"] = et0
            
            if et0 < 2:
                irrigation_need = "low"
            elif et0 < 4:
                irrigation_need = "moderate"
            elif et0 < 6:
                irrigation_need = "high"
            else:
                irrigation_need = "very_high"
            
            indices["irrigation_need"] = irrigation_need
        
        # Fertility assessment
        if "cec_0-5cm" in soil_data:
            cec = soil_data["cec_0-5cm"] / 10.0
            indices["cation_exchange_capacity"] = cec
            
            if cec < 10:
                fertility_level = "low"
            elif cec < 20:
                fertility_level = "medium"
            elif cec < 30:
                fertility_level = "high"
            else:
                fertility_level = "very_high"
            
            indices["fertility_level"] = fertility_level
        
        # Nitrogen availability
        if "nitrogen_0-5cm" in soil_data:
            nitrogen = soil_data["nitrogen_0-5cm"] / 100.0
            indices["nitrogen_content"] = nitrogen
            
            if nitrogen < 0.1:
                nitrogen_level = "deficient"
            elif nitrogen < 0.2:
                nitrogen_level = "low"
            elif nitrogen < 0.3:
                nitrogen_level = "adequate"
            else:
                nitrogen_level = "high"
            
            indices["nitrogen_level"] = nitrogen_level
        
        # Overall soil health score
        health_score = 0
        factors = 0
        
        if "carbon_level" in indices:
            health_score += {"very_low": 1, "low": 2, "medium": 3, "high": 4, "very_high": 5}[indices["carbon_level"]]
            factors += 1
        
        if "ph_category" in indices:
            health_score += {"extremely_acidic": 1, "strongly_acidic": 2, "moderately_acidic": 3, 
                           "neutral": 5, "moderately_alkaline": 3, "strongly_alkaline": 1}[indices["ph_category"]]
            factors += 1
        
        if "fertility_level" in indices:
            health_score += {"low": 1, "medium": 3, "high": 4, "very_high": 5}[indices["fertility_level"]]
            factors += 1
        
        if "compaction_risk" in indices:
            health_score += {"very_high": 1, "high": 2, "medium": 3, "low": 4, "very_low": 5}[indices["compaction_risk"]]
            factors += 1
        
        if factors > 0:
            overall_score = health_score / factors
            indices["soil_health_score"] = round(overall_score, 2)
            
            if overall_score < 2:
                health_status = "poor"
            elif overall_score < 3:
                health_status = "fair"
            elif overall_score < 4:
                health_status = "good"
            else:
                health_status = "excellent"
            
            indices["soil_health_status"] = health_status
    
    except Exception as e:
        logger.error(f"❌ Error calculating soil indices: {e}")
    
    return indices

def crawl_soil_location(location, world_bank_data=None):
    """Crawl soil data for 1 location, trả về dict kết quả."""
    try:
        logger.info(f"🔄 Crawling {location['name']}...")
        record = {
            "timestamp": get_vietnam_time().strftime("%Y-%m-%d %H:%M:%S"),
            "location": location["name"],
            "province": location["province"],
            "lat": location["lat"],
            "lon": location["lon"],
            "data_sources": []
        }
        logger.info(f"  📊 Fetching SoilGrids data...")
        soilgrids_data = get_soilgrids_data(location["lat"], location["lon"])
        if soilgrids_data:
            record.update(soilgrids_data)
            record["data_sources"].append("soilgrids")
        time.sleep(1)
        logger.info(f"  🌡️ Fetching Open-Meteo data...")
        open_meteo_data = get_open_meteo_soil_data(location["lat"], location["lon"])
        if open_meteo_data:
            record.update(open_meteo_data)
            record["data_sources"].append("open_meteo")
        time.sleep(1)
        logger.info(f"  🛰️ Fetching NASA POWER data...")
        nasa_power_data = get_nasa_power_data(location["lat"], location["lon"])
        if nasa_power_data:
            record.update(nasa_power_data)
            record["data_sources"].append("nasa_power")
        time.sleep(1)
        if world_bank_data:
            record.update(world_bank_data)
            record["data_sources"].append("world_bank")
        if AGROMONITORING_API_KEY:
            logger.info(f"  🌾 Fetching AgroMonitoring data...")
            agromonitoring_data = get_agromonitoring_data(location["lat"], location["lon"])
            if agromonitoring_data:
                record.update(agromonitoring_data)
                record["data_sources"].append("agromonitoring")
            time.sleep(1)
        if OPENWEATHER_API_KEY:
            logger.info(f"  🌤️ Fetching OpenWeather data...")
            openweather_data = get_openweather_soil_data(location["lat"], location["lon"])
            if openweather_data:
                record.update(openweather_data)
                record["data_sources"].append("openweather")
            time.sleep(1)
        logger.info(f"  🧮 Calculating soil indices...")
        indices = calculate_enhanced_soil_indices(record)
        record.update(indices)
        logger.info(f"✅ Done: {location['name']}")
        time.sleep(1)
        return record
    except Exception as ex:
        logger.error(f"❌ Error crawling {location['name']}: {ex}")
        return {
            "timestamp": get_vietnam_time().strftime("%Y-%m-%d %H:%M:%S"),
            "location": location["name"],
            "province": location["province"],
            "lat": location["lat"],
            "lon": location["lon"],
            "data_sources": [],
            "success": False,
            "error_message": str(ex)
        }

@app.post("/run_enhanced_soil_crawl")
async def run_enhanced_soil_crawl(request: Request):
    """Enhanced soil data crawling with multiple free APIs."""
    try:
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        all_locations = load_locations_from_json()
        filter_criteria = body.get("filter", {})
        locations = all_locations
        if "provinces" in filter_criteria:
            locations = [loc for loc in locations if loc.get("province") in filter_criteria["provinces"]]
        if "limit" in filter_criteria:
            locations = locations[:filter_criteria["limit"]]
        if "locations" in body:
            locations = body["locations"]
        logger.info(f"🌱 Enhanced crawling for {len(locations)} locations")
        world_bank_data = get_world_bank_climate_data()
        collected_data = []
        max_workers = min(8, len(locations))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_loc = {
                executor.submit(crawl_soil_location, loc, world_bank_data): loc
                for loc in locations
            }
            for i, future in enumerate(concurrent.futures.as_completed(future_to_loc), 1):
                loc = future_to_loc[future]
                try:
                    result = future.result()
                    collected_data.append(result)
                    logger.info(f"🔄 [{i}/{len(locations)}] {loc['name']}: {'✅' if result.get('success', True) else '❌'}")
                except Exception as ex:
                    logger.error(f"❌ Unknown error with {loc['name']}: {ex}")
                    collected_data.append({
                        "timestamp": get_vietnam_time().strftime("%Y-%m-%d %H:%M:%S"),
                        "location": loc["name"],
                        "province": loc["province"],
                        "lat": loc["lat"],
                        "lon": loc["lon"],
                        "data_sources": [],
                        "success": False,
                        "error_message": str(ex)
                    })
                time.sleep(1 if len(locations) > 60 else 0.05)
        if not collected_data:
            raise HTTPException(status_code=404, detail="No data collected")
        df = pd.DataFrame(collected_data)
        storage_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\soil\raw")
        storage_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = storage_dir / f"enhanced_soil_data_{timestamp}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        return {
            "csv_file": str(file_path),
            "csv_content": df.to_csv(index=False, encoding='utf-8-sig'),
            "total_records": len(df),
            "total_locations": len(collected_data),
            "success": True,
            "fields": list(df.columns)
        }
    except Exception as e:
        logger.error(f"❌ Error in run_enhanced_soil_crawl: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

def main():
    """Run enhanced soil crawl from command line for test/demo."""
    logger.info("🌱 Starting enhanced soil crawl (CLI mode)...")
    all_locations = load_locations_from_json()
    locations = all_locations
    logger.info(f"🌍 Will crawl {len(locations)} locations")
    world_bank_data = get_world_bank_climate_data()
    collected_data = []
    max_workers = min(5, len(locations))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_loc = {
            executor.submit(crawl_soil_location, loc, world_bank_data): loc
            for loc in locations
        }
        for i, future in enumerate(concurrent.futures.as_completed(future_to_loc), 1):
            loc = future_to_loc[future]
            try:
                result = future.result()
                collected_data.append(result)
                logger.info(f"🔄 [{i}/{len(locations)}] {loc['name']}: {'✅' if result.get('success', True) else '❌'}")
            except Exception as ex:
                logger.error(f"❌ Unknown error with {loc['name']}: {ex}")
                collected_data.append({
                    "timestamp": get_vietnam_time().strftime("%Y-%m-%d %H:%M:%S"),
                    "location": loc["name"],
                    "province": loc["province"],
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "data_sources": [],
                    "success": False,
                    "error_message": str(ex)
                })
            time.sleep(1 if len(locations) > 60 else 0.05)
    if collected_data:
        df = pd.DataFrame(collected_data)
        storage_dir = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\soil\raw")
        storage_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = storage_dir / f"enhanced_soil_data_{timestamp}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        logger.info(f"✅ Saved: {file_path}")
        logger.info(f"📊 Total: {len(df)} records")
        logger.info(f"📋 Columns: {list(df.columns)}")
        print("\n📋 Sample data:")
        print(df.head())
        print("\n📋 Data fields:")
        print(list(df.columns))
    else:
        logger.error("❌ No data collected.")

if __name__ == "__main__":
    main()