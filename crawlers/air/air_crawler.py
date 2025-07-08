import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import logging
from bs4 import BeautifulSoup
from typing import Dict, List, Optional
import re
import os
from urllib.parse import urljoin
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
from pathlib import Path
from fastapi import FastAPI, Request
import pytz

app = FastAPI()

def get_vietnam_time_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Trả về thời gian hiện tại ở Việt Nam dưới dạng chuỗi."""
    vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    return datetime.now(vietnam_tz).strftime(fmt)

@app.post("/run_optimized_crawl")
async def run_crawl(request: Request):
    """
    Cho phép truyền API key động qua body JSON (ưu tiên), hoặc lấy từ biến môi trường.
    Nếu body không phải JSON hợp lệ, sẽ bỏ qua và chỉ lấy từ biến môi trường.
    """
    try:
        if request.headers.get("content-type", "").startswith("application/json"):
            body = await request.json()
        else:
            body = {}
    except Exception:
        body = {}

    iqair_api_key = None
    openweather_api_key = body.get("openweather_api_key") if isinstance(body, dict) else None
    waqi_token = body.get("waqi_token") if isinstance(body, dict) else None

    # Nếu không truyền qua body thì lấy từ biến môi trường
    if not openweather_api_key:
        openweather_api_key = os.getenv('OPENWEATHER_API_KEY')
    if not waqi_token:
        waqi_token = os.getenv('WAQI_TOKEN', 'demo')

    crawler = AirQualityCrawler()
    result = crawler.run_optimized_crawl(
        iqair_api_key,
        openweather_api_key,
        waqi_token
    )
    # Đảm bảo trả về JSON hợp lệ cho n8n
    return result


@app.get("/health")
def health_check():
    """Health check endpoint cho service data_crawler."""
    return {"status": "ok", "service": "data_crawler"}


# Cấu hình logging với encoding UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('air_quality_crawler.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Thay thế các ký tự đặc biệt bằng ký tự ASCII
SUCCESS_MARK = '+'
FAIL_MARK = 'x'

class AirQualityCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        self.all_data = []

    def check_connectivity(self) -> bool:
        """Kiểm tra kết nối mạng trước khi crawl"""
        try:
            response = requests.get("https://www.google.com", timeout=10)
            logger.info("Internet connection OK")
            return True
        except Exception as e:
            logger.error(f"No internet connection. Please check your network: {str(e)}")
            return False

    def get_vietnam_cities(self) -> List[Dict]:
        """Danh sách mở rộng các thành phố/tỉnh của Việt Nam với tọa độ chính xác"""
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

    def extract_number(self, text: str) -> Optional[float]:
        """Cải thiện trích xuất số từ chuỗi text"""
        if not text:
            return None
        
        # Làm sạch text
        text = str(text).strip()
        # Loại bỏ các ký tự không cần thiết
        text = re.sub(r'[^\d.,\-\s]', '', text)
        text = text.replace(',', '.').strip()
        
        # Các pattern để tìm số
        patterns = [
            r'(\d+\.?\d*)',
            r'(\d+)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            if matches:
                try:
                    return float(matches[0])
                except (ValueError, IndexError):
                    continue
        return None

    def crawl_iqair_data(self, api_key: str) -> List[Dict]:
        """Crawl dữ liệu từ IQAir API với cải thiện xử lý lỗi"""
        if not api_key:
            logger.warning("IQAir API key not provided, skipping IQAir crawling")
            return []
            
        logger.info("Starting IQAir data crawling...")
        data = []
        cities = self.get_vietnam_cities()
        
        def crawl_city(city):
            names_to_try = [city['name']] + city.get('alt_names', [])
            for name in names_to_try:
                try:
                    endpoints = [
                        {
                            'url': "https://api.airvisual.com/v2/city",
                            'params': {
                                'city': name,
                                'state': city['province'],
                                'country': 'Vietnam',
                                'key': api_key
                            }
                        },
                        {
                            'url': "https://api.airvisual.com/v2/nearest_city",
                            'params': {
                                'lat': city['lat'],
                                'lon': city['lon'],
                                'key': api_key
                            }
                        }
                    ]
                    
                    for endpoint in endpoints:
                        try:
                            response = self.session.get(endpoint['url'], params=endpoint['params'], timeout=30)
                            if response.status_code == 200:
                                json_data = response.json()
                                if json_data.get('status') == 'success' and json_data.get('data'):
                                    current = json_data['data']['current']
                                    pollution = current.get('pollution', {})
                                    weather = current.get('weather', {})
                                    record = {
                                        'timestamp': get_vietnam_time_str(),
                                        'city': city['name'],
                                        'province': city['province'],
                                        'latitude': city['lat'],
                                        'longitude': city['lon'],
                                        'aqi': pollution.get('aqius'),
                                        'aqi_cn': pollution.get('aqicn'),
                                        'pm25': pollution.get('p2', {}).get('conc') if pollution.get('p2') else None,
                                        'pm10': pollution.get('p1', {}).get('conc') if pollution.get('p1') else None,
                                        'o3': pollution.get('o3', {}).get('conc') if pollution.get('o3') else None,
                                        'no2': pollution.get('n2', {}).get('conc') if pollution.get('n2') else None,
                                        'so2': pollution.get('s2', {}).get('conc') if pollution.get('s2') else None,
                                        'co': pollution.get('co', {}).get('conc') if pollution.get('co') else None,
                                        'temperature': weather.get('tp'),
                                        'humidity': weather.get('hu'),
                                        'pressure': weather.get('pr'),
                                        'wind_speed': weather.get('ws'),
                                        'wind_direction': weather.get('wd'),
                                        'visibility': weather.get('vv'),
                                        'uv_index': weather.get('uvi'),
                                        'source': 'iqair',
                                        'raw_data': json.dumps(current, ensure_ascii=False)
                                    }
                                    logger.info(f"{SUCCESS_MARK} IQAir data crawled for {city['name']}")
                                    return record
                            elif response.status_code == 429:
                                logger.warning(f"IQAir API rate limit reached for {city['name']}. Waiting 60 seconds...")
                                time.sleep(60)
                                continue
                            elif response.status_code == 401:
                                logger.error(f"IQAir API key invalid for {city['name']}. Check your API key.")
                                return None
                            else:
                                logger.debug(f"IQAir request failed for {city['name']}. Status code: {response.status_code}")
                        except requests.exceptions.RequestException as e:
                            logger.debug(f"IQAir network error for {city['name']}: {str(e)}")
                            continue
                        except Exception as e:
                            logger.debug(f"IQAir unexpected error for {city['name']}: {str(e)}")
                            continue
                        time.sleep(1)
                except Exception as e:
                    logger.debug(f"Error processing {city['name']}: {str(e)}")
                    continue
            return None
        
        # Sử dụng ThreadPoolExecutor với số lượng worker hạn chế
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_city = {executor.submit(crawl_city, city): city for city in cities}
            for future in as_completed(future_to_city):
                try:
                    result = future.result(timeout=60)
                    if result:
                        data.append(result)
                except Exception as e:
                    logger.debug(f"Thread execution error: {str(e)}")
                    continue
        
        logger.info(f"IQAir crawling completed. Retrieved {len(data)} records")
        return data

    def crawl_waqi_data(self, token: str = 'demo') -> List[Dict]:
        """Crawl WAQI với cải thiện và xử lý lỗi tốt hơn"""
        logger.info("Starting WAQI data crawling...")
        data = []
        cities = self.get_vietnam_cities()
        
        def get_city_data(city: Dict) -> Optional[Dict]:
            """Get WAQI data for a city using multiple methods"""
            try:
                # Method 1: API với token
                if token and token != 'demo':
                    api_urls = [
                        f"https://api.waqi.info/feed/geo:{city['lat']};{city['lon']}/?token={token}",
                        f"https://api.waqi.info/feed/{city['name'].lower().replace(' ', '-')}/?token={token}",
                    ]
                    
                    for url in api_urls:
                        try:
                            response = self.session.get(url, timeout=20)
                            if response.status_code == 200:
                                data_json = response.json()
                                if data_json.get('status') == 'ok' and data_json.get('data'):
                                    aqi = data_json['data'].get('aqi')
                                    if aqi and aqi != '-':
                                        iaqi = data_json['data'].get('iaqi', {})
                                        record = {
                                            'timestamp': get_vietnam_time_str(),
                                            'city': city['name'],
                                            'province': city['province'],
                                            'latitude': city['lat'],
                                            'longitude': city['lon'],
                                            'aqi': aqi if isinstance(aqi, (int, float)) else None,
                                            'pm25': iaqi.get('pm25', {}).get('v'),
                                            'pm10': iaqi.get('pm10', {}).get('v'),
                                            'o3': iaqi.get('o3', {}).get('v'),
                                            'no2': iaqi.get('no2', {}).get('v'),
                                            'so2': iaqi.get('so2', {}).get('v'),
                                            'co': iaqi.get('co', {}).get('v'),
                                            'source': 'waqi',
                                            'status': 'success',
                                            'raw_data': json.dumps(data_json['data'], ensure_ascii=False)
                                        }
                                        logger.info(f"{SUCCESS_MARK} WAQI API data crawled for {city['name']}")
                                        return record
                        except Exception as e:
                            logger.debug(f"WAQI API error for {city['name']}: {str(e)}")
                            continue
                        time.sleep(0.5)
                
                # Method 2: Web scraping
                web_urls = [
                    f"https://aqicn.org/city/{city['name'].lower().replace(' ', '-')}",
                    f"https://aqicn.org/city/vietnam/{city['name'].lower().replace(' ', '-')}",
                ]
                
                headers = {
                    'User-Agent': random.choice(self.user_agents),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                }
                
                for web_url in web_urls:
                    try:
                        response = self.session.get(web_url, headers=headers, timeout=20)
                        if response.status_code == 200:
                            soup = BeautifulSoup(response.content, 'html.parser')
                            
                            # Tìm AQI value
                            aqi_elem = soup.find('div', {'id': 'aqiwgtvalue'})
                            if not aqi_elem:
                                aqi_elem = soup.find('span', class_='aqivalue')
                            if not aqi_elem:
                                aqi_elem = soup.find('div', class_='aqivalue')
                            
                            if aqi_elem and aqi_elem.text.strip():
                                aqi_text = aqi_elem.text.strip()
                                aqi_value = self.extract_number(aqi_text)
                                
                                if aqi_value:
                                    # Tìm các pollutant values
                                    pollutants = {}
                                    pollutant_ids = ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']
                                    
                                    for pollutant in pollutant_ids:
                                        elem = soup.find('td', {'id': f'cur_{pollutant}'})
                                        if elem:
                                            pollutants[pollutant] = self.extract_number(elem.text)
                                    
                                    record = {
                                        'timestamp': get_vietnam_time_str(),
                                        'city': city['name'],
                                        'province': city['province'],
                                        'latitude': city['lat'],
                                        'longitude': city['lon'],
                                        'aqi': aqi_value,
                                        'pm25': pollutants.get('pm25'),
                                        'pm10': pollutants.get('pm10'),
                                        'o3': pollutants.get('o3'),
                                        'no2': pollutants.get('no2'),
                                        'so2': pollutants.get('so2'),
                                        'co': pollutants.get('co'),
                                        'source': 'waqi',
                                        'status': 'success'
                                    }
                                    logger.info(f"{SUCCESS_MARK} WAQI web data crawled for {city['name']}")
                                    return record
                    except Exception as e:
                        logger.debug(f"WAQI web scraping error for {city['name']}: {str(e)}")
                        continue
                    time.sleep(1)
                
                logger.debug(f"{FAIL_MARK} Could not crawl WAQI data for {city['name']}")
                return None
                
            except Exception as e:
                logger.debug(f"{FAIL_MARK} Error crawling WAQI data for {city['name']}: {str(e)}")
                return None

        # Crawl từng thành phố với retry logic
        for city in cities:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    result = get_city_data(city)
                    if result:
                        data.append(result)
                        break
                    elif attempt < max_retries - 1:
                        time.sleep(2)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.debug(f"Failed all retries for {city['name']}: {str(e)}")
                    time.sleep(2)
        
        logger.info(f"WAQI crawling completed. Retrieved {len(data)} records")
        return data

    def crawl_openweather_data(self, api_key: str) -> List[Dict]:
        """Crawl OpenWeatherMap với cải thiện xử lý lỗi"""
        if not api_key:
            logger.warning("OpenWeatherMap API key not provided, skipping OpenWeatherMap crawling")
            return []
            
        logger.info("Starting OpenWeatherMap data crawling...")
        data = []
        cities = self.get_vietnam_cities()
        
        def crawl_city_openweather(city):
            try:
                air_url = "http://api.openweathermap.org/data/2.5/air_pollution"
                weather_url = "http://api.openweathermap.org/data/2.5/weather"
                
                air_params = {
                    'lat': city['lat'],
                    'lon': city['lon'],
                    'appid': api_key
                }
                
                weather_params = {
                    'lat': city['lat'],
                    'lon': city['lon'],
                    'appid': api_key,
                    'units': 'metric'
                }
                
                record = {
                    'timestamp': get_vietnam_time_str(),
                    'city': city['name'],
                    'province': city['province'],
                    'latitude': city['lat'],
                    'longitude': city['lon'],
                    'source': 'openweathermap'
                }
                
                # Crawl air pollution data
                try:
                    air_response = self.session.get(air_url, params=air_params, timeout=30)
                    if air_response.status_code == 200:
                        air_data = air_response.json()
                        if 'list' in air_data and air_data['list']:
                            current = air_data['list'][0]
                            main = current.get('main', {})
                            components = current.get('components', {})
                            record.update({
                                'aqi': main.get('aqi'),
                                'pm25': components.get('pm2_5'),
                                'pm10': components.get('pm10'),
                                'o3': components.get('o3'),
                                'no2': components.get('no2'),
                                'so2': components.get('so2'),
                                'co': components.get('co'),
                                'nh3': components.get('nh3'),
                            })
                    elif air_response.status_code == 401:
                        logger.error(f"OpenWeatherMap API key invalid for {city['name']}")
                        return None
                    else:
                        logger.debug(f"OpenWeatherMap air pollution API failed for {city['name']}: {air_response.status_code}")
                except Exception as e:
                    logger.debug(f"OpenWeatherMap air pollution error for {city['name']}: {str(e)}")
                
                # Crawl weather data
                try:
                    weather_response = self.session.get(weather_url, params=weather_params, timeout=30)
                    if weather_response.status_code == 200:
                        weather_data = weather_response.json()
                        main_weather = weather_data.get('main', {})
                        wind_weather = weather_data.get('wind', {})
                        record.update({
                            'temperature': main_weather.get('temp'),
                            'humidity': main_weather.get('humidity'),
                            'pressure': main_weather.get('pressure'),
                            'wind_speed': wind_weather.get('speed'),
                            'wind_direction': wind_weather.get('deg'),
                            'visibility': weather_data.get('visibility'),
                            'weather_condition': weather_data.get('weather', [{}])[0].get('main') if weather_data.get('weather') else None
                        })
                    else:
                        logger.debug(f"OpenWeatherMap weather API failed for {city['name']}: {weather_response.status_code}")
                except Exception as e:
                    logger.debug(f"OpenWeatherMap weather error for {city['name']}: {str(e)}")
                
                # Chỉ trả về record nếu có ít nhất một số dữ liệu hữu ích
                if any(record.get(key) is not None for key in ['aqi', 'pm25', 'pm10', 'temperature']):
                    logger.info(f"{SUCCESS_MARK} OpenWeatherMap data crawled for {city['name']}")
                    return record
                else:
                    logger.debug(f"{FAIL_MARK} No useful data from OpenWeatherMap for {city['name']}")
                    return None
                
            except Exception as e:
                logger.error(f"Error crawling OpenWeatherMap for {city['name']}: {str(e)}")
                return None
        
        # Crawl từng thành phố
        for city in cities:
            try:
                result = crawl_city_openweather(city)
                if result:
                    data.append(result)
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.debug(f"Failed to crawl OpenWeatherMap for {city['name']}: {str(e)}")
                continue
        
        logger.info(f"OpenWeatherMap crawling completed. Retrieved {len(data)} records")
        return data

    def merge_data(self, all_data: List[Dict]) -> List[Dict]:
        """Merge data từ các nguồn khác nhau"""
        merged = []
        for record in all_data:
            # Thêm hậu tố nguồn vào city để phân biệt
            record['city_source'] = f"{record['city']} ({record['source']})"
            merged.append(record)
        return merged

    def save_to_csv(self, data: List[Dict], filename: str = None) -> str:
        if not data:
            logger.warning("No data to save")
            return None
        
        # Sử dụng thư mục /app/data nếu tồn tại (khi chạy Docker), ngược lại dùng thư mục hiện tại
        docker_data_dir = Path("/app/data")
        if docker_data_dir.exists():
            data_folder = docker_data_dir
        else:
            # --- SỬA: Lưu về thư mục local D:\Project_Dp-15\Air_Quality\data_storage\air\raw ---
            data_folder = Path(r"D:\Project_Dp-15\Air_Quality\data_storage\air\raw")
        data_folder.mkdir(parents=True, exist_ok=True)
        
        current_date = datetime.now()
        # --- SỬA: Không tạo subfolder data_export khi lưu local ---
        if filename is None:
            filename = f"air_quality_vietnam_{current_date.strftime('%Y%m%d_%H%M%S')}.csv"
        
        file_path = data_folder / filename
        
        try:
            df = pd.DataFrame(data)
            df = df.sort_values(['timestamp', 'city'], ascending=[False, True])
            
            numeric_columns = ['aqi', 'aqi_cn', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'nh3',
                            'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction', 
                            'visibility', 'uv_index', 'latitude', 'longitude']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Loại bỏ cột raw_data trước khi lưu
            if 'raw_data' in df.columns:
                df = df.drop('raw_data', axis=1)
            
            column_order = ['timestamp', 'city', 'province', 'city_source', 'latitude', 'longitude', 
                        'aqi', 'aqi_cn', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'nh3',
                        'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction',
                        'visibility', 'uv_index', 'weather_condition', 'source', 'status']
            existing_columns = [col for col in column_order if col in df.columns]
            remaining_columns = [col for col in df.columns if col not in existing_columns]
            final_columns = existing_columns + remaining_columns
            df = df[final_columns]
            
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            logger.info(f"Data exported to {file_path} ({len(df)} records)")
            
            logger.info(f"Data summary:")
            logger.info(f"  Total records: {len(df)}")
            if 'source' in df.columns:
                source_counts = df['source'].value_counts()
                for source, count in source_counts.items():
                    logger.info(f"  {source}: {count} records")
            
            return str(file_path)
        except Exception as e:
            logger.error(f"Error saving to CSV: {str(e)}")
            return None

    def run_optimized_crawl(self, iqair_api_key: str = None, openweather_api_key: str = None, waqi_token: str = 'demo'):
        """Chạy crawl tối ưu với xử lý lỗi tốt hơn và trả về cả đường dẫn file lẫn nội dung CSV"""
        logger.info("="*60)
        logger.info("STARTING ENHANCED AIR QUALITY CRAWL")
        logger.info("="*60)
        
        if not self.check_connectivity():
            logger.error("Crawl aborted due to network issues")
            return {"success": False, "error": "Network connectivity issue"}
        
        # Chuẩn bị danh sách các task crawling
        crawl_tasks = []
        
        # WAQI luôn chạy (có thể dùng demo token)
        crawl_tasks.append(('WAQI', self.crawl_waqi_data, [waqi_token]))
        
        # IQAir chỉ chạy nếu có API key
        if iqair_api_key and iqair_api_key.strip():
            crawl_tasks.append(('IQAir', self.crawl_iqair_data, [iqair_api_key]))
        else:
            logger.info("IQAir API key not provided, skipping IQAir crawling")
        
        # OpenWeatherMap chỉ chạy nếu có API key
        if openweather_api_key and openweather_api_key.strip():
            crawl_tasks.append(('OpenWeatherMap', self.crawl_openweather_data, [openweather_api_key]))
        else:
            logger.info("OpenWeatherMap API key not provided, skipping OpenWeatherMap crawling")
        
        if not crawl_tasks:
            logger.error("No valid API keys provided, cannot crawl any data")
            return {"success": False, "error": "No valid API keys provided"}
        
        all_results = []
        
        # Chạy từng task crawling với retry logic
        for source_name, crawl_func, args in crawl_tasks:
            logger.info(f"Starting {source_name} crawling...")
            start_time = time.time()
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    results = crawl_func(*args)
                    elapsed_time = time.time() - start_time
                    
                    if results:
                        all_results.extend(results)
                        logger.info(f"✓ {source_name}: {len(results)} records in {elapsed_time:.1f}s")
                        break
                    else:
                        logger.warning(f"✗ {source_name}: No data retrieved in {elapsed_time:.1f}s")
                        if attempt < max_retries - 1:
                            time.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            logger.error(f"✗ {source_name}: Failed after {max_retries} attempts")
                except Exception as e:
                    elapsed_time = time.time() - start_time
                    logger.error(f"✗ {source_name} failed in {elapsed_time:.1f}s: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"✗ {source_name}: Failed after {max_retries} attempts: {str(e)}")
            
            # Delay giữa các nguồn
            if source_name != crawl_tasks[-1][0]:
                time.sleep(3)
        
        # Xử lý kết quả
        if all_results:
            all_results = self.merge_data(all_results)
            csv_file = self.save_to_csv(all_results)
            if not csv_file:
                logger.error("Failed to save CSV file")
                return {"success": False, "error": "Failed to save CSV file"}
            
            df = pd.DataFrame(all_results)
            csv_content = df.to_csv(index=False, encoding='utf-8-sig')
            cities_covered = len(df['city'].unique())
            sources = df['source'].value_counts().to_dict()
            
            logger.info("="*60)
            logger.info("CRAWL COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            logger.info(f"Total records: {len(all_results)}")
            logger.info(f"Cities covered: {cities_covered}")
            logger.info(f"CSV file: {csv_file}")
            logger.info("Sources breakdown:")
            for source, count in sources.items():
                logger.info(f"  {source}: {count} records")
            
            return {
                'csv_file': csv_file,
                'csv_content': csv_content,
                'total_records': len(all_results),
                'cities_covered': cities_covered,
                'sources': sources,
                'success': True
            }
        else:
            logger.error("="*60)
            logger.error("CRAWL FAILED - NO DATA COLLECTED")
            logger.error("="*60)
            logger.error("Possible issues:")
            logger.error("- Network connectivity problems")
            logger.error("- Invalid API keys")
            logger.error("- API rate limits exceeded")
            logger.error("- Target websites are down")
            return {
                'success': False,
                'error': 'No data was successfully crawled from any source'
            }

def simple_run():
    from dotenv import load_dotenv
    load_dotenv()
    crawler = AirQualityCrawler()
    
    iqair_api_key = None
    openweather_api_key = os.getenv('OPENWEATHER_API_KEY') 
    waqi_token = os.getenv('WAQI_TOKEN', 'demo')
    
    logger.info("Starting scheduled crawl...")
    logger.info("API Keys Status:")
    logger.info(f"  IQAir API: {'✓ Available' if iqair_api_key else '✗ Not provided'}")
    logger.info(f"  OpenWeatherMap API: {'✓ Available' if openweather_api_key else '✗ Not provided'}")
    logger.info(f"  WAQI Token: {'✓ Custom token' if waqi_token != 'demo' else '✗ Using demo token'}")
    
    return crawler.run_optimized_crawl(iqair_api_key, openweather_api_key, waqi_token)
    
if __name__ == "__main__":
    result = simple_run()