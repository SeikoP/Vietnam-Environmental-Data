from fastapi import APIRouter, Request
import logging

router = APIRouter()
logger = logging.getLogger("api.process")

@router.post("/air-process-data")
async def process_data(request: Request):
    """
    Process air quality data and return a single summary object for n8n workflow.
    Đảm bảo trả về 1 dict duy nhất (bọc trong list cho n8n), gồm các trường tổng hợp.
    """
    try:
        if request.headers.get("content-type", "").startswith("application/json"):
            data = await request.json()
        else:
            data = {}
    except Exception as e:
        logger.error(f"Error parsing request JSON: {str(e)}")
        return [{
            "status": "error",
            "message": "No data provided",
            "processed_data": [],
            "metrics": {"processed_records": 0}
        }]

    records = data.get("data", [])
    include_health_recommendations = data.get("include_health_recommendations", True)
    alert_threshold = data.get("alert_threshold", 100)

    # Đảm bảo records là list và loại bỏ các phần tử None/rỗng
    if not isinstance(records, list):
        records = [records] if records else []
    records = [r for r in records if r]  # loại bỏ None/rỗng

    if not records:
        # Trả về đúng 1 object lỗi duy nhất cho workflow, không lặp lại
        return [{
            "status": "error",
            "message": "No data provided",
            "processed_data": [],
            "metrics": {"processed_records": 0}
        }]

    def get_aqi_level_detailed(aqi):
        if aqi <= 50:
            return "Tốt"
        elif aqi <= 100:
            return "Trung bình"
        elif aqi <= 150:
            return "Không tốt cho nhóm nhạy cảm"
        elif aqi <= 200:
            return "Có hại cho sức khỏe"
        elif aqi <= 300:
            return "Rất có hại"
        else:
            return "Nguy hại"

    def get_health_recommendations(aqi):
        if aqi <= 50:
            return "Thời điểm tuyệt vời cho các hoạt động ngoài trời."
        elif aqi <= 100:
            return "Nhóm nhạy cảm nên theo dõi triệu chứng khi hoạt động ngoài trời."
        elif aqi <= 150:
            return "Nhóm nhạy cảm nên hạn chế hoạt động ngoài trời, đeo khẩu trang."
        elif aqi <= 200:
            return "Tất cả mọi người nên hạn chế hoạt động ngoài trời, sử dụng máy lọc không khí."
        elif aqi <= 300:
            return "Tránh hoạt động ngoài trời, sử dụng máy lọc không khí chất lượng cao."
        else:
            return "KHẨN CẤP: Ở trong nhà hoàn toàn, liên hệ y tế nếu có triệu chứng."

    # Tổng hợp dữ liệu
    aqi_values = []
    affected_areas = []
    max_aqi = None
    max_aqi_city = None

    for record in records:
        try:
            aqi = float(record.get("aqi", 0))
            city = record.get("city", "Unknown")
            aqi_values.append(aqi)
            if aqi > alert_threshold:
                affected_areas.append(f"{city} (AQI: {aqi}, {get_aqi_level_detailed(aqi)})")
            if max_aqi is None or aqi > max_aqi:
                max_aqi = aqi
                max_aqi_city = city
        except Exception as e:
            logger.warning(f"Error processing record {record}: {str(e)}")
            continue

    import statistics as stats
    statistics_data = {}
    if aqi_values:
        statistics_data = {
            "total_locations": len(aqi_values),
            "average_aqi": round(sum(aqi_values) / len(aqi_values), 2),
            "max_aqi": max(aqi_values),
            "min_aqi": min(aqi_values),
            "median_aqi": round(stats.median(aqi_values), 2),
            "locations_above_threshold": len([aqi for aqi in aqi_values if aqi > alert_threshold])
        }

    if affected_areas:
        severity = "KHẨN CẤP" if max_aqi and max_aqi > 300 else "CẢNH BÁO NGHIÊM TRỌNG" if max_aqi and max_aqi > 200 else "CẢNH BÁO"
        alert_message = f"{severity}: Chất lượng không khí kém (AQI > {alert_threshold}) tại {len(affected_areas)} địa điểm. AQI cao nhất: {max_aqi} tại {max_aqi_city}."
    else:
        alert_message = f"TÍCH CỰC: Tất cả {len(aqi_values)} địa điểm đều có chất lượng không khí dưới ngưỡng cảnh báo (AQI <= {alert_threshold})."

    health_recommendations = None
    if include_health_recommendations:
        avg_aqi = statistics_data.get("average_aqi", 0)
        health_recommendations = get_health_recommendations(avg_aqi)

    # Trả về 1 object duy nhất cho workflow
    return [{
        "status": "success",
        "alert_message": alert_message,
        "affected_areas": "; ".join(affected_areas) if affected_areas else "",
        "statistics": statistics_data,
        "global_health_recommendations": health_recommendations,
        "processed_data": records,
        "metrics": {"processed_records": len(records)}
    }]

@router.post("/climate-process-data")
async def climate_process_data(request: Request):
    """
    Phân tích dữ liệu khí hậu, trả về 1 object tổng hợp cho workflow n8n.
    """
    try:
        if request.headers.get("content-type", "").startswith("application/json"):
            data = await request.json()
        else:
            data = {}
    except Exception as e:
        logger.error(f"Error parsing request JSON: {str(e)}")
        return [{
            "status": "error",
            "message": "No data provided",
            "processed_data": [],
            "metrics": {"processed_records": 0}
        }]

    records = data.get("data", [])
    include_health_recommendations = data.get("include_health_recommendations", True)
    heat_threshold = data.get("heat_threshold", 35)

    # Đảm bảo records là list và loại bỏ các phần tử None/rỗng
    if not isinstance(records, list):
        records = [records] if records else []
    records = [r for r in records if r]

    if not records:
        return [{
            "status": "error",
            "message": "No data provided",
            "processed_data": [],
            "metrics": {"processed_records": 0}
        }]

    def get_heat_level(temp):
        if temp is None:
            return "Không xác định"
        if temp < 18:
            return "Lạnh"
        elif temp < 28:
            return "Dễ chịu"
        elif temp < 35:
            return "Nóng"
        else:
            return "Nắng nóng nguy hiểm"

    def get_climate_recommendations(temp, uvi):
        if temp is None:
            return "Không đủ dữ liệu để khuyến nghị."
        if temp >= 35:
            msg = "Cảnh báo nắng nóng, hạn chế ra ngoài vào buổi trưa, uống đủ nước."
        elif temp >= 28:
            msg = "Thời tiết nóng, nên mặc quần áo nhẹ, bổ sung nước."
        elif temp < 18:
            msg = "Thời tiết lạnh, giữ ấm cơ thể."
        else:
            msg = "Thời tiết dễ chịu, thích hợp cho các hoạt động ngoài trời."
        if uvi is not None and uvi >= 6:
            msg += " Lưu ý chỉ số UV cao, nên dùng kem chống nắng và che chắn."
        return msg

    temp_values = []
    uvi_values = []
    heat_waves = []
    max_temp = None
    max_temp_city = None

    for record in records:
        try:
            temp = float(record.get("temperature", 0))
            uvi = float(record.get("uvi", 0)) if record.get("uvi") is not None else None
            city = record.get("location", "Unknown")
            temp_values.append(temp)
            if uvi is not None:
                uvi_values.append(uvi)
            if temp >= heat_threshold:
                heat_waves.append(f"{city} ({temp}°C)")
            if max_temp is None or temp > max_temp:
                max_temp = temp
                max_temp_city = city
        except Exception as e:
            logger.warning(f"Error processing record {record}: {str(e)}")
            continue

    import statistics as stats
    statistics_data = {}
    if temp_values:
        statistics_data = {
            "total_locations": len(temp_values),
            "average_temperature": round(sum(temp_values) / len(temp_values), 2),
            "max_temperature": max(temp_values),
            "min_temperature": min(temp_values),
            "median_temperature": round(stats.median(temp_values), 2),
            "locations_above_threshold": len([t for t in temp_values if t >= heat_threshold])
        }
        if uvi_values:
            statistics_data["average_uvi"] = round(sum(uvi_values) / len(uvi_values), 2)
            statistics_data["max_uvi"] = max(uvi_values)
            statistics_data["min_uvi"] = min(uvi_values)

    if heat_waves:
        alert_message = f"CẢNH BÁO: Có {len(heat_waves)} địa điểm nắng nóng (≥{heat_threshold}°C). Nhiệt độ cao nhất: {max_temp}°C tại {max_temp_city}."
    else:
        alert_message = f"TÍCH CỰC: Không có địa điểm nào vượt ngưỡng nắng nóng ({heat_threshold}°C)."

    health_recommendations = None
    if include_health_recommendations and temp_values:
        avg_temp = statistics_data.get("average_temperature", 0)
        avg_uvi = statistics_data.get("average_uvi", 0) if "average_uvi" in statistics_data else None
        health_recommendations = get_climate_recommendations(avg_temp, avg_uvi)

    return [{
        "status": "success",
        "alert_message": alert_message,
        "heat_wave_areas": "; ".join(heat_waves) if heat_waves else "",
        "statistics": statistics_data,
        "global_health_recommendations": health_recommendations,
        "processed_data": records,
        "metrics": {"processed_records": len(records)}
    }]
