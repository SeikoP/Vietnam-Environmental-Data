from fastapi import APIRouter, Request
import logging

router = APIRouter()
logger = logging.getLogger("api.process")

@router.post("/process-data")
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
