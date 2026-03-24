from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqldb import list_site_meta, list_site_transformers, map_ip_to_site_name
from influxclient import (
    ping_influx,
    list_locations,
    list_transformers_for_location,
    list_fields,
    get_series,
    get_latest,
    get_latest_all,
    get_series_range,
    get_series_range_multi,
    get_series_table,
    get_server_latest,
    get_device_health_latest,
    get_latest_alerts,
)
from datetime import datetime

app = FastAPI(title="Capstone IoT API")
templates = Jinja2Templates(directory="templates")


# Renders the historical chart page
@app.get("/history", response_class=HTMLResponse)
def history_chart_page(request: Request):
    return templates.TemplateResponse("history_chart.html", {"request": request})


# Renders the main dashboard page
@app.get("/main", response_class=HTMLResponse)
def main_page(request: Request):
    return templates.TemplateResponse("main.html", {"request": request})


# Returns all enabled sites with transformer and latest telemetry summary data
@app.get("/api/sites")
def get_sites():
    sites = list_site_meta()
    out = []

    for site in sites:
        transformers = list_site_transformers(site["id"])
        tf_results = []

        temp_value = None
        try:
            latest_temp = get_latest_all(site["location_key"], "1")
            if latest_temp:
                temp_value = latest_temp["fields"].get("temp")
        except Exception:
            temp_value = None

        for tf_id in transformers:
            latest = None
            try:
                latest = get_latest_all(site["location_key"], tf_id)
            except Exception:
                latest = None

            fields = latest["fields"] if latest else {}
            tf_results.append({
                "tf_id": tf_id,
                "freq": fields.get("freq"),
                "va": fields.get("va"),
                "ia": fields.get("ia"),
                "pa": fields.get("pa"),
            })

        out.append({
            "id": site["id"],
            "name": site["name"],
            "location": site["location_key"],
            "lat": site["lat"],
            "lng": site["lng"],
            "temp": temp_value,
            "transformers": tf_results,
        })

    return out


# Returns application health based on InfluxDB connectivity
@app.get("/api/health")
def health():
    if not ping_influx():
        raise HTTPException(status_code=500, detail="InfluxDB not reachable")
    return {"status": "ok"}


# Returns all available telemetry locations
@app.get("/api/locations")
def get_locations():
    return {"locations": list_locations()}


# Returns all transformers for a selected location
@app.get("/api/locations/{location}/transformers")
def get_transformers(location: str):
    tf_ids = list_transformers_for_location(location)
    return {"location": location, "transformers": tf_ids}


# Returns all available telemetry field names
@app.get("/api/measurements")
def get_measurements():
    fields = list_fields()
    return {"fields": fields}


# Returns recent time-series data for a single field
@app.get("/api/data")
def get_data(
    location: str,
    tf_id: str,
    field: str,
    minutes: int = Query(60, ge=1, le=60 * 24),
):
    points = get_series(location=location, tf_id=tf_id, field=field, minutes=minutes)
    return {
        "location": location,
        "tf_id": tf_id,
        "field": field,
        "minutes": minutes,
        "points": points,
    }


# Returns the latest time-series point for a single field
@app.get("/api/data/latest")
def get_data_latest(location: str, tf_id: str, field: str):
    latest = get_latest(location=location, tf_id=tf_id, field=field)
    if latest is None:
        raise HTTPException(status_code=404, detail="No data found")
    return {
        "location": location,
        "tf_id": tf_id,
        "field": field,
        "point": latest,
    }


# Returns the latest snapshot of all fields for a transformer
@app.get("/api/data/latestall")
def latest_all_endpoint(location: str, tf_id: str):
    data = get_latest_all(location=location, tf_id=tf_id)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found")
    return {
        "location": location,
        "tf_id": tf_id,
        **data
    }


# Returns time-series data for a field within a selected time range
@app.get("/api/data/range")
def get_data_range(location: str, tf_id: str, field: str, start: str, end: str):
    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="start/end must be ISO-8601 e.g. 2026-03-01T00:00:00Z"
        )

    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="end must be after start")

    points = get_series_range(location, tf_id, field, start_dt, end_dt)
    return {
        "location": location,
        "tf_id": tf_id,
        "field": field,
        "start": start,
        "end": end,
        "points": points
    }


# Returns time-series data for multiple fields within a selected time range
@app.get("/api/data/range_multi")
def get_data_range_multi(location: str, tf_id: str, fields: str, start: str, end: str):
    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="start/end must be ISO-8601 e.g. 2026-03-01T00:00:00Z",
        )

    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="end must be after start")

    field_list = [f.strip() for f in fields.split(",") if f.strip()]
    if not field_list:
        raise HTTPException(status_code=400, detail="fields must not be empty")

    series = get_series_range_multi(location, tf_id, field_list, start_dt, end_dt)
    return {
        "location": location,
        "tf_id": tf_id,
        "start": start,
        "end": end,
        "series": series,
    }


# Returns summary statistics for multiple fields within a selected time range
@app.get("/api/data/range_multi_stats")
def get_data_range_multi_stats(location: str, tf_id: str, fields: str, start: str, end: str):
    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="start/end must be ISO-8601 e.g. 2026-03-01T00:00:00Z",
        )

    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="end must be after start")

    field_list = [f.strip() for f in fields.split(",") if f.strip()]
    if not field_list:
        raise HTTPException(status_code=400, detail="fields must not be empty")

    stats = get_series_table(location, tf_id, field_list, start_dt, end_dt)

    return {
        "location": location,
        "tf_id": tf_id,
        "start": start,
        "end": end,
        "stats": stats,
    }


# Returns the latest server resource statistics
@app.get("/api/server/latest")
def api_server_latest():
    return get_server_latest()


# Returns the latest device health information with site name mapping
@app.get("/api/device-health/latest")
def api_device_health_latest():
    raw = get_device_health_latest()
    ip_map = map_ip_to_site_name()

    devices = raw.get("devices", [])

    out = []
    for d in devices:
        url = d.get("url")
        name = ip_map.get(url)
        out.append({
            "url": url,
            "name": name or url,
            "status": d.get("status"),
            "display": d.get("display"),
        })

    return {"devices": out}


# Returns the latest warning and critical alerts within the selected time window
@app.get("/api/alerts/latest")
def api_alerts_latest(hours: int = Query(24, ge=1, le=168)):
    return {"alerts": get_latest_alerts(hours=hours)}
