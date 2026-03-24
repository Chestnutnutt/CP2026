from influxdb_client import InfluxDBClient
from config import (
    INFLUX_URL,
    INFLUX_TOKEN,
    INFLUX_ORG,
    POWERMETER_BUCKET,
    MEASUREMENT,
    SERVER_BUCKET,
)
from datetime import datetime

client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)

query_api = client.query_api()


# Checks whether the InfluxDB server is reachable
def ping_influx() -> bool:
    try:
        client.ping()
        return True
    except Exception:
        return False


# Returns all distinct location tags from the telemetry bucket
def list_locations():
    flux = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(
      bucket: "{POWERMETER_BUCKET}",
      tag: "location"
    )
    '''
    tables = query_api.query(flux)
    locations = []
    for table in tables:
        for record in table.records:
            locations.append(record.get_value())
    return locations


# Returns all distinct transformer IDs for a selected location
def list_transformers_for_location(location: str):
    flux = f'''
    from(bucket: "{POWERMETER_BUCKET}")
      |> range(start: -30d)
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
      |> filter(fn: (r) => r["location"] == "{location}")
      |> keep(columns: ["tf_id"])
      |> group()
      |> distinct(column: "tf_id")
    '''
    tables = query_api.query(flux)
    tf_ids = []
    for table in tables:
        for record in table.records:
            tf_ids.append(record.get_value())
    return tf_ids


# Returns all available field names in the telemetry measurement
def list_fields():
    flux = f'''
    import "influxdata/influxdb/schema"
    schema.measurementFieldKeys(
      bucket: "{POWERMETER_BUCKET}",
      measurement: "{MEASUREMENT}"
    )
    '''
    tables = query_api.query(flux)
    fields = []
    for table in tables:
        for record in table.records:
            fields.append(record.get_value())
    return fields


# Returns recent time-series points for a single field
def get_series(location: str, tf_id: str, field: str, minutes: int = 60):
    flux = f'''
    from(bucket: "{POWERMETER_BUCKET}")
      |> range(start: -{minutes}m)
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
      |> filter(fn: (r) => r["location"] == "{location}")
      |> filter(fn: (r) => r["tf_id"] == "{tf_id}")
      |> filter(fn: (r) => r["_field"] == "{field}")
      |> keep(columns: ["_time", "_value"])
      |> sort(columns: ["_time"])
    '''
    tables = query_api.query(flux)
    points = []
    for table in tables:
        for record in table.records:
            points.append(
                {
                    "time": record["_time"].isoformat(),
                    "value": record["_value"],
                }
            )
    return points


# Returns the latest value for a single telemetry field
def get_latest(location: str, tf_id: str, field: str):
    flux = f'''
    from(bucket: "{POWERMETER_BUCKET}")
      |> range(start: -7d)
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
      |> filter(fn: (r) => r["location"] == "{location}")
      |> filter(fn: (r) => r["tf_id"] == "{tf_id}")
      |> filter(fn: (r) => r["_field"] == "{field}")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1)
    '''
    tables = query_api.query(flux)
    for table in tables:
        for record in table.records:
            return {
                "time": record["_time"].isoformat(),
                "value": record["_value"],
            }
    return None


# Returns the latest available values for all fields of a transformer
def get_latest_all(location: str, tf_id: str):
    flux = f'''
    from(bucket: "{POWERMETER_BUCKET}")
      |> range(start: -7d)
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
      |> filter(fn: (r) => r["location"] == "{location}")
      |> filter(fn: (r) => r["tf_id"] == "{tf_id}")
      |> group(columns: ["_field"])
      |> last()
      |> keep(columns: ["_time","_field","_value"])
    '''
    tables = query_api.query(flux)

    out = {}
    latest_time = None

    for table in tables:
        for record in table.records:
            field = record.get_field()
            value = record.get_value()
            t = record.get_time()

            out[field] = value
            if latest_time is None or t > latest_time:
                latest_time = t

    if not out:
        return None

    return {
        "time": latest_time.isoformat() if latest_time else None,
        "fields": out
    }


# Returns time-series data for a single field between two datetimes
def get_series_range(location: str, tf_id: str, field: str, start_dt: datetime, end_dt: datetime):
    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()

    flux = f'''
    from(bucket: "{POWERMETER_BUCKET}")
      |> range(start: time(v: "{start_iso}"), stop: time(v: "{end_iso}"))
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
      |> filter(fn: (r) => r["location"] == "{location}")
      |> filter(fn: (r) => r["tf_id"] == "{tf_id}")
      |> filter(fn: (r) => r["_field"] == "{field}")
      |> sort(columns: ["_time"])
      |> keep(columns: ["_time","_value"])
    '''
    tables = query_api.query(flux)

    out = []
    for table in tables:
        for record in table.records:
            out.append({"time": record.get_time().isoformat(), "value": record.get_value()})
    return out


# Returns time-series data for multiple fields between two datetimes
def get_series_range_multi(location: str, tf_id: str, fields: list[str], start_dt: datetime, end_dt: datetime):
    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()
    field_list = ",".join(f'"{f}"' for f in fields)

    flux = f'''
    from(bucket: "{POWERMETER_BUCKET}")
      |> range(start: time(v: "{start_iso}"), stop: time(v: "{end_iso}"))
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
      |> filter(fn: (r) => r["location"] == "{location}")
      |> filter(fn: (r) => r["tf_id"] == "{tf_id}")
      |> filter(fn: (r) => contains(value: r["_field"], set: [{field_list}]))
      |> sort(columns: ["_time"])
      |> keep(columns: ["_time","_field","_value"])
    '''
    tables = query_api.query(flux)

    out = {f: [] for f in fields}
    for table in tables:
        for record in table.records:
            f = record.get_field()
            out[f].append({
                "time": record.get_time().isoformat(),
                "value": record.get_value(),
            })
    return out


# Returns the latest server resource metrics collected by Telegraf
def get_server_latest():
    cpu_flux = f'''
        from(bucket: "{SERVER_BUCKET}")
          |> range(start: -15m)
          |> filter(fn: (r) => r["_measurement"] == "cpu")
          |> filter(fn: (r) => r["_field"] == "usage_active")
          |> filter(fn: (r) => r["cpu"] == "cpu-total")
          |> last()
        '''

    mem_flux = f'''
        from(bucket: "{SERVER_BUCKET}")
          |> range(start: -15m)
          |> filter(fn: (r) => r["_measurement"] == "mem")
          |> filter(fn: (r) => r["_field"] == "used_percent")
          |> last()
        '''

    disk_flux = f'''
        from(bucket: "{SERVER_BUCKET}")
          |> range(start: -15m)
          |> filter(fn: (r) => r["_measurement"] == "disk")
          |> filter(fn: (r) => r["path"] == "/")
          |> filter(fn: (r) =>
              r["_field"] == "used_percent" or
              r["_field"] == "total" or
              r["_field"] == "used" or
              r["_field"] == "free"
          )
          |> last()
        '''

    uptime_flux = f'''
        from(bucket: "{SERVER_BUCKET}")
          |> range(start: -15m)
          |> filter(fn: (r) => r["_measurement"] == "system")
          |> filter(fn: (r) => r["_field"] == "uptime")
          |> last()
        '''

    temp_flux = f'''
    from(bucket: "{SERVER_BUCKET}")
      |> range(start: -15m)
      |> filter(fn: (r) => r["_measurement"] == "temp")
      |> filter(fn: (r) => r["_field"] == "temp")
      |> last()
    '''

    swap_flux = f'''
    from(bucket: "{SERVER_BUCKET}")
      |> range(start: -15m)
      |> filter(fn: (r) => r["_measurement"] == "swap")
      |> filter(fn: (r) => r["_field"] == "used_percent")
      |> last()
    '''

    processes_flux = f'''
    from(bucket: "{SERVER_BUCKET}")
      |> range(start: -15m)
      |> filter(fn: (r) => r["_measurement"] == "processes")
      |> filter(fn: (r) => r["_field"] == "running")
      |> last()
    '''

    cpu_usage = None
    memory_usage = None
    uptime_seconds = None
    cpu_temp_c = None
    nvme_temp_c = None
    swap_used_percent = None
    running_processes = None

    disk = {
        "used_percent": None,
        "total": None,
        "used": None,
        "free": None,
        "path": "/"
    }

    for table in query_api.query(cpu_flux):
        for record in table.records:
            cpu_usage = record.get_value()

    for table in query_api.query(mem_flux):
        for record in table.records:
            memory_usage = record.get_value()

    for table in query_api.query(disk_flux):
        for record in table.records:
            field = record.get_field()
            disk[field] = record.get_value()
            disk["path"] = record.values.get("path", "/")

    for table in query_api.query(uptime_flux):
        for record in table.records:
            uptime_seconds = record.get_value()

    temps = {}

    for table in query_api.query(temp_flux):
        for record in table.records:
            sensor = record.values.get("sensor")
            value = record.get_value()
            if sensor:
                temps[sensor] = value

    cpu_temp_c = temps.get("coretemp_package_id_0")

    if cpu_temp_c is None:
        core_temps = [
            value for sensor, value in temps.items()
            if sensor.startswith("coretemp_core_")
        ]
        if core_temps:
            cpu_temp_c = max(core_temps)

    nvme_temp_c = temps.get("nvme_composite")

    if nvme_temp_c is None:
        nvme_temps = [
            value for sensor, value in temps.items()
            if sensor.startswith("nvme_sensor_")
        ]
        if nvme_temps:
            nvme_temp_c = max(nvme_temps)

    for table in query_api.query(swap_flux):
        for record in table.records:
            swap_used_percent = record.get_value()

    for table in query_api.query(processes_flux):
        for record in table.records:
            running_processes = record.get_value()

    return {
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage,
        "storage_usage": disk["used_percent"],
        "storage_total": disk["total"],
        "storage_used": disk["used"],
        "storage_free": disk["free"],
        "storage_path": disk["path"],
        "uptime_seconds": uptime_seconds,
        "cpu_temp_c": cpu_temp_c,
        "nvme_temp_c": nvme_temp_c,
        "swap_used_percent": swap_used_percent,
        "running_processes": running_processes,
    }


# Returns the latest connectivity status of monitored devices
def get_device_health_latest():
    flux = f'''
    from(bucket: "{SERVER_BUCKET}")
      |> range(start: -15m)
      |> filter(fn: (r) => r["_measurement"] == "ping")
      |> filter(fn: (r) =>
          r["_field"] == "average_response_ms" or
          r["_field"] == "percent_packet_loss"
      )
      |> group(columns: ["url", "_field"])
      |> last()
    '''

    rows = {}

    for table in query_api.query(flux):
        for record in table.records:
            url = record.values.get("url", "unknown")
            item = rows.setdefault(url, {
                "url": url,
                "avg_ms": None,
                "packet_loss": None
            })

            if record.get_field() == "average_response_ms":
                item["avg_ms"] = record.get_value()
            elif record.get_field() == "percent_packet_loss":
                item["packet_loss"] = record.get_value()

    devices = []
    for item in rows.values():
        avg = item["avg_ms"]
        loss = item["packet_loss"]

        if loss is not None and loss >= 100:
            status = "bad"
            display = "Timeout"
        elif avg is None:
            status = "bad"
            display = "Timeout"
        elif avg >= 150:
            status = "warn"
            display = f"{avg:.0f} ms"
        else:
            status = "good"
            display = f"{avg:.0f} ms"

        devices.append({
            **item,
            "status": status,
            "display": display,
        })

    return {"devices": sorted(devices, key=lambda x: x["url"])}


# Returns the latest warning and critical alerts from the monitoring bucket
def get_latest_alerts(hours: int = 24):
    flux = f'''
    from(bucket: "_monitoring")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r["_measurement"] == "statuses")
      |> filter(fn: (r) => r["_field"] == "_message")
      |> group(columns: ["_check_id", "location", "tf_id"])
      |> last()
      |> filter(fn: (r) => r["_level"] == "warn" or r["_level"] == "crit")
      |> sort(columns: ["_time"], desc: true)
    '''
    tables = query_api.query(flux)
    alerts = []

    for table in tables:
        for record in table.records:
            v = record.values
            alerts.append({
                "time": record.get_time().isoformat(),
                "level": v.get("_level"),
                "message": record.get_value(),
                "check_id": v.get("_check_id"),
                "check_name": v.get("_check_name"),
                "source_measurement": v.get("_source_measurement"),
                "location": v.get("location"),
                "tf_id": v.get("tf_id"),
            })

    return alerts


# Returns max, average, and latest values for selected fields in a time range
def get_series_table(location: str, tf_id: str, fields: list[str], start_dt: datetime, end_dt: datetime):
    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()
    field_list = ",".join(f'"{f}"' for f in fields)

    base = f'''
    from(bucket: "{POWERMETER_BUCKET}")
      |> range(start: time(v: "{start_iso}"), stop: time(v: "{end_iso}"))
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
      |> filter(fn: (r) => r["location"] == "{location}")
      |> filter(fn: (r) => r["tf_id"] == "{tf_id}")
      |> filter(fn: (r) => contains(value: r["_field"], set: [{field_list}]))
      |> group(columns: ["_field"])
    '''

    max_flux = base + '''
      |> max()
      |> keep(columns: ["_field", "_value"])
    '''

    avg_flux = base + '''
      |> mean()
      |> keep(columns: ["_field", "_value"])
    '''

    latest_flux = base + '''
      |> last()
      |> keep(columns: ["_field", "_value"])
    '''

    out = {
        f: {"max": None, "avg": None, "latest": None}
        for f in fields
    }

    for table in query_api.query(max_flux):
        for record in table.records:
            field = record.get_field()
            out[field]["max"] = record.get_value()

    for table in query_api.query(avg_flux):
        for record in table.records:
            field = record.get_field()
            out[field]["avg"] = record.get_value()

    for table in query_api.query(latest_flux):
        for record in table.records:
            field = record.get_field()
            out[field]["latest"] = record.get_value()

    return out
