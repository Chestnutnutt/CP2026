# Centralised Server Architecture for Power Meter Telemetry

A web-based low-voltage monitoring system that collects telemetry from distributed substations, forwards data through RabbitMQ, stores it in InfluxDB, and presents real-time and historical data through a central dashboard.

## Overview
This project redesigns the LVMS telemetry architecture from a distributed edge-heavy deployment to a centralised server model.

## Features
- RabbitMQ-based message brokering
- InfluxDB time-series storage
- FastAPI backend APIs
- Historical chart visualisation
- Server and device health monitoring

## Tech Stack
- Python
- FastAPI
- RabbitMQ
- InfluxDB
- SQLite
- HTML/CSS/JavaScript
- Apache ECharts
- Tailscale

## Network Requirements
This project assumes that the central server and edge devices are connected over a private network using Tailscale or housed locally. 
For telemetry pipeline using Tailscale, ensure that:
- Tailscale is installed on all participating devices
- all devices are joined to the same tailnet
- the configured RabbitMQ host is reachable over its Tailscale IP

## Edge device Setup Requirements
This project assumes that the Edge devices has a runnning data collection device


## Project Structure

```text
CP Code/
├── Frontend/                    # FastAPI backend and frontend templates
│   ├── main.py                  # API routes and page rendering
│   ├── influxclient.py          # InfluxDB query functions
│   ├── sqldb.py                 # SQLite metadata functions
│   ├── config.py                # Environment variable loading
│   └── templates/               # Frontend HTML pages
│       ├── main.html            # Main monitoring dashboard
│       └── history_chart.html   # Historical chart dashboard
├── Database to Broker/          # RabbitMQ consumer for telemetry ingestion
│   ├── Consumer.py              # Consumes telemetry messages and writes to InfluxDB
│   └── InfluxInteraction.py     # InfluxDB write functions
├── Edge devices/                # RabbitMQ publisher for telemetry data
│   └── RabbitMQPublisher.py     # Publishes data to the RabbitMQ broker
└── README.md                    # Project overview and setup guide
```

## Installation
1. Clone the repository.
2. Create and activate a virtual environment.
3. Install dependencies.
4. Configure the `.env` file.
5. Start the Edge Device Publisher
5. Start the FastAPI server.
6. Start the RabbitMQ consumer.

## Configuration
1.Create a `.env` file with:
- `INFLUX_URL`
- `INFLUX_TOKEN`
- `INFLUX_ORG`
- `POWERMETER_BUCKET`
- `SERVER_BUCKET`
- `MEASUREMENT`
2. Place the file with the config.py folder

## Usage
- Open `/main` for the main dashboard.
- Open `/history` for historical charts.
- Use the API endpoints to query telemetry and system health.

## Additional Documentation
- [Deployment Guide](docs/deployment.md)
