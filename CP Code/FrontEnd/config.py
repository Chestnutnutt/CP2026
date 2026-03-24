# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Reads data from .env config

INFLUX_URL = os.getenv("INFLUX_URL", "localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "")
POWERMETER_BUCKET = os.getenv("POWERMETER_BUCKET", "")
SERVER_BUCKET= os.getenv("SERVER_BUCKET", "")
MEASUREMENT = os.getenv("MEASUREMENT","")
