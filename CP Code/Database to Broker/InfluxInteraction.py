from influxdb_client import InfluxDBClient , WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime

#===================== Influx Class to handle write to InfluxDB==================
# Called by Consumer

class Influx:
	def __init__(self):
		self.url = "http://localhost:8086"
		self.token = "test"
		self.org = "test"
		self.bucket = "test"
	
		self.client = InfluxDBClient(url = self.url,token = self.token,org = self.org)
		self.write_api = self.client.write_api(write_options = SYNCHRONOUS)
	
	def sendtoDB(self,data):
		try:
			location = data["name"]
			timestamp_raw = data["timestamp"]
			voltage_a = data["voltage_a"]
			voltage_b = data["voltage_b"]
			voltage_c = data["voltage_c"]
			current_a = data["current_a"]
			current_b = data["current_b"]
			current_c = data["current_c"]
			power_a = data["power_a"]
			power_b = data["power_b"]
			power_c = data["power_c"]
			freq = data["freq"]
			temp = data["temp"]
			tf_id = data["tf_id"]
		except KeyError as e:
			print(f"Invalid Key entered, {e}")
			pass
			
			
		try:
			dt = datetime.datetime.strptime(timestamp_raw,"%Y-%m-%d %H:%M:%S")
			timestamp_ns = int(dt.timestamp()) * 1_000_000_000
			# if key for TF1/TF2 exist
			line = (f"sensor_data,location={location},tf_id={tf_id} va={voltage_a},ia={current_a},pa={power_a},vb={voltage_b},ib={current_b},pb={power_b},vc={voltage_c},ic={current_c},pc={power_c},freq={freq},temp={temp} {timestamp_ns}\n")
			
			self.write_api.write(bucket = self.bucket,org = self.org, record =line)
			print("Data send to InfluxDB")
		except Exception:
			raise
			
	
	def exit(self):
		self.client.close()

