from pyspark.sql import DataFrame


FORBIDDEN_COLUMNS = ["ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]


class DataProcessor:
	def __init__(self, input_data: DataFrame):
		self.data = input_data

	def get_dataframe(self):
		return self.data

	def remove_forbidden_columns(self):
		self.data = self.data.drop(*FORBIDDEN_COLUMNS)
