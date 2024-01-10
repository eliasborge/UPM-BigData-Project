from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType

from .const import FORBIDDEN_COLUMNS, ALLOWED_DATETIME_COLUMNS


class DataProcessor:
	def __init__(self, input_data: DataFrame):
		self.data = input_data

	def get_processed(self) -> DataFrame:
		self.remove_forbidden_columns()
		self.remove_cancelled_or_diverted_flights()
		self.convert_datetimes()
		return self.get_dataframe()

	def get_dataframe(self) -> DataFrame:
		return self.data

	def remove_forbidden_columns(self) -> None:
		self.data = self.data.drop(*FORBIDDEN_COLUMNS)

	def remove_cancelled_or_diverted_flights(self) -> None:
		self.data = self.data.filter((self.data.Cancelled != 1))
		self.data = self.data.drop("CancellationCode")

	def convert_datetimes(self):
		udf_func = udf(self.timestamp_from_datetime, FloatType())
		for column_name in ALLOWED_DATETIME_COLUMNS:
			self.data = self.data.withColumn(
				f"{column_name}_timestamp", udf_func(col("Year"), col("Month"), col("DayofMonth"), col(column_name), )
			)
		self.data = self.data.drop("Year", "Month", "DayofMonth", *ALLOWED_DATETIME_COLUMNS)

	@staticmethod
	def timestamp_from_datetime(year: int, month: int, day: int, time: str) -> float:
		"""
		Converts a datetime provided as separate values into timestamp. Timezone does not really make a difference,
		as long as all dates are converted into the same timezone.
		:param year: int value for year
		:param month: int value for month
		:param day: int value for day
		:param time: string value for time, representing "HHMM"
		:return: timestamp in seconds
		"""
		time = time.zfill(4)
		return datetime(year=year, month=month, day=day, hour=int(time[:2]), minute=int(time[2:])).timestamp()
