from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType

from .const import FORBIDDEN_COLUMNS, ALLOWED_DATETIME_COLUMNS


class DataProcessor:
	def __init__(self, input_data: DataFrame):
		self.data = input_data

	def get_processed(self) -> DataFrame:
		"""
		Function performing data processing on given data.
		:return: Processed DataFrame
		"""
		self.__remove_forbidden_columns()
		self.__remove_cancelled_flights()
		self.__convert_datetimes()
		return self.get_dataframe()

	def get_dataframe(self) -> DataFrame:
		"""
		Getter for data field
		:return: data field
		"""
		return self.data

	def __remove_forbidden_columns(self):
		"""
		Helper function for removing forbidden columns. Forbidden columns are one that are not allowed to be used in
		the process according to task specification.
		"""
		self.data = self.data.drop(*FORBIDDEN_COLUMNS)

	def __remove_cancelled_flights(self):
		"""
		Helper function for removing flights that have been cancelled.
		"""
		self.data = self.data.filter((self.data.Cancelled != 1))
		self.data = self.data.drop("CancellationCode")

	def __convert_datetimes(self):
		"""
		Helper function for converting attributes representing datetime into unix timestamp.
		"""
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
		Some <time> formats are incorrect having less than 4 digits. In this case, <time> is filled with zeros at the
		beginning of the string to match the desired length=4. Midnight is defined as 24, not 00, which is not accepted
		by datetime constructor, so conversion is being made using modulo.
		:param year: int value for year
		:param month: int value for month
		:param day: int value for day
		:param time: string value for time, representing "HHMM"
		:return: timestamp in seconds
		"""
		time = time.zfill(4)
		return datetime(year=year, month=month, day=day, hour=int(time[:2]) % 24, minute=int(time[2:]) % 60).timestamp()
