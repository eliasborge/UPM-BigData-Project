from pyspark.sql.types import StructType, StructField, StringType, IntegerType

CSV_SCHEMA = StructType([
	StructField("Year", IntegerType(), True),
	StructField("Month", IntegerType(), True),
	StructField("DayofMonth", IntegerType(), True),
	StructField("DayOfWeek", IntegerType(), True),
	StructField("DepTime", StringType(), True),
	StructField("CRSDepTime", StringType(), True),
	StructField("ArrTime", StringType(), True),
	StructField("CRSArrTime", StringType(), True),
	StructField("UniqueCarrier", StringType(), True),
	StructField("FlightNum", StringType(), True),
	StructField("TailNum", StringType(), True),
	StructField("ActualElapsedTime", IntegerType(), True),
	StructField("CRSElapsedTime", IntegerType(), True),
	StructField("AirTime", IntegerType(), True),
	StructField("ArrDelay", IntegerType(), True),
	StructField("DepDelay", IntegerType(), True),
	StructField("Origin", StringType(), True),
	StructField("Dest", StringType(), True),
	StructField("Distance", IntegerType(), True),
	StructField("TaxiIn", IntegerType(), True),
	StructField("TaxiOut", IntegerType(), True),
	StructField("Cancelled", IntegerType(), True),
	StructField("CancellationCode", StringType(), True),
	StructField("Diverted", IntegerType(), True),
	StructField("CarrierDelay", IntegerType(), True),
	StructField("WeatherDelay", IntegerType(), True),
	StructField("NASDelay", IntegerType(), True),
	StructField("SecurityDelay", IntegerType(), True),
	StructField("LateAircraftDelay", IntegerType(), True)
])
ALL_COLUMNS = CSV_SCHEMA.fieldNames()
FORBIDDEN_COLUMNS = ["ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay",
					 "NASDelay", "SecurityDelay", "LateAircraftDelay"]
ALLOWED_DATETIME_COLUMNS = ["DepTime", "CRSDepTime", "CRSArrTime"]
