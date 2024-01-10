from dotenv import load_dotenv
from pyspark.sql import SparkSession

from data_processor import DataReaderManager, DataProcessor


load_dotenv()


if __name__ == "__main__":
	spark_session = SparkSession.builder.getOrCreate()
	data_reader = DataReaderManager.get_data_reader(spark_session=spark_session)
	data = data_reader.read_all_files()
	data_parsed = DataProcessor(input_data=data).get_processed()

	print(data.printSchema())
	for line in data.take(10):
		print(line)

	print(data_parsed.printSchema())
	for line in data_parsed.take(10):
		print(line)
