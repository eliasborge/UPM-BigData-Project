from dotenv import load_dotenv
from pyspark.sql import SparkSession

from data_processor import DataReaderManager, DataProcessor


load_dotenv()


if __name__ == "__main__":
	spark_session = SparkSession.builder.getOrCreate()
	data_reader = DataReaderManager.get_data_reader(spark_session=spark_session)
	data = data_reader.read_all_files()
	for line in data.take(10):
		print(line)

	data_processor = DataProcessor(input_data=data)
	data_processor.remove_forbidden_columns()
	data_parsed = data_processor.get_dataframe()
	print(data.printSchema())
	print(data_parsed.printSchema())
