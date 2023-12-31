from pyspark.sql import SparkSession

from data_processor import DataReaderHDFS, DataReaderLocalFS, DataReader

DATA_SOURCE = "LOCAL"
HDFS_SERVER_ADDRESS = "hdfs://localhost:9000"
HDFS_DATA_DIRECTORY_PATH = "/assignment_data"
LOCAL_DATA_DIRECTORY_PATH = "/home/konrad/Desktop/UPM/big_data/assignment_data"


def get_data_reader(spark_session: SparkSession) -> DataReader:
	if DATA_SOURCE == "HDFS":
		data_reader = DataReaderHDFS(
			spark_session=spark_session,
			server_address=HDFS_SERVER_ADDRESS,
			directory_path=HDFS_DATA_DIRECTORY_PATH
		)
	elif DATA_SOURCE == "LOCAL":
		data_reader = DataReaderLocalFS(
			spark_session=spark_session,
			directory_path=LOCAL_DATA_DIRECTORY_PATH
		)
	else:
		raise ValueError(f"Unknown data source: {DATA_SOURCE}")
	return data_reader


if __name__ == "__main__":
	spark_session = SparkSession.builder.getOrCreate()
	data_reader = get_data_reader(spark_session=spark_session)
	list_of_files = data_reader.get_list_of_files()
	print(list_of_files)

	in_file = data_reader.read_file(list_of_files[0])
	for line in in_file.take(10):
		print(line)
