from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType

from data_processor import DataReaderHDFS, DataReaderLocalFS, DataReader, DataProcessor

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


def calculate_correlations(df):
	# Select numerical columns
	numerical_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, DoubleType))]

	# Calculate pairwise correlations
	correlations = {}
	for i in range(len(numerical_cols)):
		for j in range(i + 1, len(numerical_cols)):
			col1, col2 = numerical_cols[i], numerical_cols[j]
			corr_value = df.stat.corr(col1, col2)
			correlations[(col1, col2)] = corr_value

	return correlations


def visualize_correlations(correlations, threshold=0.5):
	print("\nStrongly Correlated Variable Pairs (Threshold = {}):\n".format(threshold))
	for (col1, col2), corr_value in correlations.items():
		if abs(corr_value) >= threshold:
			print(f"{col1} and {col2}: Correlation = {corr_value:.2f}")


if __name__ == "__main__":
	spark_session = SparkSession.builder.getOrCreate()
	data_reader = get_data_reader(spark_session=spark_session)
	list_of_files = data_reader.get_list_of_files()
	print(list_of_files)

	# Read the first file to define the base DataFrame schema
	base_df = data_reader.read_all_files()
	data_processor = DataProcessor(input_data=base_df)
	data_processor.remove_forbidden_columns()
	processed_df = data_processor.get_dataframe()

	# Proceed with correlation analysis only if there is more than one file
	if len(list_of_files) > 1:
		correlations = calculate_correlations(processed_df)
		visualize_correlations(correlations)

	spark_session.stop()
