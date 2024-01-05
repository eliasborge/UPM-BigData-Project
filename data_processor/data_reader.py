from os import listdir
from os.path import isfile, join

from abc import ABC
from pyspark.sql import SparkSession, DataFrame


class DataReader(ABC):
	def get_list_of_files(self) -> list[str]:
		"""
		Returns list of paths to files (excluding directories) present in directory path
		:return: list of absolute paths to files
		"""
		pass

	def read_file(self, filename_str) -> DataFrame:
		"""
		Returns pyspark DataFrame object containing data from file of given filename
		:param filename_str: name of file to be read
		:return: file content as pyspark DataFrame
		"""
		pass

	def read_all_files(self) -> DataFrame | None:
		"""
		Returns pyspark DataFrame object containing data from all files found by data reader in the location
		:return: files content as pyspark DataFrame
		"""
		filenames = self.get_list_of_files()
		if len(filenames) == 0:
			return None
		df_from_csv: DataFrame = self.read_file(filename_str=filenames[0])
		for filename in filenames[1:]:
			df_from_csv = df_from_csv.union(self.read_file(filename_str=filename))
		return df_from_csv


class DataReaderLocalFS(DataReader):
	session: SparkSession
	sc: SparkSession.sparkContext
	directory_path: str

	def __init__(self, spark_session: SparkSession, directory_path: str):
		self.session = spark_session
		self.sc = spark_session.sparkContext
		self.directory_path = directory_path

	def get_list_of_files(self) -> list[str]:
		return [
			join(self.directory_path, f)
			for f in listdir(self.directory_path) if isfile(join(self.directory_path, f))
		]

	def read_file(self, filename_str) -> DataFrame:
		df_from_csv = self.session.read.options(header='True', inferSchema='True', delimiter=',').csv(filename_str)
		return df_from_csv


class DataReaderHDFS(DataReader):
	session: SparkSession
	sc: SparkSession.sparkContext
	directory_path: str

	def __init__(self, spark_session: SparkSession, server_address: str, directory_path: str):
		self.session = spark_session
		self.sc = spark_session.sparkContext
		self.server_address = server_address
		self.hdfs_directory_path = directory_path

	def get_list_of_files(self) -> list[str]:
		URI = self.sc._gateway.jvm.java.net.URI
		Path = self.sc._gateway.jvm.org.apache.hadoop.fs.Path
		FileSystem = self.sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
		Configuration = self.sc._gateway.jvm.org.apache.hadoop.conf.Configuration

		fs = FileSystem.get(URI(self.server_address + self.hdfs_directory_path), Configuration())

		status = fs.listStatus(Path(self.hdfs_directory_path))

		return [fileStatus.getPath().toUri().getRawPath() for fileStatus in status if fileStatus.isFile()]

	def read_file(self, filename_str) -> DataFrame:
		df_from_csv = self.session.read.options(header='True', inferSchema='True', delimiter=',').csv(self.server_address + filename_str)
		return df_from_csv
