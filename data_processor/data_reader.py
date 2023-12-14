from os import listdir
from os.path import isfile, join

from abc import ABC
from pyspark.sql import SparkSession


class DataReader(ABC):
	def get_list_of_files(self) -> list[str]:
		"""
		Returns list of paths to files (excluding directories) present in directory path
		:return: list of absolute paths to files
		"""
		pass

	def read_file(self, filename_str):
		pass


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

	def read_file(self, filename_str):
		df_from_csv = self.session.read.options(header='True', inferSchema='True', delimiter=',').csv(filename_str)
		df_from_csv.printSchema()
		df_from_csv.show()
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

	def read_file(self, filename_str):
		df_from_csv = self.session.read.options(header='True', inferSchema='True', delimiter=',').csv(self.server_address + filename_str)
		df_from_csv.printSchema()
		return df_from_csv
